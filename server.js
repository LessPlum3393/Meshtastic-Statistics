require('dotenv').config();
const express = require('express');
const http = require('http');
const mqtt = require('mqtt');
const protobuf = require('protobufjs');
const crypto = require('crypto');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const { DatabaseSync } = require('node:sqlite');
const { MongoClient } = require('mongodb');

function parseBoolEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null) return defaultValue;
  const v = String(raw).trim().toLowerCase();
  if (v === '1' || v === 'true' || v === 'yes' || v === 'y' || v === 'on') return true;
  if (v === '0' || v === 'false' || v === 'no' || v === 'n' || v === 'off') return false;
  return defaultValue;
}

function parseNumberEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === '') return defaultValue;
  const n = Number(raw);
  return Number.isFinite(n) ? n : defaultValue;
}

function getMqttTopics() {
  if (process.env.MESHTASTIC_TOPICS) {
    return String(process.env.MESHTASTIC_TOPICS)
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);
  }

  // Retained map reports are the fastest path to a large node list.
  const topics = ['msh/+/2/map/#'];

  if (parseBoolEnv('MESHTASTIC_INCLUDE_ALL_CHANNELS', false)) {
    // Highest-traffic option; will discover nodes across all presets/channels.
    topics.push('msh/+/2/e/#');
  } else {
    // LongFast is high traffic; disable by default for fastest catch-up.
    if (parseBoolEnv('MESHTASTIC_INCLUDE_LONGFAST', false)) {
      topics.push('msh/+/2/e/LongFast/#');
    }
  }
  return topics;
}

// ─── Configuration ───────────────────────────────────────────────────────────
const CONFIG = {
  port: process.env.PORT || 3000,
  mqtt: {
    broker: 'mqtt://mqtt.meshtastic.org:1883',
    topics: getMqttTopics(),
    options: {
      clientId: `meshtastic-map-${Math.random().toString(16).slice(2, 10)}`,
      clean: true,
      connectTimeout: 30000,
      reconnectPeriod: 5000,
      username: 'meshdev',
      password: 'large4cats',
      protocolVersion: 4,
    }
  },
  defaultKey: Buffer.from('AQ==', 'base64'),
};

// ─── In-Memory Node Store ────────────────────────────────────────────────────
const nodes = new Map();
let stats = { totalPackets: 0, positionPackets: 0, nodesWithPosition: 0 };
let SERVER_STARTED_AT = Date.now();

// ─── Time-Series History ─────────────────────────────────────────────────────
// Records a snapshot every 10 seconds. Keeps all data since server start.
const history = []; // { t: timestamp, n: nodeCount, p: totalPackets }
const HISTORY_INTERVAL = 10000; // 10s

// ─── Persistence (optional) ──────────────────────────────────────────────────
// Config: DATA_SAVE=true (default). Also accepts data-save=true.
const DATA_SAVE_ENABLED = parseBoolEnv('DATA_SAVE', parseBoolEnv('data-save', true));
const DB_TYPE = process.env.DATABASE_TYPE || 'local';
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/meshtastic';
const DB_PATH = process.env.MESHTASTIC_DB_PATH
  ? path.resolve(process.env.MESHTASTIC_DB_PATH)
  : path.join(__dirname, 'meshtastic-data.db');
const DB_FLUSH_INTERVAL_MS = Number(process.env.MESHTASTIC_DB_FLUSH_INTERVAL_MS || 30000);
const DB_MAX_NODES = Number(process.env.MESHTASTIC_DB_MAX_NODES || 50000);
const NODE_INACTIVE_MINUTES = parseNumberEnv('NODE_INACTIVE', 24 * 60);
const NODE_INACTIVE_MS = Math.max(1, NODE_INACTIVE_MINUTES) * 60 * 1000;

let db = null;
let mongoClient = null;
let mongoDb = null;
let stmtMetaGet, stmtMetaSet, stmtNodeUpsert, stmtNodeDelete, stmtNodeCount, stmtNodePrune, stmtHistoryUpsert;
const dirtyNodeIds = new Set();
let dirtyStats = false;
let pendingHistory = [];

async function initDbAndLoad() {
  if (!DATA_SAVE_ENABLED) return;

  try {
    if (DB_TYPE === 'mongodb') {
      mongoClient = new MongoClient(MONGODB_URI);
      await mongoClient.connect();
      mongoDb = mongoClient.db();
      
      const metaCol = mongoDb.collection('meta');
      const nodesCol = mongoDb.collection('nodes');
      const historyCol = mongoDb.collection('history');

      await nodesCol.createIndex({ lastHeard: 1 });

      const startedAtRow = await metaCol.findOne({ _id: 'startedAt' });
      if (startedAtRow && startedAtRow.value) {
        const parsed = Number(startedAtRow.value);
        if (Number.isFinite(parsed) && parsed > 0) SERVER_STARTED_AT = parsed;
      } else {
        await metaCol.updateOne({ _id: 'startedAt' }, { $set: { value: String(SERVER_STARTED_AT) } }, { upsert: true });
      }

      const totalPacketsRow = await metaCol.findOne({ _id: 'totalPackets' });
      if (totalPacketsRow && totalPacketsRow.value) {
        const parsed = Number(totalPacketsRow.value);
        if (Number.isFinite(parsed) && parsed >= 0) stats.totalPackets = parsed;
      }

      const positionPacketsRow = await metaCol.findOne({ _id: 'positionPackets' });
      if (positionPacketsRow && positionPacketsRow.value) {
        const parsed = Number(positionPacketsRow.value);
        if (Number.isFinite(parsed) && parsed >= 0) stats.positionPackets = parsed;
      }

      const cutoff = Date.now() - NODE_INACTIVE_MS;
      await nodesCol.deleteMany({ lastHeard: { $lt: cutoff } });

      const nodeRows = await nodesCol.find({}).toArray();
      nodes.clear();
      stats.nodesWithPosition = 0;
      for (const r of nodeRows) {
        try {
          const n = JSON.parse(r.json);
          if (!n || !n.id) continue;
          if (!isNodeActive(n)) continue;
          nodes.set(n.id, n);
          if (n.position) stats.nodesWithPosition++;
        } catch {}
      }

      history.length = 0;
      const histRows = await historyCol.find({}).sort({ t: 1 }).toArray();
      for (const hr of histRows) {
        history.push({ t: hr.t, n: hr.n, p: hr.p });
      }
      
      console.log(`MongoDB loaded: ${nodes.size} nodes (${stats.nodesWithPosition} w/pos), ${history.length} history points`);
    } else {
      db = new DatabaseSync(DB_PATH);
      db.exec('PRAGMA journal_mode=WAL;');
      db.exec('PRAGMA synchronous=NORMAL;');
      db.exec('PRAGMA temp_store=MEMORY;');

      db.exec(`
        CREATE TABLE IF NOT EXISTS meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS nodes (
          id TEXT PRIMARY KEY,
          lastHeard INTEGER NOT NULL,
          json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_nodes_lastHeard ON nodes(lastHeard);
        CREATE TABLE IF NOT EXISTS history (
          t INTEGER PRIMARY KEY,
          n INTEGER NOT NULL,
          p INTEGER NOT NULL
        );
      `);

      stmtMetaGet = db.prepare('SELECT value FROM meta WHERE key = ?');
      stmtMetaSet = db.prepare('INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value');
      stmtNodeUpsert = db.prepare('INSERT INTO nodes(id, lastHeard, json) VALUES(?, ?, ?) ON CONFLICT(id) DO UPDATE SET lastHeard = excluded.lastHeard, json = excluded.json');
      stmtNodeDelete = db.prepare('DELETE FROM nodes WHERE id = ?');
      stmtNodeCount = db.prepare('SELECT COUNT(*) AS c FROM nodes');
      stmtNodePrune = db.prepare('DELETE FROM nodes WHERE id IN (SELECT id FROM nodes ORDER BY lastHeard ASC LIMIT ?)');
      stmtHistoryUpsert = db.prepare('INSERT INTO history(t, n, p) VALUES(?, ?, ?) ON CONFLICT(t) DO UPDATE SET n = excluded.n, p = excluded.p');
      const stmtDeleteInactive = db.prepare('DELETE FROM nodes WHERE lastHeard < ?');

      const startedAtRow = stmtMetaGet.get('startedAt');
      if (startedAtRow && startedAtRow.value) {
        const parsed = Number(startedAtRow.value);
        if (Number.isFinite(parsed) && parsed > 0) SERVER_STARTED_AT = parsed;
      } else {
        stmtMetaSet.run('startedAt', String(SERVER_STARTED_AT));
      }

      const totalPacketsRow = stmtMetaGet.get('totalPackets');
      if (totalPacketsRow && totalPacketsRow.value) {
        const parsed = Number(totalPacketsRow.value);
        if (Number.isFinite(parsed) && parsed >= 0) stats.totalPackets = parsed;
      }

      const positionPacketsRow = stmtMetaGet.get('positionPackets');
      if (positionPacketsRow && positionPacketsRow.value) {
        const parsed = Number(positionPacketsRow.value);
        if (Number.isFinite(parsed) && parsed >= 0) stats.positionPackets = parsed;
      }

      const cutoff = Date.now() - NODE_INACTIVE_MS;
      stmtDeleteInactive.run(cutoff);

      const nodeRows = db.prepare('SELECT json FROM nodes').all();
      nodes.clear();
      stats.nodesWithPosition = 0;
      for (const r of nodeRows) {
        try {
          const n = JSON.parse(r.json);
          if (!n || !n.id) continue;
          if (!isNodeActive(n)) continue;
          nodes.set(n.id, n);
          if (n.position) stats.nodesWithPosition++;
        } catch {}
      }

      history.length = 0;
      const histRows = db.prepare('SELECT t, n, p FROM history ORDER BY t ASC').all();
      for (const hr of histRows) {
        history.push({ t: hr.t, n: hr.n, p: hr.p });
      }

      console.log(`DB loaded: ${nodes.size} nodes (${stats.nodesWithPosition} w/pos), ${history.length} history points from ${DB_PATH}`);
    }
  } catch (err) {
    console.error('DB init/load failed:', err.message);
    db = null;
    mongoDb = null;
  }
}

function markNodeDirty(nodeId) {
  if (!db && !mongoDb) return;
  dirtyNodeIds.add(nodeId);
}

function isNodeActive(node, now = Date.now()) {
  return !!(node && node.lastHeard && (now - node.lastHeard) <= NODE_INACTIVE_MS);
}

let isFlushing = false;
async function flushDb() {
  if (!db && !mongoDb) return;
  if (dirtyNodeIds.size === 0 && !dirtyStats && pendingHistory.length === 0) return;
  if (isFlushing) return;
  
  isFlushing = true;
  try {
    if (mongoDb) {
      const metaCol = mongoDb.collection('meta');
      const nodesCol = mongoDb.collection('nodes');
      const historyCol = mongoDb.collection('history');
      
      const bulkNodes = [];
      for (const id of dirtyNodeIds) {
        const n = nodes.get(id);
        if (!n || !n.position) {
          bulkNodes.push({ deleteOne: { filter: { _id: id } } });
        } else {
          bulkNodes.push({
            updateOne: {
              filter: { _id: id },
              update: { $set: { lastHeard: n.lastHeard || Date.now(), json: JSON.stringify(serializeNode(n)) } },
              upsert: true
            }
          });
        }
      }
      dirtyNodeIds.clear();
      if (bulkNodes.length > 0) {
        await nodesCol.bulkWrite(bulkNodes);
      }

      if (dirtyStats) {
        const bulkMeta = [
          { updateOne: { filter: { _id: 'totalPackets' }, update: { $set: { value: String(stats.totalPackets) } }, upsert: true } },
          { updateOne: { filter: { _id: 'positionPackets' }, update: { $set: { value: String(stats.positionPackets) } }, upsert: true } },
          { updateOne: { filter: { _id: 'nodesWithPosition' }, update: { $set: { value: String(stats.nodesWithPosition) } }, upsert: true } },
        ];
        await metaCol.bulkWrite(bulkMeta);
        dirtyStats = false;
      }

      if (pendingHistory.length > 0) {
        const bulkHistory = pendingHistory.map(hp => ({
          updateOne: {
            filter: { t: hp.t },
            update: { $set: { n: hp.n, p: hp.p } },
            upsert: true
          }
        }));
        await historyCol.bulkWrite(bulkHistory);
        pendingHistory = [];
      }

      const count = await nodesCol.countDocuments();
      if (count > DB_MAX_NODES) {
        const excess = count - DB_MAX_NODES;
        const oldest = await nodesCol.find({}).sort({ lastHeard: 1 }).limit(excess).toArray();
        if (oldest.length > 0) {
          const idsToRemove = oldest.map(doc => doc._id);
          await nodesCol.deleteMany({ _id: { $in: idsToRemove } });
        }
      }
      
    } else {
      db.exec('BEGIN');

      for (const id of dirtyNodeIds) {
        const n = nodes.get(id);
        if (!n || !n.position) {
          stmtNodeDelete.run(id);
        } else {
          stmtNodeUpsert.run(id, n.lastHeard || Date.now(), JSON.stringify(serializeNode(n)));
        }
      }
      dirtyNodeIds.clear();

      if (dirtyStats) {
        stmtMetaSet.run('totalPackets', String(stats.totalPackets));
        stmtMetaSet.run('positionPackets', String(stats.positionPackets));
        stmtMetaSet.run('nodesWithPosition', String(stats.nodesWithPosition));
        dirtyStats = false;
      }

      for (const hp of pendingHistory) {
        stmtHistoryUpsert.run(hp.t, hp.n, hp.p);
      }
      pendingHistory = [];

      const count = stmtNodeCount.get().c;
      if (count > DB_MAX_NODES) {
        stmtNodePrune.run(count - DB_MAX_NODES);
      }

      db.exec('COMMIT');
    }
  } catch (err) {
    if (db) try { db.exec('ROLLBACK'); } catch {}
    console.error('DB flush failed:', err.message);
  } finally {
    isFlushing = false;
  }
}
function safeNumber(n, fallback = 0) {
  return Number.isFinite(n) ? n : fallback;
}

// Legacy JSON cache (removed). Kept as no-op for backwards compatibility if older code calls it.
function loadCacheFromDisk() {}
function writeCacheToDisk() {}

function recordHistory() {
  const point = { t: Date.now(), n: stats.nodesWithPosition, p: stats.totalPackets };
  history.push(point);
  if (db || mongoDb) pendingHistory.push(point);
}

// ─── Broadcast Throttle ─────────────────────────────────────────────────────
let pendingUpdates = new Map();
let broadcastTimer = null;
const BROADCAST_INTERVAL = 500;
let openClientCount = 0;

function queueUpdate(update) {
  const nodeId = update && ((update.node && update.node.id) || update.id);
  if (nodeId) {
    pendingUpdates.set(nodeId, update);
  } else {
    pendingUpdates.set(`_u_${pendingUpdates.size}`, update);
  }
  if (!broadcastTimer) {
    broadcastTimer = setTimeout(flushUpdates, BROADCAST_INTERVAL);
  }
}

function flushUpdates() {
  broadcastTimer = null;
  if (pendingUpdates.size === 0) return;
  if (openClientCount === 0) {
    pendingUpdates.clear();
    return;
  }
  const updates = Array.from(pendingUpdates.values());
  if (updates.length === 1) {
    broadcastToClients(updates[0]);
  } else {
    broadcastToClients({ type: 'batch', updates, stats });
  }
  pendingUpdates.clear();
}

// ─── Protobuf Setup ─────────────────────────────────────────────────────────
let ServiceEnvelope, MeshPacket, Data, Position, User, Telemetry, NodeInfo, NeighborInfo, MapReport, PortNum;

async function loadProtos() {
  const root = new protobuf.Root();
  root.resolvePath = (origin, target) => {
    return path.join(__dirname, 'protos', target);
  };
  await root.load([
    'meshtastic/mqtt.proto',
    'meshtastic/mesh.proto',
    'meshtastic/portnums.proto',
  ]);
  ServiceEnvelope = root.lookupType('meshtastic.ServiceEnvelope');
  MeshPacket = root.lookupType('meshtastic.MeshPacket');
  Data = root.lookupType('meshtastic.Data');
  Position = root.lookupType('meshtastic.Position');
  User = root.lookupType('meshtastic.User');
  Telemetry = root.lookupType('meshtastic.Telemetry');
  NodeInfo = root.lookupType('meshtastic.NodeInfo');
  NeighborInfo = root.lookupType('meshtastic.NeighborInfo');
  MapReport = root.lookupType('meshtastic.MapReport');
  PortNum = root.lookupEnum('meshtastic.PortNum');
  console.log('✅ Protobuf definitions loaded');
}

// ─── Decryption ──────────────────────────────────────────────────────────────
function normalizeAes256Key(keyBuf) {
  let k = Buffer.isBuffer(keyBuf) ? keyBuf : Buffer.from(keyBuf || '');

  if (k.length === 1) {
    const expanded = Buffer.alloc(16, k[0]);
    k = Buffer.concat([expanded, expanded]);
  } else if (k.length === 16) {
    k = Buffer.concat([k, k]);
  }

  if (k.length < 32) {
    const padded = Buffer.alloc(32);
    k.copy(padded);
    k = padded;
  }

  return k.slice(0, 32);
}

const DEFAULT_AES_KEY_32 = normalizeAes256Key(CONFIG.defaultKey);

function decrypt(encryptedBytes, packetId, fromId, key32) {
  try {
    const nonce = Buffer.alloc(16);
    nonce.writeUInt32LE(packetId, 0);
    nonce.writeUInt32LE(fromId, 4);

    const decipher = crypto.createDecipheriv('aes-256-ctr', key32, nonce);
    return Buffer.concat([decipher.update(encryptedBytes), decipher.final()]);
  } catch (err) {
    return null;
  }
}

// ─── Packet Processing ──────────────────────────────────────────────────────
function nodeIdToHex(id) {
  return '!' + id.toString(16).padStart(8, '0');
}

function processPosition(pos, nodeId) {
  if (!pos) return null;
  const lat = pos.latitudeI / 1e7;
  const lon = pos.longitudeI / 1e7;
  if (lat === 0 && lon === 0) return null;
  if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return null;
  return { lat, lon, altitude: pos.altitude || 0, time: pos.time || 0, satsInView: pos.satsInView || 0 };
}

function distanceMeters(aLat, aLon, bLat, bLon) {
  const toRad = (x) => x * Math.PI / 180;
  const R = 6371000;
  const dLat = toRad(bLat - aLat);
  const dLon = toRad(bLon - aLon);
  const lat1 = toRad(aLat);
  const lat2 = toRad(bLat);
  const s1 = Math.sin(dLat / 2);
  const s2 = Math.sin(dLon / 2);
  const h = s1 * s1 + Math.cos(lat1) * Math.cos(lat2) * s2 * s2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
}

function shouldAcceptPosition(node, nextPosition) {
  if (!nextPosition) return false;
  if (!node.position) return true;

  const now = Date.now();
  const lastAt = node._posAcceptedAt || 0;
  const dt = lastAt ? (now - lastAt) : 0;
  const meters = distanceMeters(node.position.lat, node.position.lon, nextPosition.lat, nextPosition.lon);

  // Reject obvious outliers that create "teleporting" pins on the map.
  // Allow large moves when the time gap is big (node actually traveled).
  if (dt > 0) {
    const speed = meters / (dt / 1000); // m/s
    const isTeleport = meters > 2000 && speed > 200; // >2km at >720km/h
    if (isTeleport) return false;
  }

  return true;
}

function applyPosition(node, position) {
  if (!position) return false;
  if (!shouldAcceptPosition(node, position)) return false;
  const hadPosition = !!node.position;
  node.position = position;
  node._posAcceptedAt = Date.now();
  stats.positionPackets++;
  if (!hadPosition) stats.nodesWithPosition++;
  markNodeDirty(node.id);
  dirtyStats = true;
  return true;
}

function processPacket(envelope) {
  const packet = envelope.packet;
  if (!packet) return;
  stats.totalPackets++;
  dirtyStats = true;

  const fromId = packet.from;
  const nodeHex = nodeIdToHex(fromId);
  const packetTime = packet.rxTime ? packet.rxTime * 1000 : Date.now();

  let data = null;

  if (packet.decoded) {
    data = packet.decoded;
  } else if (packet.encrypted && packet.encrypted.length > 0) {
    const decrypted = decrypt(packet.encrypted, packet.id, packet.from, DEFAULT_AES_KEY_32);
    if (decrypted) {
      try {
        data = Data.decode(decrypted);
      } catch (e) {
        return;
      }
    }
  }

  if (!data || !data.portnum) return;

  if (!nodes.has(nodeHex)) {
    nodes.set(nodeHex, {
      id: nodeHex,
      num: fromId,
      lastHeard: packetTime,
      position: null,
      user: null,
      telemetry: null,
      snr: packet.rxSnr || 0,
      rssi: packet.rxRssi || 0,
      viaMqtt: packet.viaMqtt || false,
      hopStart: packet.hopStart || 0,
    });
  }
  const node = nodes.get(nodeHex);
  node.lastHeard = Math.max(node.lastHeard || 0, packetTime);
  node.snr = packet.rxSnr || node.snr;
  node.rssi = packet.rxRssi || node.rssi;

  let update = null;

  const portnumValue = typeof data.portnum === 'number' ? data.portnum : data.portnum;

  switch (portnumValue) {
    case 3: // POSITION_APP
      try {
        const pos = Position.decode(data.payload);
        const processed = processPosition(pos, nodeHex);
        if (applyPosition(node, processed)) {
          update = { type: 'position', node: serializeNode(node) };
        }
      } catch (e) {}
      break;

    case 4: // NODEINFO_APP
      try {
        const userInfo = User.decode(data.payload);
        node.user = {
          id: userInfo.id,
          longName: userInfo.longName || '',
          shortName: userInfo.shortName || '',
          hwModel: userInfo.hwModel || 0,
          role: userInfo.role || 0,
        };
        markNodeDirty(node.id);
        update = { type: 'nodeinfo', node: serializeNode(node) };
      } catch (e) {}
      break;

    case 67: // TELEMETRY_APP
      try {
        const telemetry = Telemetry.decode(data.payload);
        if (telemetry.deviceMetrics) {
          node.telemetry = {
            batteryLevel: telemetry.deviceMetrics.batteryLevel || 0,
            voltage: telemetry.deviceMetrics.voltage || 0,
            channelUtilization: telemetry.deviceMetrics.channelUtilization || 0,
            airUtilTx: telemetry.deviceMetrics.airUtilTx || 0,
            uptimeSeconds: telemetry.deviceMetrics.uptimeSeconds || 0,
          };
          markNodeDirty(node.id);
          update = { type: 'telemetry', node: serializeNode(node) };
        }
      } catch (e) {}
      break;

    case 73: // MAP_REPORT_APP
      try {
        const mapReport = MapReport.decode(data.payload);
        if (mapReport.latitudeI && mapReport.longitudeI) {
          const lat = mapReport.latitudeI / 1e7;
          const lon = mapReport.longitudeI / 1e7;
          if (Math.abs(lat) <= 90 && Math.abs(lon) <= 180 && !(lat === 0 && lon === 0)) {
            applyPosition(node, { lat, lon, altitude: mapReport.altitude || 0, time: 0, satsInView: 0 });
          }
        }
        node.user = node.user || {};
        node.user.longName = mapReport.longName || node.user.longName || '';
        node.user.shortName = mapReport.shortName || node.user.shortName || '';
        node.user.hwModel = mapReport.hwModel || node.user.hwModel || 0;
        node.user.role = mapReport.role || node.user.role || 0;
        node.mapReport = {
          firmwareVersion: mapReport.firmwareVersion || '',
          region: mapReport.region || 0,
          modemPreset: mapReport.modemPreset || 0,
          numOnlineLocalNodes: mapReport.numOnlineLocalNodes || 0,
        };
        markNodeDirty(node.id);
        update = { type: 'mapreport', node: serializeNode(node) };
      } catch (e) {}
      break;

    case 71: // NEIGHBORINFO_APP
      try {
        const neighborInfo = NeighborInfo.decode(data.payload);
        node.neighbors = (neighborInfo.neighbors || []).map(n => ({
          nodeId: nodeIdToHex(n.nodeId),
          snr: n.snr || 0,
        }));
        markNodeDirty(node.id);
        update = { type: 'neighborinfo', node: serializeNode(node) };
      } catch (e) {}
      break;
  }

  if (update) {
    queueUpdate(update);
  }
}

function serializeNode(node) {
  return {
    id: node.id,
    num: node.num,
    lastHeard: node.lastHeard,
    position: node.position,
    user: node.user,
    telemetry: node.telemetry,
    snr: node.snr,
    rssi: node.rssi,
    mapReport: node.mapReport || null,
    neighbors: node.neighbors || [],
  };
}

// Compact serialization — strips nulls and empty arrays for faster transfer
function serializeNodeCompact(node) {
  const o = { id: node.id, lh: node.lastHeard };
  if (node.position) o.p = [node.position.lat, node.position.lon, node.position.altitude || 0];
  if (node.user) {
    o.u = {};
    if (node.user.longName) o.u.l = node.user.longName;
    if (node.user.shortName) o.u.s = node.user.shortName;
    if (node.user.hwModel) o.u.h = node.user.hwModel;
    if (node.user.role) o.u.r = node.user.role;
  }
  if (node.telemetry && node.telemetry.batteryLevel) o.bat = node.telemetry.batteryLevel;
  if (node.mapReport && node.mapReport.firmwareVersion) o.fw = node.mapReport.firmwareVersion;
  if (node.snr) o.snr = node.snr;
  return o;
}

// ─── WebSocket Broadcasting ──────────────────────────────────────────────────
let wss;

function broadcastToClients(data) {
  if (!wss || openClientCount === 0) return;
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

// ─── Express + HTTP Server ───────────────────────────────────────────────────
const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/nodes', (req, res) => {
  const allNodes = [...nodes.values()]
    .filter(n => n.position && isNodeActive(n))
    .map(serializeNode);
  res.json({ nodes: allNodes, stats });
});

app.get('/api/stats', (req, res) => {
  res.json(stats);
});

app.get('/api/history', (req, res) => {
  res.json({ startedAt: SERVER_STARTED_AT, history });
});

// ─── Bootstrap ───────────────────────────────────────────────────────────────
async function main() {
  await loadProtos();
  await initDbAndLoad();

  const server = http.createServer(app);

  // WebSocket server
  wss = new WebSocket.Server({ server });
  wss.on('connection', (ws) => {
    console.log('🌐 New WebSocket client connected');
    openClientCount++;
    // Send compact initial payload with history
    const allNodes = [...nodes.values()].filter(n => n.position && isNodeActive(n)).map(serializeNodeCompact);
    ws.send(JSON.stringify({
      type: 'init',
      nodes: allNodes,
      stats,
      startedAt: SERVER_STARTED_AT,
      history,
    }));

    ws.on('close', () => {
      console.log('🌐 WebSocket client disconnected');
      openClientCount = Math.max(0, openClientCount - 1);
    });
  });

  // MQTT connection
  console.log(`🔌 Connecting to MQTT broker: ${CONFIG.mqtt.broker}`);
  console.log(`📡 MQTT topics: ${CONFIG.mqtt.topics.join(', ')}`);
  const mqttClient = mqtt.connect(CONFIG.mqtt.broker, CONFIG.mqtt.options);

  mqttClient.on('connect', () => {
    console.log('✅ Connected to Meshtastic MQTT broker');
    CONFIG.mqtt.topics.forEach(topic => {
      mqttClient.subscribe(topic, (err) => {
        if (err) {
          console.error(`❌ Failed to subscribe to ${topic}:`, err);
        } else {
          console.log(`📡 Subscribed to: ${topic}`);
        }
      });
    });
  });

  let msgCount = 0;
  let lastLogAt = 0;
  mqttClient.on('message', (topic, message) => {
    msgCount++;
    const now = Date.now();
    if (msgCount <= 5 || now - lastLogAt >= 15000) {
      lastLogAt = now;
      console.log(`📨 MQTT msgs=${msgCount} topic=${topic} len=${message.length}`);
    }
    try {
      const envelope = ServiceEnvelope.decode(message);
      processPacket(envelope);
    } catch (err) {
      if (msgCount <= 5) {
        console.error(`❌ Decode error on msg #${msgCount}:`, err.message);
      }
    }
  });

  mqttClient.on('error', (err) => {
    console.error('❌ MQTT error:', err.message);
  });

  mqttClient.on('reconnect', () => {
    console.log('🔄 Reconnecting to MQTT broker...');
  });

  // Start HTTP server
  server.listen(CONFIG.port, () => {
    console.log(`\n📊 Meshtastic Statistics running at http://localhost:${CONFIG.port}`);
    console.log('─'.repeat(50));
  });

  // Record history snapshot every 10s
  setInterval(recordHistory, HISTORY_INTERVAL);
  // Record first point immediately
  recordHistory();

  // Periodic DB flush
  if (db || mongoDb) {
    setInterval(flushDb, DB_FLUSH_INTERVAL_MS);
  }

  // Periodic stats logging
  setInterval(() => {
    console.log(`📊 Nodes: ${nodes.size} | With Position: ${stats.nodesWithPosition} | Total Packets: ${stats.totalPackets} | History Points: ${history.length}`);
  }, 30000);

  // Periodic cleanup of stale nodes (every 10 minutes)
  setInterval(() => {
    const cutoff = Date.now() - NODE_INACTIVE_MS;
    for (const [key, node] of nodes) {
      if (node.lastHeard < cutoff) {
        if (node.position) stats.nodesWithPosition--;
        queueUpdate({ type: 'remove', id: key });
        nodes.delete(key);
        markNodeDirty(key);
        dirtyStats = true;
      }
    }
  }, 10 * 60 * 1000);

  // Periodic full-node broadcast to all clients (every 10 minutes)
  // Ensures connected clients always have fresh data even if no new MQTT packets arrive
  setInterval(() => {
    if (openClientCount === 0) return;
    const allNodes = [...nodes.values()]
      .filter(n => n.position && isNodeActive(n))
      .map(serializeNode);
    const payload = JSON.stringify({ type: 'refresh', nodes: allNodes, stats });
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    });
    console.log(`🔄 Broadcast full refresh to ${openClientCount} client(s): ${allNodes.length} nodes`);
  }, 10 * 60 * 1000);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});

process.on('SIGINT', () => {
  try { flushDb(); } catch {}
  process.exit(0);
});
