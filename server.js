const express = require('express');
const http = require('http');
const mqtt = require('mqtt');
const protobuf = require('protobufjs');
const crypto = require('crypto');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

function parseBoolEnv(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null) return defaultValue;
  const v = String(raw).trim().toLowerCase();
  if (v === '1' || v === 'true' || v === 'yes' || v === 'y' || v === 'on') return true;
  if (v === '0' || v === 'false' || v === 'no' || v === 'n' || v === 'off') return false;
  return defaultValue;
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
const SERVER_STARTED_AT = Date.now();

// ─── Time-Series History ─────────────────────────────────────────────────────
// Records a snapshot every 10 seconds. Keeps all data since server start.
const history = []; // { t: timestamp, n: nodeCount, p: totalPackets }
const HISTORY_INTERVAL = 10000; // 10s

// ─── Persistence (optional) ──────────────────────────────────────────────────
const CACHE_ENABLED = parseBoolEnv('MESHTASTIC_CACHE_ENABLED', true);
const CACHE_PATH = process.env.MESHTASTIC_CACHE_PATH
  ? path.resolve(process.env.MESHTASTIC_CACHE_PATH)
  : path.join(__dirname, 'nodes-cache.json');
const CACHE_SAVE_INTERVAL_MS = Number(process.env.MESHTASTIC_CACHE_SAVE_INTERVAL_MS || 60000);
const CACHE_MAX_NODES = Number(process.env.MESHTASTIC_CACHE_MAX_NODES || 25000);
let cacheDirty = false;

function safeNumber(n, fallback = 0) {
  return Number.isFinite(n) ? n : fallback;
}

function loadCacheFromDisk() {
  if (!CACHE_ENABLED) return;
  try {
    if (!fs.existsSync(CACHE_PATH)) return;
    const raw = fs.readFileSync(CACHE_PATH, 'utf8');
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.nodes)) return;

    nodes.clear();
    stats.nodesWithPosition = 0;

    for (const n of parsed.nodes) {
      if (!n || !n.id) continue;
      const node = {
        id: String(n.id),
        num: safeNumber(n.num, 0),
        lastHeard: safeNumber(n.lastHeard, 0),
        position: n.position || null,
        user: n.user || null,
        telemetry: n.telemetry || null,
        snr: safeNumber(n.snr, 0),
        rssi: safeNumber(n.rssi, 0),
        mapReport: n.mapReport || null,
        neighbors: Array.isArray(n.neighbors) ? n.neighbors : [],
      };
      nodes.set(node.id, node);
      if (node.position) stats.nodesWithPosition++;
    }

    console.log(`💾 Loaded cache: ${nodes.size} nodes (${stats.nodesWithPosition} with position) from ${CACHE_PATH}`);
  } catch (err) {
    console.error('💾 Cache load failed:', err.message);
  }
}

function writeCacheToDisk() {
  if (!CACHE_ENABLED || !cacheDirty) return;
  cacheDirty = false;

  try {
    const list = [...nodes.values()]
      .filter(n => n.position)
      .sort((a, b) => (b.lastHeard || 0) - (a.lastHeard || 0))
      .slice(0, CACHE_MAX_NODES)
      .map(serializeNode);

    const tmp = `${CACHE_PATH}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ t: Date.now(), nodes: list }), 'utf8');
    fs.renameSync(tmp, CACHE_PATH);
  } catch (err) {
    console.error('💾 Cache save failed:', err.message);
  }
}

function recordHistory() {
  history.push({
    t: Date.now(),
    n: stats.nodesWithPosition,
    p: stats.totalPackets,
  });
}

// ─── Broadcast Throttle ─────────────────────────────────────────────────────
let pendingUpdates = new Map();
let broadcastTimer = null;
const BROADCAST_INTERVAL = 500;
let openClientCount = 0;

function queueUpdate(update) {
  const nodeId = update && update.node && update.node.id;
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

function applyPosition(node, position) {
  if (!position) return false;
  const hadPosition = !!node.position;
  node.position = position;
  stats.positionPackets++;
  if (!hadPosition) stats.nodesWithPosition++;
  cacheDirty = true;
  return true;
}

function processPacket(envelope) {
  const packet = envelope.packet;
  if (!packet) return;
  stats.totalPackets++;

  const fromId = packet.from;
  const nodeHex = nodeIdToHex(fromId);

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
      lastHeard: Date.now(),
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
  node.lastHeard = Date.now();
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
        cacheDirty = true;
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
          cacheDirty = true;
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
        cacheDirty = true;
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
        cacheDirty = true;
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
    .filter(n => n.position)
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
  loadCacheFromDisk();

  const server = http.createServer(app);

  // WebSocket server
  wss = new WebSocket.Server({ server });
  wss.on('connection', (ws) => {
    console.log('🌐 New WebSocket client connected');
    openClientCount++;
    // Send compact initial payload with history
    const allNodes = [...nodes.values()].filter(n => n.position).map(serializeNodeCompact);
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

  // Periodic cache save
  if (CACHE_ENABLED) {
    setInterval(writeCacheToDisk, CACHE_SAVE_INTERVAL_MS);
  }

  // Periodic stats logging
  setInterval(() => {
    console.log(`📊 Nodes: ${nodes.size} | With Position: ${stats.nodesWithPosition} | Total Packets: ${stats.totalPackets} | History Points: ${history.length}`);
  }, 30000);

  // Periodic cleanup of stale nodes (older than 24 hours)
  setInterval(() => {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    for (const [key, node] of nodes) {
      if (node.lastHeard < cutoff) {
        if (node.position) stats.nodesWithPosition--;
        nodes.delete(key);
        cacheDirty = true;
      }
    }
  }, 60 * 60 * 1000);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});

process.on('SIGINT', () => {
  try { writeCacheToDisk(); } catch {}
  process.exit(0);
});
