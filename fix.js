const fs = require('fs');

let code = fs.readFileSync('server.js', 'utf8');

code = code.replace("const { DatabaseSync } = require('node:sqlite');",
  "const { DatabaseSync } = require('node:sqlite');\nconst { MongoClient } = require('mongodb');");

const startToken = '// ─── Persistence (optional) ──────────────────────────────────────────────────';
const endToken = 'function safeNumber(n, fallback = 0)';

const oldDbBlock = code.substring(
  code.indexOf(startToken),
  code.indexOf(endToken)
);

let newDbBlock = `// ─── Persistence (optional) ──────────────────────────────────────────────────
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
      
      console.log(\`MongoDB loaded: \${nodes.size} nodes (\${stats.nodesWithPosition} w/pos), \${history.length} history points\`);
    } else {
      db = new DatabaseSync(DB_PATH);
      db.exec('PRAGMA journal_mode=WAL;');
      db.exec('PRAGMA synchronous=NORMAL;');
      db.exec('PRAGMA temp_store=MEMORY;');

      db.exec(\`
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
      \`);

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

      console.log(\`DB loaded: \${nodes.size} nodes (\${stats.nodesWithPosition} w/pos), \${history.length} history points from \${DB_PATH}\`);
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
`;

code = code.replace(oldDbBlock, newDbBlock);
code = code.replace(
  '  await loadProtos();\n  initDbAndLoad();',
  '  await loadProtos();\n  await initDbAndLoad();'
);
code = code.replace(
  '  if (db) {\n    setInterval(flushDb, DB_FLUSH_INTERVAL_MS);\n  }',
  '  if (db || mongoDb) {\n    setInterval(flushDb, DB_FLUSH_INTERVAL_MS);\n  }'
);

fs.writeFileSync('server.js', code);
console.log('patched');
