require("dotenv").config();
const express = require("express");
const mongoose = require("mongoose");
const cors = require("cors");
const http = require("http");
const { WebSocketServer } = require("ws");
const bodyParser = require("body-parser");
const { Parser } = require("json2csv");
const cron = require("node-cron");
const fs = require("fs");
const path = require("path");
const MachineData = require("./models/machineData");
const LiveStatus = require("./models/LiveStatus");

const app = express();
const ALERT_THRESHOLD_MINUTES = 10;

// ─────────────────────────────────────────────
// Show memory usage every 5 minutes (helps find leaks)
setInterval(() => {
  const mu = process.memoryUsage();
  console.log(
    `[MEM ${new Date().toISOString()}] ` +
      `RSS: ${Math.round(mu.rss / 1024 / 1024)} MB | ` +
      `Heap used: ${Math.round(mu.heapUsed / 1024 / 1024)} MB | ` +
      `Heap total: ${Math.round(mu.heapTotal / 1024 / 1024)} MB`,
  );
}, 300000);

/* =========================================================
   TIME HELPERS
   ========================================================= */
function parseToUTC(value) {
  if (!value) return null;
  if (value instanceof Date) return value;
  if (typeof value === "string") {
    const hasTimezone = /Z$|[+-]\d{2}:\d{2}$/.test(value);
    if (!hasTimezone) {
      const dt = new Date(value);
      const pktOffset = 5 * 60 * 60 * 1000;
      return new Date(dt.getTime() - pktOffset);
    }
    const dt = new Date(value);
    return isNaN(dt.getTime()) ? null : dt;
  }
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d;
}

function utcToPKT(date) {
  if (!date) return null;
  const d = new Date(date);
  if (isNaN(d.getTime())) return null;
  const PKT_OFFSET = 5 * 60 * 60 * 1000;
  return new Date(d.getTime() + PKT_OFFSET).toISOString();
}

/* =========================================================
   Middleware
   ========================================================= */
app.use(
  cors({
    origin: "*",
    credentials: false,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  }),
);

app.use(bodyParser.json({ limit: "10mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "10mb" }));

app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} ${req.method} ${req.url}`);
  if (req.method === "POST" && req.body) {
    console.log(
      `📥 Request body length: ${Array.isArray(req.body) ? req.body.length : 1}`,
    );
  }
  next();
});

/* =========================================================
   MongoDB Connection (with automatic retry)
   ========================================================= */
const MONGO_URI =
  process.env.MONGO_URI || "mongodb://127.0.0.1:27017/factory_monitor";

const connectDB = async () => {
  try {
    await mongoose.connect(MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
      socketTimeoutMS: 45000,
    });
    console.log("✅ MongoDB Connected");

    await MachineData.createIndexes([
      { machineName: 1, timestamp: -1 },
      { timestamp: -1 },
    ]);
    console.log("✅ MachineData indexes created");

    await LiveStatus.createIndexes([{ machineName: 1 }]);
    console.log("✅ LiveStatus index ready");
  } catch (err) {
    console.error("❌ MongoDB Connection Error:", err.message);
    setTimeout(connectDB, 5000);
  }
};

connectDB();

/* =========================================================
   HTTP + WebSocket Server
   ========================================================= */
const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: "/ws/machine-data",
  verifyClient: (info, callback) => {
    console.log(`🔗 WebSocket attempt from: ${info.origin || "Unknown"}`);
    callback(true);
  },
});

wss.on("connection", (ws, req) => {
  console.log(`🔗 New WS connection from ${req.socket.remoteAddress}`);
  ws.on("close", () => {
    console.log(`🔗 WS closed from ${req.socket.remoteAddress}`);
  });
});

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  let count = 0;
  wss.clients.forEach((c) => {
    if (c.readyState === c.OPEN) {
      c.send(msg);
      count++;
    }
  });
  if (count > 0) console.log(`📡 Broadcasted to ${count} clients`);
}

/* =========================================================
   Duplicate filter - uses Set (lighter memory)
   ========================================================= */
function filterDuplicates(items) {
  if (items.length === 0) return items;
  console.log(`🔍 Checking ${items.length} items for duplicates...`);

  const uniqueItems = [];
  const seen = new Set();

  for (const item of items) {
    const tsUTC = parseToUTC(item.timestamp);
    if (!tsUTC || !item.machine) continue;

    const timeKey = Math.floor(tsUTC.getTime() / 1000);
    const key = `${item.machine}_${timeKey}_${item.status || "UNKNOWN"}`;

    if (!seen.has(key)) {
      seen.add(key);
      uniqueItems.push(item);
    }
  }

  console.log(`🔍 Kept ${uniqueItems.length} unique items`);
  return uniqueItems;
}

/* =========================================================
   Save batch of machine events
   ========================================================= */
async function saveBatch(items) {
  const uniqueItems = filterDuplicates(items);

  if (uniqueItems.length === 0) {
    console.log(`📭 All ${items.length} items were duplicates`);
    return [];
  }

  console.log(`💾 Processing ${uniqueItems.length} unique items`);

  const saved = [];
  const CHUNK_SIZE = 50;

  for (
    let startIdx = 0;
    startIdx < uniqueItems.length;
    startIdx += CHUNK_SIZE
  ) {
    const chunk = uniqueItems.slice(startIdx, startIdx + CHUNK_SIZE);
    console.log(
      `📦 Chunk ${Math.floor(startIdx / CHUNK_SIZE) + 1} (${chunk.length} items)`,
    );

    for (const item of chunk) {
      const {
        timestamp,
        machine,
        status,
        durationSeconds = 0,
        shift = null,
      } = item;
      const tsUTC = parseToUTC(timestamp);
      if (!tsUTC || !machine) continue;

      try {
        const roundedTimestamp = new Date(
          Math.floor(tsUTC.getTime() / 1000) * 1000,
        );

        const result = await MachineData.findOneAndUpdate(
          {
            machineName: machine,
            timestamp: {
              $gte: new Date(roundedTimestamp.getTime() - 1000),
              $lte: new Date(roundedTimestamp.getTime() + 1000),
            },
            status: status || "UNKNOWN",
          },
          {
            $setOnInsert: {
              timestamp: roundedTimestamp,
              machineName: machine,
              status: status || "UNKNOWN",
              machinePower: status === "RUNNING" || status === "DOWNTIME",
              downtime: status === "DOWNTIME",
              shift:
                shift ||
                (() => {
                  const hour = tsUTC.getUTCHours() + 5;
                  if (hour >= 7 && hour < 15) return "Morning";
                  if (hour >= 15 && hour < 23) return "Evening";
                  return "Night";
                })(),
              durationSeconds,
            },
            $set: { updatedAt: new Date() },
          },
          {
            upsert: true,
            new: true,
            runValidators: true,
            setDefaultsOnInsert: true,
          },
        );

        if (result.wasNew || !result._id) {
          console.log(
            `✅ Inserted: ${machine} ${roundedTimestamp.toISOString()} ${status}`,
          );
          saved.push(result);
        } else {
          console.log(
            `🔄 Updated: ${machine} ${roundedTimestamp.toISOString()}`,
          );
        }
      } catch (err) {
        if (err.code === 11000) {
          console.log(`⏭️ Duplicate prevented: ${machine}`);
        } else {
          console.error(`❌ Save error ${machine}: ${err.message}`);
        }
      }
    }

    chunk.length = 0; // help garbage collector
    await new Promise((r) => setImmediate(r));
  }

  if (saved.length > 0) {
    console.log(`📡 Broadcasting ${saved.length} items`);
    broadcast(
      saved.map((d) => ({
        type: "machine_update",
        machine: d.machineName,
        status: d.status,
        shift: d.shift,
        timestamp: utcToPKT(d.timestamp),
      })),
    );
  }

  return saved;
}

/* =========================================================
   Save live statuses (using bulkWrite - more efficient)
   ========================================================= */
async function saveLiveStatuses(items) {
  if (!items?.length) return;

  const ops = items
    .filter((item) => item.machine && item.status)
    .map((item) => ({
      updateOne: {
        filter: { machineName: item.machine },
        update: {
          $set: {
            status: item.status,
            updatedAt: parseToUTC(item.timestamp || new Date()),
          },
        },
        upsert: true,
      },
    }));

  if (ops.length === 0) return;

  try {
    await LiveStatus.bulkWrite(ops, { ordered: false });
    console.log(`📡 Bulk updated ${ops.length} live statuses`);

    broadcast({
      type: "live_status_update",
      data: items.map((i) => ({
        machine: i.machine,
        status: i.status,
        timestamp: utcToPKT(i.timestamp || new Date()),
      })),
    });
  } catch (err) {
    console.error("❌ Bulk live status error:", err.message);
  }
}

/* =========================================================
   ROUTES
   ========================================================= */
app.get("/", (_, res) => {
  res.json({
    message: "✅ Factory Monitoring Backend Running",
    version: "1.0",
    endpoints: {
      postData: "POST /api/machine-data",
      getData: "GET /api/machine-data?machine=&from=&to=&limit=",
      dashboard: "GET /api/dashboard/overview",
      stats: "GET /api/dashboard/stats",
      export: "GET /api/export?machine=&from=&to=",
    },
  });
});

app.post("/api/machine-data", async (req, res) => {
  const items = Array.isArray(req.body) ? req.body : [req.body];
  console.log(`📥 Received ${items.length} items`);

  const saved = await saveBatch(items);

  res.status(201).json({
    ok: true,
    saved: saved.length,
    received: items.length,
    timestamp: new Date().toISOString(),
  });
});

app.get("/api/dashboard/overview", async (_, res) => {
  try {
    const rows = await MachineData.aggregate([
      { $sort: { timestamp: -1 } },
      {
        $group: {
          _id: "$machineName",
          status: { $first: "$status" },
          timestamp: { $first: "$timestamp" },
          shift: { $first: "$shift" },
        },
      },
    ]);

    const result = rows.map((r) => ({
      machineName: r._id,
      latestStatus: r.status,
      lastTimestamp: utcToPKT(r.timestamp),
      shift: r.shift,
    }));

    console.log(`📊 Dashboard overview: ${result.length} machines`);
    res.json(result);
  } catch (err) {
    console.error("❌ Dashboard overview error:", err);
    res.status(500).json({ error: "Failed to get dashboard overview" });
  }
});

app.get("/api/dashboard/stats", async (_, res) => {
  try {
    const stats = await MachineData.aggregate([
      { $sort: { timestamp: -1 } },
      { $group: { _id: "$machineName", status: { $first: "$status" } } },
      { $group: { _id: "$status", count: { $sum: 1 } } },
    ]);

    const result = { RUNNING: 0, DOWNTIME: 0, OFF: 0, UNKNOWN: 0, total: 0 };

    stats.forEach((s) => {
      const status = s._id || "UNKNOWN";
      result[status] = s.count;
      result.total += s.count;
    });

    console.log(`📊 Dashboard stats:`, result);
    res.json(result);
  } catch (err) {
    console.error("❌ Dashboard stats error:", err);
    res.status(500).json({ error: "Failed to get dashboard stats" });
  }
});

app.get("/api/machines/status", async (_, res) => {
  try {
    const rows = await MachineData.aggregate([
      { $sort: { timestamp: -1 } },
      { $group: { _id: "$machineName", status: { $first: "$status" } } },
    ]);

    const result = {};
    rows.forEach((r) => {
      result[r._id] = r.status;
    });

    console.log(`📊 Real-time status: ${Object.keys(result).length} machines`);
    res.json(result);
  } catch (err) {
    console.error("❌ Machine status error:", err);
    res.status(500).json({ error: "Failed to get machine statuses" });
  }
});

app.get("/api/machine-data", async (req, res) => {
  const { machine, from, to, limit = 99999 } = req.query;
  console.log(
    `📤 GET request: machine=${machine}, from=${from}, to=${to}, limit=${limit}`,
  );

  const q = {};
  if (machine) q.machineName = machine;
  q.timestamp = {
    $gte: from ? parseToUTC(from) : new Date(Date.now() - 86400000),
    $lte: to ? parseToUTC(to) : new Date(),
  };

  try {
    const docs = await MachineData.find(q)
      .sort({ timestamp: -1 })
      .limit(Math.min(parseInt(limit), 99999))
      .lean();

    console.log(`📊 Found ${docs.length} documents`);

    const results = docs.map((d) => ({
      ...d,
      timestamp: utcToPKT(d.timestamp),
      _id: d._id.toString(),
    }));

    res.json(results);
  } catch (err) {
    console.error("❌ Error fetching data:", err);
    res
      .status(500)
      .json({ error: "Failed to fetch data", details: err.message });
  }
});

app.get("/api/export", async (req, res) => {
  const { from, to, machine } = req.query;
  console.log(`📥 Export request: machine=${machine}, from=${from}, to=${to}`);

  const q = {};
  if (machine) q.machineName = machine;
  if (from || to) {
    q.timestamp = {};
    if (from) q.timestamp.$gte = parseToUTC(from);
    if (to) q.timestamp.$lte = parseToUTC(to);
  }

  try {
    const docs = await MachineData.find(q).sort({ timestamp: 1 }).lean();

    if (docs.length === 0) {
      return res.status(404).json({ error: "No data found" });
    }

    const exportData = docs.map((d) => ({
      ...d,
      timestamp: utcToPKT(d.timestamp),
      _id: d._id.toString(),
    }));

    const csv = new Parser({
      fields: [
        "timestamp",
        "machineName",
        "status",
        "machinePower",
        "downtime",
        "shift",
        "durationSeconds",
      ],
    }).parse(exportData);

    const filename = `machine-data-${new Date().toISOString().slice(0, 10)}.csv`;

    res.header("Content-Type", "text/csv");
    res.attachment(filename);
    res.send(csv);

    console.log(`✅ Exported ${docs.length} records`);
  } catch (err) {
    console.error("❌ Export error:", err);
    res
      .status(500)
      .json({ error: "Failed to export data", details: err.message });
  }
});

app.get("/health", async (_, res) => {
  try {
    const dbStatus =
      mongoose.connection.readyState === 1 ? "connected" : "disconnected";
    const healthData = {
      status: "healthy",
      timestamp: new Date().toISOString(),
      database: dbStatus,
      websocketClients: wss.clients.size,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
    };

    if (dbStatus === "connected") {
      const totalRecords = await MachineData.countDocuments({});
      const latestRecord = await MachineData.findOne({}).sort({
        timestamp: -1,
      });
      healthData.totalRecords = totalRecords;
      healthData.latestRecord = latestRecord
        ? {
            machine: latestRecord.machineName,
            timestamp: utcToPKT(latestRecord.timestamp),
            status: latestRecord.status,
          }
        : null;
    }

    res.json(healthData);
  } catch (err) {
    console.error("❌ Health check error:", err);
    res.status(500).json({ status: "unhealthy", error: err.message });
  }
});

/* =========================================================
   RETENTION CRON - low memory streaming version
   ========================================================= */
cron.schedule("0 3 * * *", async () => {
  console.log("🔄 Running retention cron (streaming)...");
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - 3);

  const archiveDir = path.join(__dirname, "archives");
  if (!fs.existsSync(archiveDir)) fs.mkdirSync(archiveDir, { recursive: true });

  const archiveFile = path.join(archiveDir, `archive-${Date.now()}.json`);
  const writeStream = fs.createWriteStream(archiveFile);
  writeStream.write("[\n");

  let first = true;
  let idsToDelete = [];
  let count = 0;

  const cursor = MachineData.find({ timestamp: { $lte: cutoff } })
    .sort({ _id: 1 })
    .cursor();

  for await (const doc of cursor) {
    if (!first) writeStream.write(",\n");
    writeStream.write(JSON.stringify(doc));
    first = false;

    idsToDelete.push(doc._id);
    count++;

    if (idsToDelete.length >= 500) {
      await MachineData.deleteMany({ _id: { $in: idsToDelete } });
      idsToDelete = [];
      console.log(`Archived & deleted batch (${count} total)`);
    }
  }

  if (idsToDelete.length) {
    await MachineData.deleteMany({ _id: { $in: idsToDelete } });
  }

  writeStream.write("\n]");
  writeStream.end();

  console.log(`✅ Archived ${count} records to ${archiveFile}`);
});

/* =========================================================
   LIVE STATUS ENDPOINTS
   ========================================================= */
app.put("/api/live-status", async (req, res) => {
  const items = Array.isArray(req.body) ? req.body : [req.body];
  await saveLiveStatuses(items);
  res.json({ ok: true, updated: items.length });
});

app.get("/api/live-status", async (_, res) => {
  const rows = await LiveStatus.find({}).lean();
  res.json(
    rows.map((r) => ({
      machine: r.machineName,
      status: r.status,
      updatedAt: utcToPKT(r.updatedAt),
    })),
  );
});

app.get("/api/live-status/map", async (_, res) => {
  const rows = await LiveStatus.find({}).lean();
  const map = {};
  rows.forEach((r) => (map[r.machineName] = r.status));
  res.json(map);
});

/* =========================================================
   ERROR HANDLER
   ========================================================= */
app.use((err, req, res, next) => {
  console.error("🔥 Unhandled error:", err);
  res.status(500).json({
    error: "Internal server error",
    message: err.message,
    timestamp: new Date().toISOString(),
  });
});

/* =========================================================
   START SERVER
   ========================================================= */
const PORT = process.env.PORT || 5000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`🌐 Health check: http://localhost:${PORT}/health`);
  console.log(`📊 Dashboard: http://localhost:${PORT}/api/dashboard/overview`);
});
