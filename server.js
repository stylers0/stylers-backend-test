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

/* =========================================================
   🕒 TIME HELPERS (UPDATED FOR PAKISTAN TIME)
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

// Convert UTC Date → Pakistan Time for frontend (UTC+5)
function utcToPKT(date) {
  if (!date) return null;

  // Ensure it's a valid Date object
  const d = new Date(date);
  if (isNaN(d.getTime())) return null;

  // Convert UTC → PKT (UTC+5)
  const PKT_OFFSET = 5 * 60 * 60 * 1000; // +5 hours in milliseconds
  return new Date(d.getTime() + PKT_OFFSET).toISOString();
}

/* =========================================================
   Middleware
   ========================================================= */
app.use(cors());
app.use(bodyParser.json({ limit: "10mb" }));
app.use(bodyParser.urlencoded({ extended: true, limit: "10mb" }));

// Add request logging middleware
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
   MongoDB
   ========================================================= */
const MONGO_URI =
  process.env.MONGO_URI || "mongodb://127.0.0.1:27017/factory_monitor";

mongoose
  .connect(MONGO_URI)
  .then(async () => {
    console.log("✅ MongoDB Connected");

    // Existing MachineData indexes
    try {
      await MachineData.createIndexes([
        { machineName: 1, timestamp: -1 },
        { timestamp: -1 },
      ]);
      console.log("✅ MachineData indexes created");
    } catch (err) {
      console.log("ℹ️ MachineData index note:", err.message);
    }

    // ✅ ADD THIS BLOCK (LiveStatus index)
    try {
      await LiveStatus.createIndexes([{ machineName: 1 }]);
      console.log("✅ LiveStatus index ready");
    } catch (err) {
      console.log("ℹ️ LiveStatus index note:", err.message);
    }
  })
  .catch((err) => console.error("❌ MongoDB Connection Error:", err));

/* =========================================================
   HTTP + WebSocket
   ========================================================= */
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws/machine-data" });

// WebSocket connection logging
wss.on("connection", (ws, req) => {
  console.log(`🔗 New WebSocket connection from ${req.socket.remoteAddress}`);
  ws.on("close", () => {
    console.log(
      `🔗 WebSocket connection closed from ${req.socket.remoteAddress}`,
    );
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
  if (count > 0) {
    console.log(`📡 Broadcasted to ${count} WebSocket clients`);
  }
}

async function filterDuplicates(items) {
  if (items.length === 0) return items;

  console.log(`🔍 Checking ${items.length} items for duplicates...`);
  const uniqueItems = [];
  const seen = new Map(); // Track machine + timestamp combinations

  for (const item of items) {
    const tsUTC = parseToUTC(item.timestamp);
    if (!tsUTC || !item.machine) continue;

    // Create a unique key: machine + rounded timestamp (to the second)
    const timeKey = Math.floor(tsUTC.getTime() / 1000);
    const uniqueKey = `${item.machine}_${timeKey}_${item.status || "UNKNOWN"}`;

    if (!seen.has(uniqueKey)) {
      seen.set(uniqueKey, true);
      uniqueItems.push(item);
    } else {
      console.log(
        `  🔍 Skipping in-memory duplicate: ${item.machine} at ${tsUTC.toISOString()}`,
      );
    }
  }

  console.log(
    `🔍 Filtered ${items.length - uniqueItems.length} duplicates, ${uniqueItems.length} unique items remain`,
  );
  return uniqueItems;
}

/* =========================================================
   🧠 SAVE LOGIC (UPDATED WITH DUPLICATE PREVENTION)
   ========================================================= */
async function saveBatch(items) {
  // Step 1: Filter duplicates in memory
  const uniqueItems = await filterDuplicates(items);

  if (uniqueItems.length === 0) {
    console.log(
      `📭 All ${items.length} items were duplicates, nothing to save`,
    );
    return [];
  }

  console.log(
    `💾 Processing ${uniqueItems.length} unique items (filtered from ${items.length} total)...`,
  );
  const saved = [];
  const CHUNK_SIZE = 50;

  // ✅ FIXED: Process uniqueItems, not items
  for (
    let startIdx = 0;
    startIdx < uniqueItems.length;
    startIdx += CHUNK_SIZE
  ) {
    const chunk = uniqueItems.slice(startIdx, startIdx + CHUNK_SIZE);
    const chunkNumber = Math.floor(startIdx / CHUNK_SIZE) + 1;
    const totalChunks = Math.ceil(uniqueItems.length / CHUNK_SIZE);

    console.log(
      `📦 Processing chunk ${chunkNumber}/${totalChunks} (items ${startIdx + 1}-${Math.min(startIdx + CHUNK_SIZE, uniqueItems.length)})`,
    );

    // Process this chunk
    for (let i = 0; i < chunk.length; i++) {
      const item = chunk[i];
      const {
        timestamp,
        machine,
        status,
        durationSeconds = 0,
        shift = null,
      } = item;

      const tsUTC = parseToUTC(timestamp);
      if (!tsUTC || !machine) {
        console.log(`  ⚠️ Skipping: Invalid data`);
        continue;
      }

      try {
        // =========================================================
        // ✅ PERMANENT DUPLICATE PREVENTION - NEW CODE
        // =========================================================

        // 1. Round the timestamp to nearest second (removes millisecond differences)
        const roundedTimestamp = new Date(
          Math.floor(tsUTC.getTime() / 1000) * 1000,
        );

        // 2. Use upsert with rounded timestamp
        const result = await MachineData.findOneAndUpdate(
          {
            machineName: machine,
            timestamp: {
              $gte: new Date(roundedTimestamp.getTime() - 1000), // 1 second before
              $lte: new Date(roundedTimestamp.getTime() + 1000), // 1 second after
            },
            status: status || "UNKNOWN",
          },
          {
            $setOnInsert: {
              timestamp: roundedTimestamp, // Use rounded timestamp
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
              durationSeconds: durationSeconds,
            },
            $set: {
              // Update these fields if document already exists
              updatedAt: new Date(),
            },
          },
          {
            upsert: true, // Insert if doesn't exist
            new: true, // Return the new/updated document
            runValidators: true,
            setDefaultsOnInsert: true,
          },
        );

        if (result.$isNew || !result._id) {
          console.log(
            `  ✅ Inserted new: ${machine} at ${roundedTimestamp.toISOString()} - ${status}`,
          );
          saved.push(result);
        } else {
          console.log(
            `  🔄 Updated existing: ${machine} at ${roundedTimestamp.toISOString()}`,
          );
        }
        // =========================================================
        // END OF NEW CODE
        // =========================================================
      } catch (err) {
        if (err.code === 11000) {
          console.log(`  ⏭️ MongoDB prevented duplicate for ${machine}`);
        } else {
          console.error(`  ❌ Save error for ${machine}:`, err.message);
        }
      }
    }

    // Memory cleanup
    chunk.length = 0;
    await new Promise((resolve) => setImmediate(resolve));

    // Log memory usage
    if (chunkNumber % 5 === 0) {
      const used = process.memoryUsage();
      console.log(
        `📊 Memory after chunk ${chunkNumber}: ${Math.round(used.heapUsed / 1024 / 1024)}MB`,
      );
    }
  }

  if (saved.length > 0) {
    console.log(`📡 Broadcasting ${saved.length} saved items`);
    broadcast(
      saved.map((d) => ({
        type: "machine_update",
        machine: d.machineName,
        status: d.status,
        shift: d.shift,
        timestamp: utcToPKT(d.timestamp),
      })),
    );
  } else {
    console.log(`📭 No new items saved from this batch`);
  }

  return saved;
}
/* =========================================================
   REST APIs
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

  console.log(`📥 Received ${items.length} items from collector`);
  if (items.length > 0) {
    console.log(`📦 First item:`, {
      timestamp: items[0].timestamp,
      machine: items[0].machine,
      status: items[0].status,
      shift: items[0].shift,
    });
  }

  const saved = await saveBatch(items);

  res.status(201).json({
    ok: true,
    saved: saved.length,
    received: items.length,
    timestamp: new Date().toISOString(),
  });
});

/* =========================================================
   DASHBOARD ENDPOINTS
   ========================================================= */
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
      lastTimestamp: utcToPKT(r.timestamp), // PKT time
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

    const result = {
      RUNNING: 0,
      DOWNTIME: 0,
      OFF: 0,
      UNKNOWN: 0,
      total: 0,
    };

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
  const { machine, from, to, limit = 200 } = req.query;
  console.log(
    `📤 GET request: machine=${machine}, from=${from}, to=${to}, limit=${limit}`,
  );

  const q = {};

  if (machine) q.machineName = machine;

  q.timestamp = {
    $gte: from ? parseToUTC(from) : new Date(Date.now() - 86400000), // Last 24 hours
    $lte: to ? parseToUTC(to) : new Date(),
  };

  try {
    const docs = await MachineData.find(q)
      .sort({ timestamp: -1 })
      .limit(Math.min(parseInt(limit), 200)) // Cap at 5000 for safety
      .lean();

    console.log(`📊 Found ${docs.length} documents`);

    // Convert UTC timestamps to PKT for frontend
    const results = docs.map((d) => ({
      ...d,
      timestamp: utcToPKT(d.timestamp), // Convert to PKT
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

/* =========================================================
   CSV EXPORT
   ========================================================= */

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
      return res
        .status(404)
        .json({ error: "No data found for the specified criteria" });
    }

    // Convert timestamps to PKT for export
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

    const filename = `machine-data-${new Date()
      .toISOString()
      .slice(0, 10)}.csv`;

    res.header("Content-Type", "text/csv");
    res.attachment(filename);
    res.send(csv);

    console.log(`✅ Exported ${docs.length} records to CSV`);
  } catch (err) {
    console.error("❌ Export error:", err);
    res
      .status(500)
      .json({ error: "Failed to export data", details: err.message });
  }
});

/* =========================================================
   HEALTH CHECK
   ========================================================= */
app.get("/health", async (_, res) => {
  try {
    // Check MongoDB connection
    await mongoose.connection.db.admin().ping();

    // Get some stats
    const totalRecords = await MachineData.countDocuments({});
    const latestRecord = await MachineData.findOne({}).sort({ timestamp: -1 });

    res.json({
      status: "healthy",
      timestamp: new Date().toISOString(),
      database: "connected",
      totalRecords,
      latestRecord: latestRecord
        ? {
            machine: latestRecord.machineName,
            timestamp: utcToPKT(latestRecord.timestamp),
            status: latestRecord.status,
          }
        : null,
      websocketClients: wss.clients.size,
    });
  } catch (err) {
    console.error("❌ Health check failed:", err);
    res.status(500).json({
      status: "unhealthy",
      error: err.message,
      timestamp: new Date().toISOString(),
    });
  }
});

/* =========================================================
   RETENTION CRON
   ========================================================= */
cron.schedule("0 3 * * *", async () => {
  console.log("🔄 Running retention cron job...");
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - 3);

  try {
    const old = await MachineData.find({ timestamp: { $lte: cutoff } }).lean();
    if (!old.length) {
      console.log("📭 No old records to archive");
      return;
    }

    // Ensure archive directory exists
    const archiveDir = path.join(__dirname, "archives");
    if (!fs.existsSync(archiveDir)) {
      fs.mkdirSync(archiveDir, { recursive: true });
    }

    const archiveFile = path.join(archiveDir, `archive-${Date.now()}.json`);
    fs.writeFileSync(archiveFile, JSON.stringify(old, null, 2));

    await MachineData.deleteMany({ _id: { $in: old.map((d) => d._id) } });

    console.log(`✅ Archived ${old.length} records to ${archiveFile}`);
  } catch (err) {
    console.error("❌ Retention cron job failed:", err);
  }
});

async function saveLiveStatuses(items) {
  const ops = [];

  for (const item of items) {
    if (!item.machine || !item.status) continue;

    ops.push(
      LiveStatus.updateOne(
        { machineName: item.machine },
        {
          $set: {
            status: item.status,
            updatedAt: parseToUTC(item.timestamp || new Date()),
          },
        },
        { upsert: true },
      ),
    );
  }

  if (ops.length) {
    await Promise.all(ops);

    broadcast({
      type: "live_status_update",
      data: items.map((i) => ({
        machine: i.machine,
        status: i.status,
        timestamp: utcToPKT(i.timestamp || new Date()),
      })),
    });
  }
}

/* =========================================================
   ✅ NEW LIVE STATUS APIs (ADDITION ONLY)
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
   ERROR HANDLING MIDDLEWARE
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
   SERVER
   ========================================================= */
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`🌐 Health check: http://localhost:${PORT}/health`);
  console.log(`📊 Dashboard: http://localhost:${PORT}/api/dashboard/overview`);
});
