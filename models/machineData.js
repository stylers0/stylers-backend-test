const mongoose = require("mongoose");

const MachineDataSchema = new mongoose.Schema(
  {
    // 🔴 Always stored in UTC (JS Date is UTC internally)
    timestamp: {
      type: Date,
      required: true,
      index: true,
    },

    machineName: {
      type: String,
      required: true,
      index: true,
    },

    // RUNNING | OFF | DOWNTIME
    status: {
      type: String,
      enum: ["RUNNING", "OFF", "DOWNTIME", "UNKNOWN"],
      default: "UNKNOWN",
    },

    machinePower: {
      type: Boolean,
      default: false,
    },

    downtime: {
      type: Boolean,
      default: false,
    },

    shift: {
      type: String,
      default: null,
    },

    // Duration until NEXT state change (seconds)
    durationSeconds: {
      type: Number,
      default: 0,
      min: 0,
    },
  },
  {
    timestamps: true, // createdAt / updatedAt
    versionKey: false,
  },
);

/* =========================================================
   🔐 CRITICAL INDEXES
   ========================================================= */

// Add this method to your schema (inside the schema definition):
MachineDataSchema.methods.getUniqueKey = function () {
  // Round timestamp to nearest second (removes milliseconds)
  const roundedTimestamp = new Date(
    Math.floor(this.timestamp.getTime() / 1000) * 1000,
  );
  return `${this.machineName}_${roundedTimestamp.toISOString()}`;
};

// Create a static method to check for duplicates
MachineDataSchema.statics.findDuplicate = async function (
  machineName,
  timestamp,
) {
  // Round timestamp to nearest second for comparison
  const roundedTimestamp = new Date(
    Math.floor(timestamp.getTime() / 1000) * 1000,
  );

  return await this.findOne({
    machineName: machineName,
    timestamp: {
      $gte: new Date(roundedTimestamp.getTime() - 1000), // 1 second before
      $lte: new Date(roundedTimestamp.getTime() + 1000), // 1 second after
    },
  });
};

// ✅ Prevent duplicates forever
MachineDataSchema.index({ machineName: 1, timestamp: 1 }, { unique: true });

// ✅ Fast "latest status per machine"
MachineDataSchema.index({ machineName: 1, timestamp: -1 });

// ✅ Fast time-range queries (dashboard, export)
MachineDataSchema.index({ timestamp: -1 });

module.exports = mongoose.model("MachineData", MachineDataSchema);
