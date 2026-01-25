const mongoose = require("mongoose");

const LiveStatusSchema = new mongoose.Schema(
  {
    machineName: { type: String, required: true, unique: true },
    status: {
      type: String,
      enum: ["RUNNING", "OFF", "DOWNTIME", "UNKNOWN"],
      default: "UNKNOWN",
    },
    updatedAt: { type: Date, required: true },
  },
  { versionKey: false }
);

module.exports = mongoose.model("LiveStatus", LiveStatusSchema);
