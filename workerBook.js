import mongo from "./mongo.js";
import { Worker } from "bullmq";
import axios from "axios";
import IORedis from "ioredis";
import { ObjectId } from "mongodb";

// Using the same Redis connection if needed; otherwise default
// const redis = new IORedis();

/**
 * Worker: fetches depth-5 snapshots post-signal and stores in MongoDB.
 * Uses Bitget REST endpoint: /api/v2/spot/market/merge-depth?symbol=<symbol>&limit=5
 */

new Worker("order", async job => {
  const { symbol, id, tOffset } = job.data;

  try {
    // Fetch top-5 order book levels from Bitget
    const res = await axios.get("https://api.bitget.com/api/v2/spot/market/merge-depth", {
      params: { symbol: symbol.toUpperCase(), limit: 5 }
    });

    const data = res.data.data || {};
    const bids = data.bids?.slice(0, 5) || [];
    const asks = data.asks?.slice(0, 5) || [];

    const bidSum = bids.reduce((sum, [price, qty]) => sum + parseFloat(qty), 0);
    const askSum = asks.reduce((sum, [price, qty]) => sum + parseFloat(qty), 0);
    const imbalance = (bidSum - askSum) / (bidSum + askSum + 1e-8);

    await mongo.orderbooks.updateOne(
      { signal_id: new ObjectId(id) },
      {
        $set: { symbol },
        $push: {
          snapshots: {
            t_offset_s: tOffset,
            ts: Date.now(),
            bid_sum: bidSum,
            ask_sum: askSum,
            imbalance
          }
        }
      },
      { upsert: true }
    );
  } catch (err) {
    console.error("workerBook error:", err);
    throw err;
  }
}, {
    connection: new IORedis({
        maxRetriesPerRequest: null
    })
});