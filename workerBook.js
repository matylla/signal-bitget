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

new Worker("bitget_order", async job => {
  const { symbol, id, tOffset } = job.data;

  try {
    // Fetch top-5 order book levels from Bitget
    const res = await axios.get("https://api.bitget.com/api/v2/spot/market/merge-depth", {
      params: { symbol: symbol.toUpperCase(), limit: 5 }
    });

    const data = res.data.data || {};

    const bids = data.bids?.slice(0, 5) || [];
    const asks = data.asks?.slice(0, 5) || [];

    // Calculate sums in token amounts
    const bidSum = bids.slice(0, 5).reduce((s, [, q]) => s + Number(q), 0);
    const askSum = asks.slice(0, 5).reduce((s, [, q]) => s + Number(q), 0);

    // Get best bid and ask prices from the orderbook
    const bestBid = bids[0] ? Number(bids[0][0]) : 0;
    const bestAsk = asks[0] ? Number(asks[0][0]) : 0;

    // Calculate midpoint price for normalization
    const midPrice = (bestBid + bestAsk) / 2;

    // Calculate USDT normalized values
    const bidSumUsdt = bidSum * midPrice;
    const askSumUsdt = askSum * midPrice;
    const totalUsdt = bidSumUsdt + askSumUsdt;

    // Calculate both raw and normalized imbalances
    const imbalance = (bidSum - askSum) / (bidSum + askSum + 1e-8);
    const imbalanceUsdt = (bidSumUsdt - askSumUsdt) / (totalUsdt + 1e-8);

    // Update MongoDB with both raw and normalized values
    await mongo.orderbooks.updateOne({
        signal_id: new ObjectId(id)
    }, {
        $set: {
            symbol
        },
        $push: {
            snapshots: {
                t_offset_s: tOffset,
                ts: Date.now(),
                // Raw token amounts (backward compatibility)
                bid_sum: bidSum,
                ask_sum: askSum,
                imbalance: imbalance,
                // NEW: USDT normalized values
                bid_sum_usdt: bidSumUsdt,
                ask_sum_usdt: askSumUsdt,
                total_liquidity_usdt: totalUsdt,
                imbalance_usdt: imbalanceUsdt,
                // Price reference
                mid_price: midPrice,
                best_bid: bestBid,
                best_ask: bestAsk,
                spread_bps: ((bestAsk - bestBid) / bestAsk) * 10000
            }
        }
    }, {
        upsert: true
    });
  } catch (err) {
    console.error("workerBook error:", err);
    throw err;
  }
}, {
    connection: new IORedis({
        maxRetriesPerRequest: null
    })
});