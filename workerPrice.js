import { ObjectId } from "mongodb";
import mongo from "./mongo.js";
import { Worker } from "bullmq";
import IORedis from "ioredis";

// Offsets for post-signal price snapshots (in seconds)
const OFFSETS_S = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,45,60,90,120,150,180,210,240,270,300,330,360,390,420,450,480,510,540,570,600,660,720,780,840,900,960,1020,1080,1140,1200,1260,1320,1380,1440,1500,1560,1620,1680,1740,1800];

// Redis connection for fetching hot bars
const redis = new IORedis();

/**
 * Compute realized sigma (volatility) from an array of bars.
 * Each bar should have { c: close }.
 */
function realisedSigmaFromBars(bars) {
  if (bars.length < 2) return null;
  const rets = [];
  for (let i = 1; i < bars.length; i++) {
    const p0 = bars[i - 1].c;
    const p1 = bars[i].c;
    if (p0 > 0) {
      rets.push(Math.log(p1 / p0));
    }
  }
  const μ = rets.reduce((s, x) => s + x, 0) / rets.length;
  const var_ = rets.reduce((s, x) => s + Math.pow(x - μ, 2), 0) / rets.length;
  return Math.sqrt(var_);
}

new Worker("bitget_price", async job => {
  try {
    await processJob(job);
  } catch (err) {
    console.error("priceWorker error:", err);
  }
}, {
    connection: new IORedis({
        maxRetriesPerRequest: null
    })
});

async function processJob(job) {
  const { id, symbol, timestamp } = job.data;
  const startSec = Math.floor(timestamp / 1000);
  const endSec = startSec + 1800; // 30 minutes later

  // Fetch 1-second bars from Redis
  const key = `bitget:secbar:${symbol.toLowerCase()}`;
  const rawBars = await redis.zrangebyscore(key, startSec, endSec);
  const bars = rawBars.map(str => JSON.parse(str));

  // Compute sigma over full 30m window
  const sigma30m = realisedSigmaFromBars(bars);

  // Build priceRows for each offset
  const barMap = new Map(bars.map(bar => [bar.t, bar]));
  const priceRows = OFFSETS_S.map(sec => {
    const t = startSec + sec;
    const bar = barMap.get(t) || bars.find(b => b.t >= t) || bars[bars.length - 1];
    return {
      t_offset_s: sec,
      price: bar ? bar.c : null,
      volume: bar ? bar.v : null
    };
  });

  await mongo.prices.insertOne({
    signal_id: new ObjectId(id),
    symbol,
    sigma_30m: sigma30m,
    prices: priceRows
  });
}
