import IORedis from "ioredis";

/**
 * HotBarAggregator
 * ----------------
 * Builds 1-second and 1-minute OHLCV bars from an incoming trade stream
 * and stores completed second-bars (and optionally minute-bars) in Redis sorted sets.
 *
 * Usage:
 *   const redis = new IORedis();
 *   const agg   = new HotBarAggregator({ redisClient: redis });
 *   // On each trade:
 *   agg.processTrade(symbol, tradePrice, tradeVolume, eventTimeMs);
 */

export default class HotBarAggregator {
  /**
   * @param {{ redisClient: IORedis, secExpireSecs?: number, minExpireSecs?: number }} opts
   */
  constructor({ redisClient, secExpireSecs = 6 * 3600, minExpireSecs = 24 * 3600 }) {
    this.redis = redisClient;
    this.secExpireSecs = secExpireSecs;
    this.minExpireSecs = minExpireSecs;
    // Per-symbol in-memory state for bars
    this.buffers = new Map();
  }

  /**
   * Process a new trade tick for the given symbol.
   * @param {string} symbol         Upper- or lower-case symbol (e.g. "BTCUSDT").
   * @param {number} tradePrice     Price of the trade.
   * @param {number} tradeVolume    Volume of the trade (in quote units or base*price, per your convention).
   * @param {number} eventTimeMs    Timestamp in milliseconds.
   */
  processTrade(symbol, tradePrice, tradeVolume, eventTimeMs) {
    const sym = symbol.toLowerCase();
    let buf = this.buffers.get(sym);
    if (!buf) {
      buf = {
        secBucketTS: null,
        secOpen: 0, secHigh: 0, secLow: 0, secClose: 0, secVol: 0,
        minBucketTS: null,
        minOpen: 0, minHigh: 0, minLow: 0, minClose: 0, minVol: 0
      };
      this.buffers.set(sym, buf);
    }

    const secBucket = Math.floor(eventTimeMs / 1000);
    const minBucket = Math.floor(eventTimeMs / 60000);

    // --- Second bar logic ---
    if (buf.secBucketTS === null) {
      // initialize first second bar
      buf.secBucketTS = secBucket;
      buf.secOpen = buf.secHigh = buf.secLow = buf.secClose = tradePrice;
      buf.secVol = tradeVolume;
    } else if (secBucket === buf.secBucketTS) {
      // update existing bar
      buf.secHigh = Math.max(buf.secHigh, tradePrice);
      buf.secLow  = Math.min(buf.secLow,  tradePrice);
      buf.secClose = tradePrice;
      buf.secVol += tradeVolume;
    } else {
      // flush completed second-bar to Redis
      const bar = {
        t: buf.secBucketTS,
        o: buf.secOpen,
        h: buf.secHigh,
        l: buf.secLow,
        c: buf.secClose,
        v: buf.secVol
      };
      const key = `bitget:secbar:${sym}`;
      this.redis
        .multi()
        .zadd(key, bar.t, JSON.stringify(bar))
        .expire(key, this.secExpireSecs)
        .exec()
        .catch(err => console.error("HotBarAggregator sec ZADD error", err));

      // start new second-bar
      buf.secBucketTS = secBucket;
      buf.secOpen = buf.secHigh = buf.secLow = buf.secClose = tradePrice;
      buf.secVol = tradeVolume;
    }

    // --- Minute bar logic (optional) ---
    if (buf.minBucketTS === null) {
      buf.minBucketTS = minBucket;
      buf.minOpen = buf.minHigh = buf.minLow = buf.minClose = tradePrice;
      buf.minVol = tradeVolume;
    } else if (minBucket === buf.minBucketTS) {
      buf.minHigh = Math.max(buf.minHigh, tradePrice);
      buf.minLow  = Math.min(buf.minLow,  tradePrice);
      buf.minClose = tradePrice;
      buf.minVol += tradeVolume;
    } else if (secBucket % 60 === 0) {
      // only flush at exact minute boundaries to avoid duplicates
      const mbar = {
        t: buf.minBucketTS,
        o: buf.minOpen,
        h: buf.minHigh,
        l: buf.minLow,
        c: buf.minClose,
        v: buf.minVol
      };
      const mkey = `bitget:minbar:${sym}`;
      this.redis
        .multi()
        .zadd(mkey, mbar.t, JSON.stringify(mbar))
        .expire(mkey, this.minExpireSecs)
        .exec()
        .catch(err => console.error("HotBarAggregator min ZADD error", err));

      // start new minute-bar
      buf.minBucketTS = minBucket;
      buf.minOpen = buf.minHigh = buf.minLow = buf.minClose = tradePrice;
      buf.minVol = tradeVolume;
    }
  }
}
