const parameters = {
    TIER_LARGE_CAP_MIN_MARKET_CAP: 5_000_000_000,  // 5 Billion USD
  TIER_MID_CAP_MIN_MARKET_CAP:   500_000_000,    // 500 Million USD

  // Time cache
  TIME_CACHE_DURATION_MS: 60_000,                 // 1 minute

  // Signal checking
  CHECK_SIGNAL_INTERVAL_MS: 250,                  // run checkSignal every 250 ms

  // Price bucketing for momentum
  PRICE_BUCKET_DURATION_MS: 100,                  // bucket resolution (unused for hotbars)
  PRICE_LOOKBACK_WINDOW_MS: 2_500,                // 2.5 seconds for momentum
  PRICE_SLOPE_ALPHA: 0.4,                         // EWMA for slope (~2 s)
  PRICE_SLOPE_ZSCORE: 1.5,

  // Volume thresholds
  MIN_TRADES_IN_1S: 3,
  MAX_BID_ASK_SPREAD_PCT: 0.005,

  // EWMA volume baselines (dt=0.25 s)
  EWMA_ALPHA_VOL_FAST: 0.1175,
  EWMA_ALPHA_VOL_SLOW: 0.000833,
  EWMA_ALPHA_VOL_MED: 0.00416,

  // Volume spike detection
  MIN_VOLUME_SPIKE_RATIO_1M5M: 1.3,
  VOLUME_ACCEL_ZSCORE: 1.5,

  // ATR for volatility
  ATR_PERIOD_SECONDS: 60,
  ATR_ALPHA: 2 / (30 + 1),                        // fast ATR EWMA
  ATR_ALPHA_SLOW: 2 / (300 + 1),                  // slow ATR EWMA

  // Signal cooldown after trigger
  SIGNAL_COOLDOWN_MS: 6_000
};

export default parameters;