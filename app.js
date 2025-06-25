import mongo from "./mongo.js";
import IORedis from "ioredis";
import BitgetWebsockets from "./websockets.js";
import HotBarAggregator from "./hotBarAggregator.js";
import SymbolMonitor from "./symbolMonitor.js";
import params from "./parameters.js";
import fetchBestPairs from "./pairSelector.js";

(async () => {
    await mongo.connect();
    console.log("MongoDB connected.");
})();

import "./workerBook.js";
import "./workerPrice.js";

(async () => {
  const redisClient = new IORedis(process.env.REDIS_URL || {
    host: "127.0.0.1",
    port: 6379,
  });

  // Redis is initialized inside HotBarAggregator via mongo.redisConnection
  const hotBarAgg = new HotBarAggregator({ redisClient: redisClient });

  const symbolMonitors = new Map();
  let bgStream;
  let signalCheckIntervalId;


  // Process each normalized Binance-shape message
  function processCombinedStreamData(rawMessage) {
    try {
      const message = JSON.parse(rawMessage);
      if (!message.stream || !message.data) return;

      const [symbol, streamType] = message.stream.split("@");
      const data = message.data;
      const symbolUpper = symbol.toUpperCase();

      // Feed the HotBarAggregator for later 1-s bars
      if (streamType === "aggTrade") {
        const price = parseFloat(data.p);
        const qty   = parseFloat(data.q);
        const volume = price * qty;
        const ts = data.E;
        hotBarAgg.processTrade(symbolUpper, price, volume, ts);
      }

      const monitor = symbolMonitors.get(symbolUpper);
      if (!monitor) return;

      switch (streamType) {
        case "aggTrade":
          monitor.addAggTrade(data);
          break;
        case "ticker":
          monitor.applyTickerUpdate(data);
          break;
        case "bookTicker":
          monitor.applyBookTickerUpdate(data);
          break;
        case "depth5@100ms":
          monitor.updateDepthSnapshot(data);
          break;
      }
    } catch (err) {
      console.error("processStreamData error:", err);
    }
  }

  async function start() {
    console.log("Starting Cryptana on Bitget Spot...");

    const initialPairs = await fetchBestPairs();
    if (!initialPairs || initialPairs.length === 0) {
      console.error("FATAL: No symbols returned by pair selector.");
      process.exit(1);
    }

    console.log(`Found ${initialPairs.length} pairs to monitor.`);
    initialPairs.forEach(p => {
      symbolMonitors.set(p.symbol, new SymbolMonitor(p.symbol, p.tier));
    });

    // Build Binance-style stream array
    const streams = initialPairs.flatMap(p => {
      const sym = p.symbol.toLowerCase();
      return [`${sym}@aggTrade`, `${sym}@ticker`, `${sym}@depth5@100ms`];
    });

    // Initialize Bitget WS
    bgStream = new BitgetWebsockets();
    bgStream.on("open", () => console.log("WebSocket connection established."));
    bgStream.on("message", raw => {
        const msg = JSON.parse(raw);
            // console.log(raw);
        // if (msg.stream.endsWith("@aggTrade")) {
        //     console.log(`[DEBUG][TRADE] ${msg.stream}: price=${msg.data.p} qty=${msg.data.q}`);
        //   }

        processCombinedStreamData(raw);
    });
    bgStream.on("error", err => console.error("WebSocket error:", err));
    bgStream.on("close", () => console.warn("WebSocket closed; restarting required."));
    bgStream.connect(streams);

    // Periodic signal checks
    signalCheckIntervalId = setInterval(() => {
      for (const mon of symbolMonitors.values()) {
        mon.performPeriodicCalculations();
        mon.checkSignal();
      }
    }, params.CHECK_SIGNAL_INTERVAL_MS);

    console.log(`System started; monitoring ${symbolMonitors.size} symbols.`);
  }

  // Graceful shutdown
  async function stop() {
    console.log("Shutting down...");
    clearInterval(signalCheckIntervalId);
    if (bgStream) bgStream.disconnect();
    console.log("Shutdown complete.");
    process.exit(0);
  }

  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);

  start();
})();