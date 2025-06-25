import WebSocket from "ws";
import EventEmitter from "events";

const WS_URL                = "wss://ws.bitget.com/v2/ws/public";
const MAX_ARGS_PER_WS        = 50;      // Bitget docs: keep ≤50 channels per socket for stability
const SUB_BATCH_SIZE         = 20;      // send subscribe packets in reasonable chunks
const PING_INTERVAL_MS       = 30_000;  // client → server heartbeat
const RECONNECT_DELAY_MS     = 2_000;   // delay before spawning replacement after an unexpected close

/*───────────────────────────────────────────────────────────────────────────*/
/**
 * Map Binance‑style stream string → Bitget v2 arg‑object.
 * Unsupported stream returns null.
 */
function mapChannelToArg(ch) {
    const at = ch.indexOf("@");
    if (at === -1) return null;

    const symbol  = ch.slice(0, at).toUpperCase();
    const stream  = ch.slice(at + 1).toLowerCase();

    switch (stream) {
        case "aggtrade":       // trades
            return { instType: "SPOT", channel: "trade",  instId: symbol };
        case "ticker":         // 24 h stats + best bid/ask
            return { instType: "SPOT", channel: "ticker", instId: symbol };
        case "depth5@100ms":   // 5‑level order book snapshot (≈200ms)
            return { instType: "SPOT", channel: "books5", instId: symbol };
        // @bookTicker intentionally **not** mapped – derived from books5
        default:
            return null;
    }
}

/*───────────────────────────────────────────────────────────────────────────*/
/**
 * Translate Bitget push‑message → array of Binance‑shape messages.
 */
function translateBitget(msg) {
    const out = [];

    if (!msg.arg || !msg.data) return out;

    const { channel, instId } = msg.arg;
    const symbolLower = instId.toLowerCase();

    if (channel === "trade") {
        const trades = Array.isArray(msg.data) ? msg.data : [msg.data];

        for (const t of trades) {
            out.push({
                stream: `${symbolLower}@aggTrade`,
                data: {
                    p: t.price,                    // string
                    q: t.size,                     // string
                    E: parseInt(t.ts, 10),         // event‑time (ms)
                    m: (t.side || "").toLowerCase() === "sell" // buyer‑maker flag
                }
            });
        }
    }
    else if (channel === "ticker") {
        const d = Array.isArray(msg.data) ? msg.data[0] : msg.data;
        out.push({
            stream: `${symbolLower}@ticker`,
            data: {
                q: parseFloat(d.quoteVolume ?? "0"),            // 24 h quote vol
                P: (parseFloat(d.change24h ?? "0") * 100).toFixed(4), // 24 h %
                h: d.high24h,
                l: d.low24h,
                c: d.lastPr
            }
        });
    }
    else if (channel === "books5") {
        const d = Array.isArray(msg.data) ? msg.data[0] : msg.data;

        const bids = d.bids ?? [];
        const asks = d.asks ?? [];

        out.push({
            stream: `${symbolLower}@depth5@100ms`,
            data: { bids, asks }
        });

        if (bids.length && asks.length) {
            out.push({
                stream: `${symbolLower}@bookTicker`,
                data: {
                    b: bids[0][0],   // best bid px (string)
                    a: asks[0][0]    // best ask px (string)
                }
            });
        }
    }

    return out;
}

/*───────────────────────────────────────────────────────────────────────────*/
export default class BitgetWebsockets extends EventEmitter {
    /** @param {{ reconnectIntervalMs?: number }} opts */
    constructor(opts = {}) {
        super();
        this.reconnectMs  = opts.reconnectIntervalMs ?? RECONNECT_DELAY_MS;
        this._sockets     = [];
        this._pingTimers  = new Map(); // WebSocket → timerId
    }

    /**
     * Establish connections and subscribe to the given Binance‑style channels.
     * @param {string[]} channels
     */
    connect(channels = []) {
        this.disconnect();

        const args = channels
            .map(mapChannelToArg)
            .filter(Boolean);

        if (!args.length) {
            console.warn("[BitgetWS] No valid channels to subscribe.");
            return;
        }

        // chunk into ≤MAX_ARGS_PER_WS per socket
        const chunks = [];
        for (let i = 0; i < args.length; i += MAX_ARGS_PER_WS) {
            chunks.push(args.slice(i, i + MAX_ARGS_PER_WS));
        }

        chunks.forEach(chunk => this._spawnSocket(chunk));
        console.log(`[BitgetWS] Spawned ${chunks.length} connection(s) for ${args.length} channels.`);
    }

    /** Close & clean up everything. */
    disconnect() {
        for (const ws of this._sockets) {
            clearInterval(this._pingTimers.get(ws));
            ws.removeAllListeners();
            ws.close();
        }
        this._sockets = [];
        this._pingTimers.clear();
        this.emit("disconnected");
    }

    /*───────────────────────────  internal  ───────────────────────────────*/

    _spawnSocket(argChunk) {
        const ws = new WebSocket(WS_URL);
        this._sockets.push(ws);

        ws.on("open", () => {
            // subscribe in SUB_BATCH_SIZE batches to avoid oversize frames
            // console.log("[BitgetWS] subscribing with args:", JSON.stringify({ op: "subscribe", args: argChunk }));

            for (let i = 0; i < argChunk.length; i += SUB_BATCH_SIZE) {
                const slice = argChunk.slice(i, i + SUB_BATCH_SIZE);
                ws.send(JSON.stringify({ op: "subscribe", args: slice }));
            }

            // start client heartbeat
            const tId = setInterval(() => {
                if (ws.readyState === WebSocket.OPEN) ws.send("ping");
            }, PING_INTERVAL_MS);

            this._pingTimers.set(ws, tId);

            this.emit("open");
        });

        ws.on("message", raw => {
            try {
                const str = raw.toString();

                // server heartbeat – reply with pong
                if (str === "ping") {
                    ws.send("pong");
                    return;
                }

                if (str === "pong") return; // ignore own heartbeat replies

                const msg = JSON.parse(str);

                // Bitget confirm / error frames carry retCode
                if (msg.retCode && msg.retCode !== 0) {
                    console.error("[BitgetWS] retCode", msg.retCode, msg.retMsg);
                    return;
                }

                // translate & re‑emit
                for (const m of translateBitget(msg)) {
                    this.emit("message", JSON.stringify(m));
                }
            } catch (err) {
                this.emit("error", err);
            }
        });

        ws.on("error", err => this.emit("error", err));

        ws.on("close", () => {
            clearInterval(this._pingTimers.get(ws));
            this._pingTimers.delete(ws);
            this.emit("close");

            // respawn after delay with the same argChunk
            setTimeout(() => this._spawnSocket(argChunk), this.reconnectMs);
        });
    }
}