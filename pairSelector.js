import axios from "axios";
import params from "./parameters.js";

// Bitget public API endpoints
const BITGET_SYMBOLS_URL = "https://api.bitget.com/api/v2/spot/public/symbols";
const COINGECKO_API_URL = "https://api.coingecko.com/api/v3/coins/markets";

// Configuration constants
const PAIRS_TO_SELECT = 150;
const STABLECOIN_SYMBOLS = new Set([
  "USDC", "USDT", "BUSD", "DAI", "TUSD", "USDP", "FDUSD", "GUSD", "FRAX", "USDD",
  "PYUSD", "PAXG", "USDE", "EURS", "USDX", "OUSD"
]);
const MIN_24H_VOLUME = 1_000_000;    // $1M
const MIN_MARKET_CAP = 50_000_000;   // $50M

function classifyTier(marketCap) {
  if (marketCap >= params.TIER_LARGE_CAP_MIN_MARKET_CAP) return "large";
  if (marketCap >= params.TIER_MID_CAP_MIN_MARKET_CAP) return "mid";
  return "small";
}

/**
 * Fetch all USDT-quoted SPOT symbols tradable on Bitget
 * Filters by quoteCoin and online status
 * @returns {Promise<string[]>}
 */
async function fetchBitgetSymbols() {
  const res = await axios.get(BITGET_SYMBOLS_URL);
  // API returns { code, msg, requestTime, data: [ ... ] }
  const symbols = Array.isArray(res.data?.data) ? res.data.data : [];
  return symbols
    .filter(s => s.quoteCoin === "USDT" && s.status === "online")
    .map(s => s.symbol.toUpperCase());
}

/**
 * Select the top PAIRS_TO_SELECT volatile coins by 24h price change
 * among those tradable on Bitget. Excludes stablecoins and applies
 * market cap and volume floors.
 */
async function fetchBestPairs() {
  console.log("Fetching best pairs from Bitget");

  try {
    const [symbols, cgResponse] = await Promise.all([
      fetchBitgetSymbols(),
      axios.get(COINGECKO_API_URL, {
        params: { vs_currency: "usd", per_page: 250, page: 1 }
      })
    ]);

    const tradableSet = new Set(symbols);
    const candidates = Array.isArray(cgResponse.data) ? cgResponse.data : [];

    const selected = candidates
      .map(coin => ({
        ...coin,
        bitgetSymbol: `${coin.symbol.toUpperCase()}USDT`
      }))
      .filter(coin => tradableSet.has(coin.bitgetSymbol))
      .filter(coin => !STABLECOIN_SYMBOLS.has(coin.symbol.toUpperCase()))
      .filter(coin => coin.total_volume > MIN_24H_VOLUME)
      .filter(coin => coin.market_cap >= MIN_MARKET_CAP)
      .sort((a, b) =>
        Math.abs(b.price_change_percentage_24h) - Math.abs(a.price_change_percentage_24h)
      )
      .slice(0, PAIRS_TO_SELECT)
      .map(coin => ({
        symbol: coin.bitgetSymbol,
        tier: classifyTier(coin.market_cap)
      }));

    console.log(`Selected ${selected.length} pairs`);
    return selected;
  } catch (err) {
    console.error("Error in fetchBestPairs:", err);
    return [];
  }
}

export default fetchBestPairs;
