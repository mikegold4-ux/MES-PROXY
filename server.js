const https = require('https');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

// Coinbase PERP pairs to track
const PERP_PAIRS = [
  'BTC-PERP-INTX','ETH-PERP-INTX','SOL-PERP-INTX','XRP-PERP-INTX',
  'DOGE-PERP-INTX','AVAX-PERP-INTX','LINK-PERP-INTX','ADA-PERP-INTX',
  'DOT-PERP-INTX','LTC-PERP-INTX','BCH-PERP-INTX','NEAR-PERP-INTX',
  'UNI-PERP-INTX','AAVE-PERP-INTX','BNB-PERP-INTX','MATIC-PERP-INTX',
  'XLM-PERP-INTX','ZEC-PERP-INTX'
];

// Price cache: perpId -> { price, ts }
const priceCache = {};

// Fetch ticker price — last trade price, real-time (no candle lag)
function fetchTickerPrice(perpId) {
  return new Promise((resolve) => {
    const path = `/products/${perpId}/ticker`;
    const options = {
      hostname: 'api.exchange.coinbase.com',
      path,
      method: 'GET',
      headers: { 'User-Agent': 'MES-Proxy/1.0' }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          // 'price' = last trade price (real-time)
          const price = parseFloat(json.price || 0);
          if (price > 0) resolve(price);
          else resolve(null);
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(5000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}

// Fallback: candle close if ticker fails
function fetchCandlePrice(perpId) {
  return new Promise((resolve) => {
    const path = `/products/${perpId}/candles?granularity=ONE_MINUTE&limit=2`;
    const options = {
      hostname: 'api.exchange.coinbase.com',
      path,
      method: 'GET',
      headers: { 'User-Agent': 'MES-Proxy/1.0' }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json && json.length > 0) {
            const close = parseFloat(json[0][4]);
            if (close > 0) resolve(close);
            else resolve(null);
          } else resolve(null);
        } catch(e) { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(5000, () => { req.destroy(); resolve(null); });
    req.end();
  });
}

async function fetchPrice(perpId) {
  const ticker = await fetchTickerPrice(perpId);
  if (ticker) return ticker;
  return fetchCandlePrice(perpId); // fallback
}

// Poll all pairs every 3 seconds, staggered to avoid rate limits
async function pollPrices() {
  for (let i = 0; i < PERP_PAIRS.length; i++) {
    const id = PERP_PAIRS[i];
    const price = await fetchPrice(id);
    if (price) {
      priceCache[id] = { price, ts: Date.now() };
    }
    if (i < PERP_PAIRS.length - 1) await sleep(200);
  }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Start polling loop
(async () => {
  console.log('[MES-Proxy] Starting price poll loop (ticker last trade)...');
  await pollPrices();
  setInterval(pollPrices, 3000); // refresh every 3s
})();

// WebSocket server — MES connects here to get live prices
const wss = new WebSocket.Server({ port: PORT });
console.log(`[MES-Proxy] WebSocket server running on port ${PORT}`);

wss.on('connection', (ws, req) => {
  console.log('[MES-Proxy] Client connected:', req.socket.remoteAddress);

  ws.send(JSON.stringify({ type: 'prices', data: priceCache }));

  const interval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'prices', data: priceCache }));
    }
  }, 2000);

  ws.on('close', () => {
    clearInterval(interval);
    console.log('[MES-Proxy] Client disconnected');
  });

  ws.on('error', () => clearInterval(interval));
});
