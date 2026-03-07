const { WebSocketServer } = require("ws");
const http = require("http");
const fs = require("fs");
const path = require("path");

function loadEnvFile() {
  const envPath = path.join(__dirname, ".env");
  if (!fs.existsSync(envPath)) return;
  try {
    const raw = fs.readFileSync(envPath, "utf8");
    for (const line of raw.split(/\r?\n/)) {
      const row = String(line || "").trim();
      if (!row || row.startsWith("#")) continue;
      const m = row.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
      if (!m) continue;
      const key = m[1];
      let val = m[2] || "";
      if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
      }
      if (!(key in process.env)) process.env[key] = val;
    }
  } catch (e) {
    console.warn("[env] failed to read .env:", e?.message || e);
  }
}

loadEnvFile();

const PORT = Number(process.env.PORT || 3000);
const TURN_MS = 30000;
const REJOIN_GRACE_MS = 30000;
const FINISH_TO_LOBBY_MS = 4500;
const READY_MS = 15000;
const DEFAULT_STAKE = 1;
const DEFAULT_CURRENCY = "ton";

const PUBLIC_BASE_URL = "https://durak.clown-on-stonks.fun";

function getPublicBaseUrl() {
  const raw = String(process.env.PUBLIC_BASE_URL || PUBLIC_BASE_URL).trim();
  if (!raw) return PUBLIC_BASE_URL;
  return raw.replace(/\/$/, "");
}

const dataDir = path.join(__dirname, "data");
const balancesPath = path.join(dataDir, "balances.json");
const ordersPath = path.join(dataDir, "orders.json");

function loadStore() {
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    if (!fs.existsSync(balancesPath)) {
      const initial = { users: {}, platform: { earned: { ton: 0, stars: 0 } } };
      fs.writeFileSync(balancesPath, JSON.stringify(initial, null, 2), "utf8");
      return initial;
    }
    const parsed = JSON.parse(fs.readFileSync(balancesPath, "utf8") || "{}");
    if (!parsed.users || typeof parsed.users !== "object") parsed.users = {};
    if (!parsed.platform || typeof parsed.platform !== "object") parsed.platform = { earned: { ton: 0, stars: 0 } };
    if (!parsed.platform.earned || typeof parsed.platform.earned !== "object") parsed.platform.earned = { ton: 0, stars: 0 };
    if (!Number.isFinite(parsed.platform.earned.ton)) parsed.platform.earned.ton = 0;
    if (!Number.isFinite(parsed.platform.earned.stars)) parsed.platform.earned.stars = 0;
    for (const userId of Object.keys(parsed.users)) {
      const u = parsed.users[userId] || {};
      if (!u.balances || typeof u.balances !== "object") {
        const old = Number(u.balance) || 0;
        u.balances = { ton: old, stars: 0 };
      }
      if (!Number.isFinite(u.balances.ton)) u.balances.ton = 0;
      if (!Number.isFinite(u.balances.stars)) u.balances.stars = 0;
      delete u.balance;
      parsed.users[userId] = u;
    }
    return parsed;
  } catch {
    return { users: {}, platform: { earned: { ton: 0, stars: 0 } } };
  }
}

function loadOrders() {
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    if (!fs.existsSync(ordersPath)) {
      const initial = { orders: {} };
      fs.writeFileSync(ordersPath, JSON.stringify(initial, null, 2), "utf8");
      return initial;
    }
    const parsed = JSON.parse(fs.readFileSync(ordersPath, "utf8") || "{}");
    if (!parsed.orders || typeof parsed.orders !== "object") parsed.orders = {};
    return parsed;
  } catch {
    return { orders: {} };
  }
}

function saveStoreAtomic(store) {
  fs.mkdirSync(dataDir, { recursive: true });
  const tmp = balancesPath + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(store, null, 2), "utf8");
  fs.renameSync(tmp, balancesPath);
}

const balanceStore = loadStore();
const ordersStore = loadOrders();

function saveOrdersAtomic(store) {
  fs.mkdirSync(dataDir, { recursive: true });
  const tmp = ordersPath + ".tmp";
  fs.writeFileSync(tmp, JSON.stringify(store, null, 2), "utf8");
  fs.renameSync(tmp, ordersPath);
}

function parseUserId(userKey) {
  if (!userKey) return null;
  if (String(userKey).startsWith("tg:")) return String(userKey).slice(3);
  return String(userKey);
}

function ensureUserBalance(userId) {
  if (!userId) return { username: "", name: "", balances: { ton: 0, stars: 0 } };
  if (!balanceStore.users[userId]) balanceStore.users[userId] = { username: "", name: "", balances: { ton: 0, stars: 0 }, walletAddress: "" };
  const row = balanceStore.users[userId];
  if (!row.balances || typeof row.balances !== "object") row.balances = { ton: 0, stars: 0 };
  if (!Number.isFinite(row.balances.ton)) row.balances.ton = 0;
  if (!Number.isFinite(row.balances.stars)) row.balances.stars = 0;
  if (typeof row.username !== "string") row.username = "";
  if (typeof row.name !== "string") row.name = "";
  if (typeof row.walletAddress !== "string") row.walletAddress = "";
  return balanceStore.users[userId];
}

function getBalances(userId) {
  const b = ensureUserBalance(userId).balances;
  return { ton: Number(b.ton) || 0, stars: Number(b.stars) || 0 };
}

function sendBalanceToWs(ws, userId) {
  send(ws, { type: "balance", balances: getBalances(userId), walletAddress: ensureUserBalance(userId).walletAddress || "" });
}

function pushBalanceToUser(userId) {
  for (const c of wss.clients) {
    if (c?.userKey && parseUserId(c.userKey) === userId) sendBalanceToWs(c, userId);
  }
}

function normalizeCurrency(v) {
  const c = String(v || "").toLowerCase();
  return c === "stars" ? "stars" : (c === "ton" ? "ton" : null);
}

function setUserMetaFromProfile(userId, profile) {
  if (!userId || !profile) return;
  const row = ensureUserBalance(userId);
  const username = String(profile.username || "").replace(/^@/, "").trim().toLowerCase();
  row.username = username;
  row.name = safeStr(profile.name || "", 64);
  saveStoreAtomic(balanceStore);
}

const PAYMENT_ROUTES = new Set([
  "/pay/stars/link",
  "/pay/ton/order",
  "/pay/ton/confirm",
  "/pay/ton/wallet-balance"
]);

function parseApiPrefixes() {
  const raw = String(process.env.API_PREFIX || "/api").trim();
  if (!raw) return ["/api"];
  const out = raw
    .split(",")
    .map(v => String(v || "").trim())
    .filter(Boolean)
    .map(v => {
      const n = v.startsWith("/") ? v : `/${v}`;
      return n.replace(/\/+$/, "") || "/";
    });
  if (!out.includes("/api")) out.push("/api");
  return Array.from(new Set(out));
}

const API_PREFIXES = parseApiPrefixes();

function getRequestPath(reqUrl) {
  try {
    const u = new URL(reqUrl || "/", "http://localhost");
    const p = u.pathname.replace(/\/+$/, "");
    return p || "/";
  } catch {
    return "/";
  }
}

function toApiPath(pathname) {
  const clean = String(pathname || "/").replace(/\/+$/, "") || "/";
  for (const prefix of API_PREFIXES) {
    if (clean === prefix) return "/";
    if (clean.startsWith(prefix + "/")) {
      const v = clean.slice(prefix.length);
      return v.startsWith("/") ? v : `/${v}`;
    }
  }
  return clean;
}

function writeJson(res, code, data, extraHeaders = {}) {
  res.writeHead(code, { "Content-Type": "application/json", ...extraHeaders });
  res.end(JSON.stringify(data));
}

function logEnvWarnings() {
  const missing = [];
  if (!process.env.BOT_TOKEN) missing.push("BOT_TOKEN");
  if (!process.env.TON_RECEIVER) missing.push("TON_RECEIVER");
  if (!process.env.TONCENTER_API_KEY) missing.push("TONCENTER_API_KEY");
  if (!process.env.ADMIN_SECRET) missing.push("ADMIN_SECRET");
  if (!process.env.ADMIN_IDS) missing.push("ADMIN_IDS");
  if (!String(process.env.PUBLIC_BASE_URL || "").trim()) missing.push("PUBLIC_BASE_URL");
  if (missing.length) console.warn("[env] critical env missing:", missing.join(", "));
  console.info("[env] api prefixes:", API_PREFIXES.join(", "));
}

const server = http.createServer((req, res) => {
  const pathname = getRequestPath(req.url);
  const apiPath = toApiPath(pathname);

  if (PAYMENT_ROUTES.has(apiPath) && req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": req.headers.origin || "*",
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    });
    res.end();
    return;
  }

  if (PAYMENT_ROUTES.has(apiPath) && req.method !== "POST") {
    console.info("[pay] method mismatch", req.method, pathname);
    writeJson(res, 405, { ok: false, error: "method_not_allowed", method: req.method, allow: ["POST"] }, { Allow: "POST" });
    return;
  }

  if (req.method === "POST" && apiPath === "/admin/topup") {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      try {
        const payload = JSON.parse(body || "{}");
        if (!process.env.ADMIN_SECRET || payload.secret !== process.env.ADMIN_SECRET) {
          res.writeHead(403, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "forbidden" }));
          return;
        }
        const userId = safeStr(payload.userId || "", 128);
        const currency = normalizeCurrency(payload.currency);
        const amount = Number(payload.amount);
        if (!userId || !currency || !Number.isFinite(amount) || amount <= 0) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "bad_payload" }));
          return;
        }
        ensureUserBalance(userId).balances[currency] = roundMoney(ensureUserBalance(userId).balances[currency] + amount);
        saveStoreAtomic(balanceStore);
        pushBalanceToUser(userId);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, balances: getBalances(userId) }));
      } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "bad_json" }));
      }
    });
    return;
  }
  if (req.method === "POST" && apiPath === "/admin/grant_username") {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      try {
        const payload = JSON.parse(body || "{}");
        if (!process.env.ADMIN_SECRET || payload.secret !== process.env.ADMIN_SECRET) {
          res.writeHead(403, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "forbidden" }));
          return;
        }
        const currency = normalizeCurrency(payload.currency);
        const amount = Number(payload.amount);
        const username = String(payload.username || "").replace(/^@/, "").trim().toLowerCase();
        if (!username || !currency || !Number.isFinite(amount) || amount <= 0) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "bad_payload" }));
          return;
        }
        const matched = Object.keys(balanceStore.users).filter(uid => String(balanceStore.users[uid]?.username || "") === username);
        if (matched.length === 0) {
          res.writeHead(404, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "user_not_found" }));
          return;
        }
        if (matched.length > 1) {
          res.writeHead(409, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "ambiguous_username" }));
          return;
        }
        const userId = matched[0];
        ensureUserBalance(userId).balances[currency] = roundMoney(ensureUserBalance(userId).balances[currency] + amount);
        saveStoreAtomic(balanceStore);
        pushBalanceToUser(userId);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, userId, balances: getBalances(userId) }));
      } catch (e) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "bad_json" }));
      }
    });
    return;
  }

  if (req.method === "POST" && apiPath === "/pay/stars/link") {
    console.info("[pay] stars link", pathname);
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", async () => {
      try {
        if (!process.env.BOT_TOKEN) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "bot_token_missing" }));
          return;
        }
        const payload = JSON.parse(body || "{}");
        const userId = safeStr(payload.userId || "", 128);
        const stars = Number(payload.stars);
        if (!userId || !Number.isInteger(stars) || stars < 1) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "bad_payload" }));
          return;
        }
        const nonce = randId("N");
        const invoicePayload = `topup_stars:${userId}:${stars}:${nonce}`;
        const r = await fetch(`https://api.telegram.org/bot${process.env.BOT_TOKEN}/createInvoiceLink`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            title: "Top up Stars",
            description: `Top up ${stars} Stars`,
            payload: invoicePayload,
            currency: "XTR",
            prices: [{ label: "Top up", amount: stars }]
          })
        });
        const j = await r.json();
        if (!j.ok || !j.result) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "tg_create_invoice_failed" }));
          return;
        }
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, url: j.result }));
      } catch (e) {
        console.error("[pay] stars link error", e?.message || e);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "bad_json" }));
      }
    });
    return;
  }

  if (req.method === "POST" && apiPath === "/pay/ton/order") {
    console.info("[pay] ton order", pathname);
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => {
      try {
        if (!process.env.TON_RECEIVER) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "ton_receiver_missing" }));
          return;
        }
        const payload = JSON.parse(body || "{}");
        const userId = safeStr(payload.userId || "", 128);
        const ton = Number(payload.ton);
        if (!userId || !Number.isFinite(ton) || ton < 0.1) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "bad_payload" }));
          return;
        }
        const orderId = randId("TON");
        const amountNano = String(BigInt(Math.round(ton * 1e9)));
        const comment = `EVILTOPUP:${orderId}:${userId}`;
        ordersStore.orders[orderId] = { orderId, userId, ton, amountNano, comment, status: "pending", createdAt: now(), txHash: null };
        saveOrdersAtomic(ordersStore);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, orderId, to: process.env.TON_RECEIVER, amountNano, comment }));
      } catch (e) {
        console.error("[pay] ton order error", e?.message || e);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "bad_json" }));
      }
    });
    return;
  }

  if (req.method === "POST" && apiPath === "/pay/ton/confirm") {
    console.info("[pay] ton confirm", pathname);
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", async () => {
      try {
        if (!process.env.TON_RECEIVER) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "ton_receiver_missing" }));
          return;
        }
        if (!process.env.TONCENTER_API_KEY) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "toncenter_api_key_missing" }));
          return;
        }
        const payload = JSON.parse(body || "{}");
        const orderId = safeStr(payload.orderId || "", 128);
        const userId = safeStr(payload.userId || "", 128);
        const ord = ordersStore.orders[orderId];
        if (!ord || ord.userId !== userId) {
          res.writeHead(404, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "not_found" }));
          return;
        }
        if (ord.status === "paid") {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: true, balances: getBalances(userId), alreadyPaid: true }));
          return;
        }

        const txResp = await fetch(`https://toncenter.com/api/v2/getTransactions?address=${encodeURIComponent(process.env.TON_RECEIVER)}&limit=50&to_lt=0&archival=true`, {
          headers: { "X-API-Key": process.env.TONCENTER_API_KEY }
        });
        const txJson = await txResp.json();
        const txs = Array.isArray(txJson?.result) ? txJson.result : [];
        const targetComment = `EVILTOPUP:${orderId}:${userId}`;
        const minNano = BigInt(ord.amountNano);
        let foundHash = null;
        for (const tx of txs) {
          const inMsg = tx?.in_msg || {};
          const value = BigInt(String(inMsg.value || "0"));
          const msg = String(inMsg.message || inMsg.msg_data?.text || "");
          if (value >= minNano && msg.includes(targetComment)) {
            foundHash = tx?.transaction_id?.hash || tx?.hash || randId("tx");
            break;
          }
        }
        if (!foundHash) {
          res.writeHead(404, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "not_found" }));
          return;
        }
        const duplicate = Object.values(ordersStore.orders).find(o => o && o.status === "paid" && o.txHash === foundHash);
        if (duplicate) {
          res.writeHead(409, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "already_used_tx" }));
          return;
        }
        ord.status = "paid";
        ord.paidAt = now();
        ord.txHash = foundHash;
        ensureUserBalance(userId).balances.ton = roundMoney(ensureUserBalance(userId).balances.ton + Number(ord.ton));
        saveOrdersAtomic(ordersStore);
        saveStoreAtomic(balanceStore);
        pushBalanceToUser(userId);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, balances: getBalances(userId) }));
      } catch (e) {
        console.error("[pay] ton confirm error", e?.message || e);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "bad_json" }));
      }
    });
    return;
  }
  if (req.method === "POST" && apiPath === "/pay/ton/wallet-balance") {
    console.info("[pay] wallet balance", pathname);
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", async () => {
      try {
        if (!process.env.TONCENTER_API_KEY) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "toncenter_api_key_missing" }));
          return;
        }
        const payload = JSON.parse(body || "{}");
        const walletAddress = safeStr(payload.walletAddress || "", 128);
        if (!walletAddress) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: false, error: "wallet_required" }));
          return;
        }
        const r = await fetch(`https://toncenter.com/api/v2/getAddressBalance?address=${encodeURIComponent(walletAddress)}`, {
          headers: { "X-API-Key": process.env.TONCENTER_API_KEY }
        });
        const j = await r.json();
        const nano = BigInt(String(j?.result || "0"));
        const ton = Number(nano) / 1e9;
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, ton: roundMoney(ton) }));
      } catch (e) {
        console.error("[pay] wallet balance error", e?.message || e);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "bad_json" }));
      }
    });
    return;
  }

  if (req.method === "GET" && (pathname === "/tonconnect-manifest.json" || API_PREFIXES.some(prefix => pathname === `${prefix}/tonconnect-manifest.json`))) {
    const base = getPublicBaseUrl();
    const fallback = {
      url: base + "/",
      name: "Durak Mini App",
      iconUrl: base + "/icon.svg",
      termsOfUseUrl: process.env.TERMS_URL || (base + "/terms"),
      privacyPolicyUrl: process.env.PRIVACY_URL || (base + "/privacy")
    };
    let manifest = fallback;
    try {
      const local = JSON.parse(fs.readFileSync(path.join(__dirname, "tonconnect-manifest.json"), "utf8"));
      manifest = { ...fallback, ...local };
    } catch {}
    console.info("[manifest] served", pathname, "base=", base);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(manifest));
    return;
  }
  if (API_PREFIXES.some(prefix => pathname.startsWith(prefix + "/"))) {
    writeJson(res, 404, { ok: false, error: "not_found", path: pathname });
    return;
  }
  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ noServer: true });

/** ======================
 *  Helpers
 ======================= */

function now() { return Date.now(); }

function safeStr(v, max = 64) {
  return (v ?? "").toString().slice(0, max);
}

function sanitizeProfile(p) {
  const name = safeStr(p?.name || p?.username || "Player", 32);
  const username = safeStr(p?.username || "", 32);
  const photoUrl = safeStr(p?.photoUrl || "", 512);
  return { name, username, photoUrl };
}

function send(ws, data) {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify(data));
}

function kickWs(ws, message = "Вы зашли с другого устройства.") {
  if (!ws) return;
  send(ws, { type: "kicked", message });
  ws.kicked = true;
  try { ws.close(); } catch {}
}

function randId(prefix = "R") {
  return prefix + Math.random().toString(16).slice(2, 10) + "-" + Date.now().toString(16).slice(-6);
}

function getRoomByUserKey(userKey) {
  const roomId = userToRoom.get(userKey);
  if (!roomId) return null;
  return rooms.get(roomId) || null;
}

function broadcastToRoom(room, data) {
  if (!room) return;
  for (const p of room.players || []) {
    if (p?.ws && p.ws.readyState === 1) {
      send(p.ws, data);
    }
  }
}

function roundMoney(v) {
  return Math.round((Number(v) || 0) * 100) / 100;
}

function lockSeatStake(room, role, userKey) {
  const userId = parseUserId(userKey);
  if (!userId) return { ok: false, error: "Нет userId" };
  const stake = Number(room.stake || DEFAULT_STAKE);
  const currency = normalizeCurrency(room.currency || DEFAULT_CURRENCY);
  if (!Number.isFinite(stake) || stake <= 0) return { ok: false, error: "Некорректная ставка" };
  if (!currency) return { ok: false, error: "Некорректная валюта" };
  const user = ensureUserBalance(userId);
  if ((user.balances[currency] || 0) < stake) return { ok: false, error: "Недостаточно средств" };
  const fee = roundMoney(stake * 0.1);
  const pot = roundMoney(stake - fee);
  user.balances[currency] = roundMoney(user.balances[currency] - stake);
  balanceStore.platform.earned[currency] = roundMoney((balanceStore.platform.earned[currency] || 0) + fee);
  room.escrow[role] = { userId, stake, currency, pot, fee, locked: true };
  saveStoreAtomic(balanceStore);
  return { ok: true, userId };
}

function refundSeatStake(room, role) {
  const e = room?.escrow?.[role];
  if (!e || !e.locked) return;
  const currency = normalizeCurrency(e.currency || room.currency || DEFAULT_CURRENCY) || DEFAULT_CURRENCY;
  ensureUserBalance(e.userId).balances[currency] = roundMoney(ensureUserBalance(e.userId).balances[currency] + e.stake);
  balanceStore.platform.earned[currency] = roundMoney((balanceStore.platform.earned[currency] || 0) - e.fee);
  delete room.escrow[role];
  saveStoreAtomic(balanceStore);
}

function forfeitSeatStake(room, role) {
  const e = room?.escrow?.[role];
  if (!e || !e.locked) return;
  const currency = normalizeCurrency(e.currency || room.currency || DEFAULT_CURRENCY) || DEFAULT_CURRENCY;
  balanceStore.platform.earned[currency] = roundMoney((balanceStore.platform.earned[currency] || 0) + e.pot);
  delete room.escrow[role];
  saveStoreAtomic(balanceStore);
}

function markHardLose(room, state, role) {
  if (!state || !role) return;
  if (!state.hardLoseRoles) state.hardLoseRoles = {};
  state.hardLoseRoles[role] = true;
}

function markFinishChanges(room, state) {
  if (!state || !state.players) return;
  if (!state.finishedSet) state.finishedSet = {};
  if (!Array.isArray(state.finishedOrder)) state.finishedOrder = [];
  const newFinishers = [];
  for (const role of room.roles) {
    const handLen = state.players?.[role]?.hand?.length ?? 0;
    if (handLen === 0 && !state.finishedSet[role]) newFinishers.push(role);
  }
  if (newFinishers.length > 0) {
    for (const role of newFinishers) {
      state.finishedSet[role] = true;
      state.finishedOrder.push(role);
    }
    state.lastFinishGroup = newFinishers;
    if (!state.mainWinner) state.mainWinner = state.finishedOrder[0] || null;
  }
}

let tgUpdateOffset = 0;
async function tgApi(method, payload) {
  if (!process.env.BOT_TOKEN) throw new Error("bot_token_missing");
  const r = await fetch(`https://api.telegram.org/bot${process.env.BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {})
  });
  return r.json();
}

async function tgResolveUserIdByUsername(username) {
  const uname = String(username || "").replace(/^@/, "").trim().toLowerCase();
  if (!uname) return null;
  try {
    const j = await tgApi("getChat", { chat_id: "@" + uname });
    if (!j?.ok || !j?.result?.id) return null;
    return String(j.result.id);
  } catch {
    return null;
  }
}

function parseStarsPayload(s) {
  const m = String(s || "").match(/^topup_stars:([^:]+):(\d+):(.+)$/);
  if (!m) return null;
  return { userId: m[1], stars: Number(m[2]), nonce: m[3] };
}

function isAdminTelegramId(id) {
  const raw = String(process.env.ADMIN_IDS || "");
  const set = new Set(raw.split(",").map(x => x.trim()).filter(Boolean));
  return set.has(String(id || ""));
}

function parseBalanceCommand(text) {
  const t = String(text || "").trim();
  let m = t.match(/^\/balance\s+@([a-zA-Z0-9_]{3,64})\s+([0-9]+)\s+(TON|STARS)$/i);
  if (m) return { by: "username", username: m[1].toLowerCase(), amount: Number(m[2]), currency: m[3].toLowerCase() === "ton" ? "ton" : "stars" };
  m = t.match(/^\/balance\s+([0-9]{4,20})\s+([0-9]+)\s+(TON|STARS)$/i);
  if (m) return { by: "id", userId: String(m[1]), amount: Number(m[2]), currency: m[3].toLowerCase() === "ton" ? "ton" : "stars" };
  return null;
}

async function processTgUpdate(u) {
  try {
    const msgText = u.message?.text || "";
    const fromId = u.message?.from?.id;
    const cmd = parseBalanceCommand(msgText);

    if (String(msgText || "").trim().startsWith('/balance')) {
      console.info("[bot][balance] incoming", {
        text: String(msgText || "").slice(0, 120),
        fromId: String(fromId || ""),
        chatId: String(u.message?.chat?.id || ""),
        parsed: !!cmd,
        isAdmin: isAdminTelegramId(fromId)
      });
    }

    if (cmd) {
      if (!isAdminTelegramId(fromId)) {
        console.warn("[bot][balance] forbidden", { fromId: String(fromId || "") });
        if (u.message?.chat?.id) await tgApi("sendMessage", { chat_id: u.message.chat.id, text: "Нет прав для команды /balance" });
      } else {
        let userId = "";
        if (cmd.by === "id") {
          userId = cmd.userId;
        } else {
          const matched = Object.keys(balanceStore.users).filter(uid => String(balanceStore.users[uid]?.username || "") === cmd.username);
          if (matched.length === 1) {
            userId = matched[0];
          } else {
            userId = await tgResolveUserIdByUsername(cmd.username) || "";
            if (userId) {
              const row = ensureUserBalance(userId);
              row.username = cmd.username;
              saveStoreAtomic(balanceStore);
              console.info("[bot][balance] resolved via getChat", { username: cmd.username, userId });
            }
          }
        }

        if (!userId) {
          console.warn("[bot][balance] user_not_found", { by: cmd.by, username: cmd.username || null, userId: cmd.userId || null });
          if (u.message?.chat?.id) await tgApi("sendMessage", { chat_id: u.message.chat.id, text: "Пользователь не найден" });
        } else {
          ensureUserBalance(userId).balances[cmd.currency] = roundMoney(ensureUserBalance(userId).balances[cmd.currency] + cmd.amount);
          if (cmd.by === "username") ensureUserBalance(userId).username = cmd.username;
          saveStoreAtomic(balanceStore);
          pushBalanceToUser(userId);
          if (u.message?.chat?.id) {
            const who = cmd.by === "id" ? `id ${userId}` : `@${cmd.username}`;
            await tgApi("sendMessage", {
              chat_id: u.message.chat.id,
              text: `Все начислено: ${who} +${cmd.amount} ${cmd.currency.toUpperCase()}`
            });
          }
        }
      }
    } else if (String(msgText || "").trim().startsWith('/balance') && u.message?.chat?.id) {
      await tgApi("sendMessage", { chat_id: u.message.chat.id, text: "Неверный формат. Используйте: /balance @username X TON|STARS или /balance <tg_user_id> X TON|STARS" });
    }
    if (u.pre_checkout_query?.id) {
      await tgApi("answerPreCheckoutQuery", { pre_checkout_query_id: u.pre_checkout_query.id, ok: true });
    }
    const sp = u.message?.successful_payment;
    if (!sp) return;
    const parsed = parseStarsPayload(sp.invoice_payload);
    if (!parsed || !parsed.userId || !Number.isInteger(parsed.stars) || parsed.stars <= 0) return;
    ensureUserBalance(parsed.userId).balances.stars = roundMoney(ensureUserBalance(parsed.userId).balances.stars + parsed.stars);
    saveStoreAtomic(balanceStore);
    pushBalanceToUser(parsed.userId);
  } catch (e) {
    console.warn("[bot] process update error", e?.message || e);
  }
}

async function pollTelegramUpdates() {
  if (!process.env.BOT_TOKEN) return;
  try {
    const r = await fetch(`https://api.telegram.org/bot${process.env.BOT_TOKEN}/getUpdates?timeout=20&offset=${tgUpdateOffset}`);
    const j = await r.json();
    if (!j?.ok || !Array.isArray(j.result)) return;
    for (const upd of j.result) {
      tgUpdateOffset = Math.max(tgUpdateOffset, Number(upd.update_id || 0) + 1);
      await processTgUpdate(upd);
    }
  } catch {}
}

/** ======================
 *  Data
 ======================= */

const rooms = new Map(); // roomId -> room
// room:
// {
//   id,isPrivate,password,maxPlayers,winMode,allowTransfer,
//   roles,createdAt,startedAt,
//   players:[{ws,userKey,role,profile,ready}],
//   state:null|gameState,
//   timer:null,
//   disconnectTimers:Map,
//   finishResetTimer:null
// }

const userToRoom = new Map(); // userKey -> roomId

const quickQueue = {
  waiting: null, // { userKey, ws, profile, winMode, allowTransfer, ts }
};

/** ======================
 *  Lobby lists rules
 *  - if game started and not finished => hidden from list
 *  - if finished: show ONLY if missing players
 ======================= */

function roomToListItem(room) {
  const players = room.players
    .filter(p => !!p.userKey)
    .map(p => ({
      role: p.role,
      username: p.profile?.username || "",
      name: p.profile?.name || "",
      photoUrl: p.profile?.photoUrl || ""
    }));

  const playersCount = room.players.filter(p => !!p.userKey).length;

  return {
    id: room.id,
    isPrivate: !!room.isPrivate,
    maxPlayers: room.maxPlayers,
    winMode: room.winMode,
    allowTransfer: !!room.allowTransfer,
    throwInMode: room.throwInMode || "all",
    stake: Number(room.stake || DEFAULT_STAKE),
    currency: room.currency || DEFAULT_CURRENCY,
    playersCount,
    started: !!room.state,
    createdAt: room.createdAt,
    players
  };
}

function buildLobbyLists() {
  const pub = [];
  const priv = [];
  const emptyRooms = [];
  for (const room of rooms.values()) {
    const playersCount = room.players.filter(p => !!p.userKey).length;

    // 0 игроков — в идеале таких уже не будет (удалим ниже), но на всякий
    if (playersCount === 0) {
      emptyRooms.push(room);
      continue;
    }

    // Идёт игра => скрываем
    if (room.state && room.state.phase !== "finished") continue;

    // Игра завершилась => показываем только если не хватает игроков
    if (room.state && room.state.phase === "finished") {
      if (playersCount >= room.maxPlayers) continue;
    }

    // Лобби (room.state == null) => показываем
    const item = roomToListItem(room);
    if (room.isPrivate) priv.push(item);
    else pub.push(item);
  }
  pub.sort((a,b) => b.createdAt - a.createdAt);
  priv.sort((a,b) => b.createdAt - a.createdAt);
  for (const room of emptyRooms) deleteRoomIfEmpty(room, { broadcast: false });
  return { public: pub, private: priv };
}

function broadcastLobbyLists() {
  const lists = buildLobbyLists();
  for (const ws of wss.clients) {
    if (ws.readyState !== 1) continue;
    send(ws, { type:"lobbies", ...lists });
  }
}

function playersCount(room) {
  return room.players.filter(p => !!p.userKey).length;
}

function clearReadyTimer(room) {
  if (room.readyTimer) clearTimeout(room.readyTimer);
  room.readyTimer = null;
  room.readyDeadlineTs = null;
}

function scheduleReadyCountdown(room) {
  if (room.state) {
    clearReadyTimer(room);
    return;
  }
  if (playersCount(room) !== room.maxPlayers) {
    clearReadyTimer(room);
    return;
  }
  if (!room.readyDeadlineTs || room.readyDeadlineTs <= now()) {
    room.readyDeadlineTs = now() + READY_MS;
  }
  if (room.readyTimer) return;
  room.readyTimer = setTimeout(() => onReadyTimeout(room), READY_MS + 50);
}

function onReadyTimeout(room) {
  room.readyTimer = null;
  if (room.state) return;
  if (playersCount(room) !== room.maxPlayers) {
    room.readyDeadlineTs = null;
    return;
  }
  const allReady = room.roles.every(r => !!getPlayer(room, r)?.ready);
  if (allReady) {
    tryStartGame(room);
    return;
  }
  for (const p of room.players) p.ready = false;
  room.readyDeadlineTs = now() + READY_MS;
  sendLobbyState(room);
  scheduleReadyCountdown(room);
}

/** ======================
 *  Game Logic
 ======================= */

const SUITS = ["♣", "♦", "♥", "♠"];
const RANKS = [6, 7, 8, 9, 10, "J", "Q", "K", "A"];
const RANK_VALUE = new Map(RANKS.map((r, i) => [r, i]));

function createDeck() {
  const deck = [];
  let id = 1;
  for (const suit of SUITS) for (const rank of RANKS) deck.push({ id: id++, suit, rank });
  return deck;
}

function shuffle(a) {
  for (let pass = 0; pass < 3; pass++) {
    for (let i = a.length - 1; i > 0; i--) {
      const j = (Math.random() * (i + 1)) | 0;
      [a[i], a[j]] = [a[j], a[i]];
    }
  }
  if (a.length > 1) {
    const cut = (Math.random() * a.length) | 0;
    if (cut > 0) a.push(...a.splice(0, cut));
  }
}

function randomChoice(list) {
  return list[(Math.random() * list.length) | 0];
}

function randomBool() {
  return Math.random() < 0.5;
}

function getPlayer(room, role) {
  return room.players.find(p => p.role === role) || null;
}

function nextRole(room, role) {
  const idx = room.roles.indexOf(role);
  if (idx === -1) return room.roles[0];
  return room.roles[(idx + 1) % room.roles.length];
}

function buildRoles(maxPlayers) {
  return Array.from({ length: maxPlayers }, (_, i) => `p${i + 1}`);
}

function throwInRoles(room, state) {
  const roles = room.roles.filter(r => r !== state.defender);
  if (room.throwInMode !== "neighbors" || room.roles.length < 4) return roles;

  const idx = room.roles.indexOf(state.defender);
  const prev = room.roles[(idx - 1 + room.roles.length) % room.roles.length];
  const next = room.roles[(idx + 1) % room.roles.length];
  return roles.filter(r => r === prev || r === next);
}

function dealUpTo6(state, role) {
  const hand = state.players[role].hand;
  while (hand.length < 6 && state.deck.length > 0) hand.push(state.deck.shift());
}

function dealRound(state, room) {
  let r = state.attacker;
  for (let i = 0; i < room.roles.length; i++) {
    dealUpTo6(state, r);
    r = nextRole(room, r);
  }
}

function cardBeats(trumpSuit, def, atk) {
  if (def.suit === atk.suit) return RANK_VALUE.get(def.rank) > RANK_VALUE.get(atk.rank);
  if (def.suit === trumpSuit && atk.suit !== trumpSuit) return true;
  return false;
}

function ranksOnTable(state) {
  const set = new Set();
  for (const p of state.table) {
    if (!p) continue;
    set.add(p.attack.rank);
    if (p.defend) set.add(p.defend.rank);
  }
  return set;
}

function canAttackAddMore(state) {
  const totalPairs = state.table.filter(Boolean).length;
  const defenderHand = state.players?.[state.defender]?.hand?.length ?? 0;
  const defendedCount = state.table.filter(p => p && p.defend).length;
  const roundPairsLimit = state.roundIndex === 1 ? 5 : 6;
  const maxPairs = Math.min(roundPairsLimit, defenderHand + defendedCount);
  if (maxPairs <= 0) return false;
  return totalPairs < maxPairs;
}

function isOutClassic(room, state, role) {
  if (room.winMode !== "classic") return false;
  return state.deck.length === 0 && (state.players?.[role]?.hand?.length || 0) === 0;
}

function isOutDraw(room, state, role) {
  if (room.winMode !== "draw") return false;
  return (state.players?.[role]?.hand?.length || 0) === 0;
}

function isOutRole(room, state, role) {
  return isOutClassic(room, state, role) || isOutDraw(room, state, role);
}

function nextActiveRole(room, state, fromRole) {
  let cur = fromRole;
  for (let i=0;i<room.roles.length;i++) {
    cur = nextRole(room, cur);
    if (!isOutRole(room, state, cur)) return cur;
  }
  return fromRole;
}

function computeActiveRole(state, room) {
  if (!state || state.phase === "finished") return null;

  const hasUndefended = state.table.some(p => p && !p.defend);
  if ((state.phase === "defend" || state.phase === "taking") && hasUndefended) return state.defender;

  // when attack phase and table empty -> attacker
  return state.attacker;
}

function normalizeTurn(room, state) {
  if (!state || state.phase === "finished") return;

  if (room.roles.length < 2) return;

  if (state.attacker && !room.roles.includes(state.attacker)) state.attacker = room.roles[0];
  if (state.defender && !room.roles.includes(state.defender)) state.defender = nextRole(room, state.attacker);

  if (isOutRole(room, state, state.attacker)) {
    state.attacker = nextActiveRole(room, state, state.attacker);
  }
  if (isOutRole(room, state, state.defender) || state.defender === state.attacker) {
    state.defender = nextActiveRole(room, state, state.attacker);
  }
  if (state.defender) state.lastDefenderRole = state.defender;
}

function resetTakingPass(room, state) {
  state.takingPass = {};
  for (const r of throwInRoles(room, state)) state.takingPass[r] = false;
  state.takingLeaderPassed = false;
}

function allAttackersPassed(room, state) {
  const ats = throwInRoles(room, state);
  return ats.every(r => state.takingPass?.[r] === true);
}

function finalizeTakingRound(room, state, defender) {
  const takingReason = state.takingReason || "take";

  if (takingReason === "take") {
    const takeTo = state.players[defender].hand;
    for (const pair of state.table) {
      if (!pair) continue;
      takeTo.push(pair.attack);
      if (pair.defend) takeTo.push(pair.defend);
    }
    state.table = [null,null,null,null,null,null];

    dealRound(state, room);
    markFinishChanges(room, state);

    if (checkImmediateClassicWin(room, state)) return state;

    state.attacker = nextActiveRole(room, state, defender);
    state.defender = nextActiveRole(room, state, state.attacker);
  } else {
    for (const pair of state.table) {
      if (!pair) continue;
      state.discard.push(pair.attack);
      if (pair.defend) state.discard.push(pair.defend);
    }
    state.table = [null,null,null,null,null,null];

    dealRound(state, room);
    markFinishChanges(room, state);

    if (checkImmediateClassicWin(room, state)) return state;

    state.attacker = defender;
    if (isOutRole(room, state, state.attacker)) state.attacker = nextActiveRole(room, state, state.attacker);
    state.defender = nextActiveRole(room, state, state.attacker);
  }

  state.phase = "attack";
  state.roundIndex += 1;
  state.message = "";
  state.takingReason = null;
  resetTakingPass(room, state);
  if (state.defender) state.lastDefenderRole = state.defender;

  checkFinish(room, state);
  return state;
}

function roleCanActInTaking(state, role, attacker, defender) {
  if (role === defender) return false;
  if (state.takingPass?.[role]) return false;
  if (state.takingReason === "bito" && role !== attacker && !state.takingLeaderPassed) return false;
  return true;
}

function lowestTrumpAttacker(room, state) {
  let bestRole = null;
  let bestValue = Infinity;
  for (const r of room.roles) {
    const hand = state.players?.[r]?.hand || [];
    for (const c of hand) {
      if (c.suit !== state.trumpSuit) continue;
      const val = RANK_VALUE.get(c.rank);
      if (typeof val !== "number") continue;
      if (val < bestValue) {
        bestValue = val;
        bestRole = r;
      }
    }
  }
  return bestRole;
}

function finishFirstOut(state, room, winners) {
  const winnerList = Array.isArray(winners) ? winners : [winners];
  state.phase = "finished";
  state.winners = winnerList;
  state.loser = room.roles.find(r => !winnerList.includes(r)) || null;
  state.losers = state.loser ? [state.loser] : [];
  state.finishMode = room.winMode;
  state.message = winnerList.length > 1 ? "Победа: несколько игроков без карт." : "Победа: первый без карт.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function finishClassic(state, room, loser) {
  state.phase = "finished";
  state.loser = loser;
  state.winners = room.roles.filter(r => r !== loser);
  state.losers = loser ? [loser] : [];
  state.finishMode = "classic";
  state.message = loser ? "Игра окончена." : "Ничья.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function finishClassicImmediate(state, room, winner) {
  state.phase = "finished";
  state.loser = null;
  state.winners = winner ? [winner] : [];
  state.losers = [];
  state.finishMode = "classic";
  state.message = "Победа.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function finishDraw(state, room) {
  state.phase = "finished";
  const losers = Array.isArray(state.lastFinishGroup) ? state.lastFinishGroup.slice() : [];
  const winners = room.roles.filter(r => !losers.includes(r));
  state.loser = null;
  state.losers = losers;
  state.winners = winners;
  state.finishMode = "draw";
  state.message = "Ничья.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function checkImmediateClassicWin(room, state) {
  if (state.phase === "finished") return true;
  if (room.winMode !== "classic") return false;
  const withCards = room.roles.filter(r => (state.players?.[r]?.hand?.length || 0) > 0);
  if (withCards.length === 1) {
    finishClassic(state, room, withCards[0]);
    return true;
  }
  if (withCards.length === 0) {
    const fallbackLoser = state.lastDefenderRole && room.roles.includes(state.lastDefenderRole)
      ? state.lastDefenderRole
      : (room.roles[0] || null);
    finishClassic(state, room, fallbackLoser);
    return true;
  }
  return false;
}

function checkFinish(room, state) {
  if (state.phase === "finished") return true;

  if (room.roles.length < 2) {
    // если осталось 1 игрок — он победил
    const only = room.roles[0] || null;
    state.phase = "finished";
    state.winners = only ? [only] : [];
    state.loser = null;
    state.losers = [];
    state.finishMode = room.winMode;
    state.message = "Игра окончена.";
    state.activeRole = null;
    state.deadlineTs = null;
    return true;
  }

  if (room.winMode === "classic") {
    if (checkImmediateClassicWin(room, state)) return true;
    return false;
  }

  if (room.winMode === "firstout") {
    const winners = room.roles.filter(r => state.players[r].hand.length === 0);
    if (winners.length > 0) {
      finishFirstOut(state, room, winners);
      return true;
    }
    return false;
  }

  if (room.winMode === "draw") {
    markFinishChanges(room, state);
    if (state.deck.length > 0) return false;
    const withCards = room.roles.filter(r => state.players[r].hand.length > 0);
    if (withCards.length === 0) { finishDraw(state, room); return true; }
    return false;
  }

  return false;
}

function createNewGame(room) {
  const deck = createDeck();
  shuffle(deck);

  const trumpCard = deck[deck.length - 1];
  const trumpSuit = trumpCard.suit;

  const state = {
    trumpSuit,
    trumpCard,
    deck,
    discard: [],
    table: [null,null,null,null,null,null],
    attacker: room.roles[0],
    defender: nextRole(room, room.roles[0]),
    phase: "attack",
    roundIndex: 1,
    players: {},
    message: "",
    winners: [],
    loser: null,
    losers: [],
    mainWinner: null,
    finishMode: room.winMode,
    payoutBreakdown: null,
    hardLoseRoles: {},
    finishedSet: {},
    finishedOrder: [],
    lastFinishGroup: [],
    lastDefenderRole: nextRole(room, room.roles[0]),
    activeRole: null,
    deadlineTs: null,
    takingPass: {},
    takingLeaderPassed: false,
    takingReason: null,
  };

  for (const r of room.roles) state.players[r] = { hand: [] };
  for (const r of room.roles) dealUpTo6(state, r);

  const firstAttacker = lowestTrumpAttacker(room, state);
  if (firstAttacker) {
    state.attacker = firstAttacker;
    state.defender = nextRole(room, firstAttacker);
  }

  state.lastDefenderRole = state.defender;
  markFinishChanges(room, state);

  return state;
}

function settleRoomPayouts(room, state) {
  if (!state || state.payoutBreakdown) return;
  const mode = room.winMode;
  const roles = Object.keys(room.escrow || {});
  const N = roles.length;
  const S = Number(room.stake || DEFAULT_STAKE);
  const currency = normalizeCurrency(room.currency || DEFAULT_CURRENCY) || DEFAULT_CURRENCY;
  const basePool = roundMoney(0.9 * N * S);
  const winners = Array.isArray(state.winners) ? state.winners.slice() : [];
  const losers = Array.isArray(state.losers) && state.losers.length ? state.losers.slice() : (state.loser ? [state.loser] : []);
  let mainWinner = state.mainWinner || (state.finishedOrder?.[0] || null);
  if (mainWinner && !winners.includes(mainWinner)) mainWinner = winners[0] || null;

  let winnersPool = basePool;
  let platformEarnedDelta = 0;
  const perRole = {};
  const hardLoseRoles = state.hardLoseRoles || {};
  for (const role of roles) {
    const e = room.escrow?.[role] || null;
    perRole[role] = {
      userId: e?.userId || null,
      stake: e?.stake || S,
      fee: e?.fee || roundMoney(S * 0.1),
      pot: e?.pot || roundMoney(S * 0.9),
      hardLose: !!hardLoseRoles[role],
      payout: 0,
      refund: 0
    };
  }

  if (mode === "draw") {
    let nonHardK = 0;
    for (const role of losers) {
      if (hardLoseRoles[role]) continue;
      nonHardK += 1;
      const refund = roundMoney(0.5 * S);
      perRole[role].refund = refund;
      ensureUserBalance(perRole[role].userId).balances[currency] = roundMoney(ensureUserBalance(perRole[role].userId).balances[currency] + refund);
      const feeDelta = roundMoney(0.25 * S);
      balanceStore.platform.earned[currency] = roundMoney((balanceStore.platform.earned[currency] || 0) + feeDelta);
      platformEarnedDelta = roundMoney(platformEarnedDelta + feeDelta);
    }
    winnersPool = roundMoney(basePool - (0.25 * nonHardK * S));
  }

  const W = winners.length;
  if (W > 0) {
    const totalShares = W + 1;
    const mw = mainWinner || winners[0];
    for (const role of winners) {
      const share = role === mw ? 2 / totalShares : 1 / totalShares;
      const payout = roundMoney(winnersPool * share);
      perRole[role].payout = roundMoney(perRole[role].payout + payout);
      ensureUserBalance(perRole[role].userId).balances[currency] = roundMoney(ensureUserBalance(perRole[role].userId).balances[currency] + payout);
    }
  } else {
    balanceStore.platform.earned[currency] = roundMoney((balanceStore.platform.earned[currency] || 0) + winnersPool);
    platformEarnedDelta = roundMoney(platformEarnedDelta + winnersPool);
  }

  saveStoreAtomic(balanceStore);
  state.payoutBreakdown = { mode, N, S, currency, basePool, winnersPool, winners, losers, mainWinner: mainWinner || null, perRole, platformEarnedDelta };
  for (const p of room.players || []) {
    if (p?.ws && p.userKey) sendBalanceToWs(p.ws, parseUserId(p.userKey));
  }
}

function removeRoleFromGame(room, state, role, toDiscard = true) {
  if (!room.roles.includes(role)) return;

  // move hand to discard
  if (toDiscard && state?.players?.[role]?.hand) {
    for (const c of state.players[role].hand) state.discard.push(c);
    state.players[role].hand.length = 0;
  }

  // remove from roles
  room.roles = room.roles.filter(r => r !== role);

  // remove player hand holder
  if (state.players[role]) delete state.players[role];

  // if role was defender/attacker fix
  if (state.attacker === role) state.attacker = room.roles[0] || null;
  if (state.defender === role) state.defender = room.roles[0] ? nextRole(room, state.attacker) : null;

  // remove any table cards that belong to role? table cards are anonymous, keep.
  // re-init takingPass
  resetTakingPass(room, state);

  // normalize
  normalizeTurn(room, state);
}

function canTransfer(room, state, role, card) {
  if (!room.allowTransfer) return false;
  if (state.defender !== role) return false;
  if (state.phase !== "defend") return false;

  // Transfer allowed only if there is at least one attack and NO defended cards yet
  const anyAttack = state.table.some(p => p && p.attack);
  if (!anyAttack) return false;
  const anyDefended = state.table.some(p => p && p.defend);
  if (anyDefended) return false;

  // Card rank must match rank on table (any attack)
  const ranks = ranksOnTable(state);
  return ranks.has(card.rank);
}

function applyAction(room, state, role, action) {
  if (!action?.kind) return null;
  if (state.phase === "finished") return null;
  if (!room.roles.includes(role)) return null;

  const attacker = state.attacker;
  const defender = state.defender;

  // resign
  if (action.kind === "resign") {
    // 2p: immediate finish
    if (room.roles.length === 2) {
      state.phase = "finished";
      state.winners = [room.roles.find(r => r !== role)];
      state.loser = role;
      state.losers = [role];
      state.finishMode = room.winMode;
      state.message = "Сдача.";
      state.activeRole = null;
      state.deadlineTs = null;
      return state;
    }

    // 3+ classic: remove resigning role, move hand to discard, continue
    if (room.roles.length >= 3 && room.winMode === "classic") {
      state.message = "Игрок сдался.";
      removeRoleFromGame(room, state, role, true);

      if (room.roles.length < 2) {
        checkFinish(room, state);
        return state;
      }

      // if table has cards, keep them; turn must normalize
      normalizeTurn(room, state);
      state.phase = "attack"; // упрощенно: после сдачи — продолжаем с атакой
      state.table = state.table; // keep
      checkFinish(room, state);
      return state;
    }

    // for 3+ firstout — считаем сдачу как поражение для игрока и конец (упрощаем)
    state.phase = "finished";
    state.winners = room.roles.filter(r => r !== role);
    state.loser = role;
    state.losers = [role];
    state.finishMode = room.winMode;
    state.message = "Сдача.";
    state.activeRole = null;
    state.deadlineTs = null;
    return state;
  }

  // transfer
  if (action.kind === "transfer") {
    const cardId = action.cardId;
    if (!cardId) return null;
    const hand = state.players[role]?.hand;
    if (!hand) return null;
    const idx = hand.findIndex(c => c.id === cardId);
    if (idx === -1) return null;

    const card = hand[idx];
    if (!canTransfer(room, state, role, card)) return null;

    // put as new attack card into free slot
    const free = state.table.findIndex(p => !p);
    if (free === -1) return null;

    hand.splice(idx, 1);
    state.table[free] = { attack: card, defend: null };
    markFinishChanges(room, state);

    // defender becomes attacker, next player becomes defender
    const oldDef = state.defender;
    state.attacker = oldDef;
    state.defender = nextRole(room, oldDef);

    state.phase = "defend";
    state.message = "Перевод!";
    resetTakingPass(room, state);
    if (checkImmediateClassicWin(room, state)) return state;
    return state;
  }

  // attack
  if (action.kind === "attack") {
    const cardId = action.cardId;
    if (!cardId) return null;

    if (role === defender) return null;
    if (!(state.phase === "attack" || state.phase === "defend" || state.phase === "taking")) return null;
	if (state.phase === "taking") {
      if (!roleCanActInTaking(state, role, attacker, defender)) return null;
    }

    const hand = state.players[role]?.hand;
    if (!hand) return null;
    const idx = hand.findIndex(c => c.id === cardId);
    if (idx === -1) return null;

    const curPairs = state.table.filter(Boolean).length;

    if (curPairs === 0) {
      if (role !== attacker) return null;
      if (!canAttackAddMore(state)) return null;
      const free = state.table.findIndex(p => !p);
      if (free === -1) return null;
      const card = hand.splice(idx, 1)[0];
      state.table[free] = { attack: card, defend: null };
      markFinishChanges(room, state);
      state.phase = "defend";
      state.message = "";
      if (checkImmediateClassicWin(room, state)) return state;
      return state;
    }

    if (role !== attacker && state.phase !== "taking") return null;

    const allowedThrowIn = throwInRoles(room, state);
    if (!allowedThrowIn.includes(role)) return null;

    if (!canAttackAddMore(state)) return null;
    const ranks = ranksOnTable(state);
    if (!ranks.has(hand[idx].rank)) return null;

    const free = state.table.findIndex(p => !p);
    if (free === -1) return null;

    const card = hand.splice(idx, 1)[0];
    state.table[free] = { attack: card, defend: null };
    markFinishChanges(room, state);
    state.phase = "defend";
    state.message = "";
    if (checkImmediateClassicWin(room, state)) return state;
    return state;
  }

  // defend
  if (action.kind === "defend") {
    if (role !== defender) return null;
    if (state.phase !== "defend") return null;

    const { cardId, targetAttackId } = action;
    if (!cardId || !targetAttackId) return null;

    const hand = state.players[role]?.hand;
    if (!hand) return null;
    const idx = hand.findIndex(c => c.id === cardId);
    if (idx === -1) return null;

    const pairIndex = state.table.findIndex(p => p && p.attack.id === targetAttackId);
    if (pairIndex === -1) return null;

    const pair = state.table[pairIndex];
    if (pair.defend) return null;

    const defCard = hand[idx];
    if (!cardBeats(state.trumpSuit, defCard, pair.attack)) return null;

    pair.defend = hand.splice(idx, 1)[0];
    markFinishChanges(room, state);

	if (state.table.filter(Boolean).every(p => p.defend)) {
      state.phase = "taking";
      state.message = "Отбито. Подкиньте или нажмите ПАС.";
      state.takingReason = "bito";
      resetTakingPass(room, state);
    } else {
      state.phase = "defend";
    }
    if (state.phase !== "taking") state.message = "";
    if (checkImmediateClassicWin(room, state)) return state;
    return state;
  }

  // take
  if (action.kind === "take") {
    if (role !== defender) return null;
    const curPairs = state.table.filter(Boolean).length;
    if (curPairs === 0) return null;
    if (state.phase === "taking") return null;

    state.phase = "taking";
    state.message = "Защитник берёт. Подкиньте или нажмите ПАС.";
    state.takingReason = "take";
    resetTakingPass(room, state);
    return state;
  }

  // pass
  if (action.kind === "pass") {
if (state.phase !== "taking") return null;
    if (!roleCanActInTaking(state, role, attacker, defender)) return null;

    if (Object.prototype.hasOwnProperty.call(state.takingPass, role)) {
      state.takingPass[role] = true;
    }
    if (role === attacker) {
      state.takingLeaderPassed = true;
    }

    if (!allAttackersPassed(room, state)) return state;

return finalizeTakingRound(room, state, defender);
  }

  // end (bito)
  if (action.kind === "end") {
    if (state.phase === "taking") {
      if (!roleCanActInTaking(state, role, attacker, defender)) return null;

      if (Object.prototype.hasOwnProperty.call(state.takingPass, role)) {
        state.takingPass[role] = true;
      }
      if (role === attacker) state.takingLeaderPassed = true;
      if (!allAttackersPassed(room, state)) return state;
      return finalizeTakingRound(room, state, defender);
    }

    if (role !== attacker) return null;
    const curPairs = state.table.filter(Boolean).length;
    if (curPairs === 0) return null;
    if (!state.table.filter(Boolean).every(p => p.defend)) return null;

    state.phase = "taking";
    state.message = "Отбито. Подкиньте или нажмите ПАС.";
    state.takingReason = "bito";
    resetTakingPass(room, state);
    return state;
  }

  return null;
}

/** ======================
 *  Timers
 ======================= */

function clearRoomTimer(room) {
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;
}

function scheduleTurnTimer(room) {
  if (!room.state) return;
  clearRoomTimer(room);

  const state = room.state;
  if (state.phase === "finished") return;

  normalizeTurn(room, state);

  state.activeRole = computeActiveRole(state, room);
  state.deadlineTs = now() + TURN_MS;

  room.timer = setTimeout(() => onTimeout(room), TURN_MS + 80);
}

function onTimeout(room) {
  if (!room.state) return;
  const state = room.state;
  if (state.phase === "finished") return;

  normalizeTurn(room, state);
  const active = computeActiveRole(state, room);
  if (!active) return;

  const anyOnTable = state.table.some(Boolean);
  const allDef = anyOnTable && state.table.filter(Boolean).every(p => p.defend);

  if (active === state.defender && anyOnTable) {
    const next = applyAction(room, state, state.defender, { kind:"take" });
    if (next) room.state = next;
    scheduleTurnTimer(room);
    broadcastGameState(room);
    return;
  }

  if (active === state.attacker && allDef && state.phase === "attack") {
    const next = applyAction(room, state, state.attacker, { kind:"end" });
    if (next) room.state = next;
    scheduleTurnTimer(room);
    broadcastGameState(room);
    return;
  }

  if (state.phase === "taking") {
    const next = applyAction(room, state, active, { kind:"pass" });
    if (next) room.state = next;
    scheduleTurnTimer(room);
    broadcastGameState(room);
    return;
  }

  const mustAttackNow = active === state.attacker && state.phase === "attack" && !anyOnTable;
  const next = applyAction(room, state, active, { kind:"resign" });
  if (mustAttackNow) markHardLose(room, state, active);
  if (next) room.state = next;
  clearRoomTimer(room);
  broadcastGameState(room);
}

/** ======================
 *  State sanitize
 ======================= */

function sanitizeStateFor(room, role, state) {
  const profiles = {};
  for (const r of room.roles) profiles[r] = getPlayer(room, r)?.profile || { name:"", username:"", photoUrl:"" };

  const handsCount = {};
  for (const r of room.roles) handsCount[r] = state.players[r]?.hand?.length ?? 0;

  // include allowTransfer so UI can show zone
  return {
    youRole: role,
    roles: room.roles.slice(),
    profiles,
    winMode: room.winMode,
    allowTransfer: !!room.allowTransfer,
    throwInMode: room.throwInMode || "all",

    trumpSuit: state.trumpSuit,
    trumpCard: state.trumpCard ? { suit: state.trumpCard.suit, rank: state.trumpCard.rank } : null,
    deckCount: state.deck.length,
    discardCount: state.discard.length,

    attacker: state.attacker,
    defender: state.defender,
    attackers: throwInRoles(room, state),

    phase: state.phase,
    roundIndex: state.roundIndex,
    message: state.message || "",
    takingPass: state.takingPass || {},
    takingLeaderPassed: !!state.takingLeaderPassed,
    takingReason: state.takingReason || null,

    activeRole: state.activeRole,
    deadlineTs: state.deadlineTs,
    turnMs: TURN_MS,

    table: state.table.map(p => {
      if (!p) return null;
      return {
        attack: { id:p.attack.id, suit:p.attack.suit, rank:p.attack.rank },
        defend: p.defend ? { id:p.defend.id, suit:p.defend.suit, rank:p.defend.rank } : null
      };
    }),

    yourHand: state.players[role].hand.map(c => ({ id:c.id, suit:c.suit, rank:c.rank })),

    handsCount,
    winners: state.winners || [],
    loser: state.loser || null,
    losers: state.losers || [],
    mainWinner: state.mainWinner || null,
    finishMode: state.finishMode || room.winMode,
    winnersSeat: (state.winners || []).map(r => room.roles.indexOf(r)).filter(i => i >= 0),
    loserSeat: state.loser ? (room.roles.indexOf(state.loser) >= 0 ? room.roles.indexOf(state.loser) : null) : null,
    losersSeat: (state.losers || []).map(r => room.roles.indexOf(r)).filter(i => i >= 0),
    payoutBreakdown: state.payoutBreakdown || null
  };
}

/** ======================
 *  Lobby state
 ======================= */

function sendLobbyState(room) {
  const profiles = {};
  for (const r of room.roles) profiles[r] = { name:"", username:"", photoUrl:"" };
  for (const p of room.players) profiles[p.role] = p.profile;

  const players = room.roles.map(role => {
    const pl = getPlayer(room, role);
    return pl ? { role, userKey: pl.userKey, ready: !!pl.ready, connected: !!pl.ws } : { role, userKey:null, ready:false, connected:false };
  });

  for (const pl of room.players) {
    if (pl.ws && pl.ws.readyState === 1) {
      send(pl.ws, {
        type:"lobby_state",
        roomId: room.id,
        isPrivate: !!room.isPrivate,
        maxPlayers: room.maxPlayers,
        roles: room.roles,
        winMode: room.winMode,
        allowTransfer: !!room.allowTransfer,
        throwInMode: room.throwInMode || "all",
        stake: Number(room.stake || DEFAULT_STAKE),
        currency: room.currency || DEFAULT_CURRENCY,
        readyDeadlineTs: room.readyDeadlineTs,
        readyTimeoutMs: READY_MS,
        youRole: pl.role,
        players,
        profiles
      });
    }
  }
}

function tryStartGame(room) {
  if (room.state) return;
  const filled = room.roles.every(r => !!getPlayer(room, r));
  if (!filled) return;
  const allReady = room.roles.every(r => !!getPlayer(room, r)?.ready);
  if (!allReady) return;

  room.startedAt = now();
  room.state = createNewGame(room);
  clearReadyTimer(room);
  for (const p of room.players) p.ready = false;

  broadcastLobbyLists(); // hide from list now
  scheduleTurnTimer(room);
  broadcastGameState(room);
}

function broadcastGameState(room) {
  const state = room.state;
  if (!state) return;

  normalizeTurn(room, state);
  state.activeRole = computeActiveRole(state, room);

  for (const pl of room.players) {
    if (pl.ws && pl.ws.readyState === 1) {
      send(pl.ws, { type:"state", state: sanitizeStateFor(room, pl.role, state) });
    }
  }

  // if finished -> schedule reset back to lobby mode
  if (state.phase === "finished") {
    settleRoomPayouts(room, state);
    clearRoomTimer(room);
    scheduleFinishToLobby(room);
  }
}

function scheduleFinishToLobby(room) {
  if (room.finishResetTimer) return;
  room.finishResetTimer = setTimeout(() => {
    room.finishResetTimer = null;

    // Convert room back to lobby (so remaining can wait and start again)
    // Keep roles/players, but if roles < maxPlayers we still show in list
    room.state = null;
    room.startedAt = null;

    // ready flags reset
    for (const p of room.players) p.ready = false;

    scheduleReadyCountdown(room);
    sendLobbyState(room);
    broadcastLobbyLists();
  }, FINISH_TO_LOBBY_MS);
}

/** ======================
 *  Reconnect & leave
 ======================= */

function attachToRoomByUserKey(ws, userKey, profile) {
  const roomId = userToRoom.get(userKey);
  if (!roomId) return false;

  const room = rooms.get(roomId);
  if (!room) return false;

  const pl = room.players.find(p => p.userKey === userKey);
  if (!pl) return false;

  if (pl.ws && pl.ws !== ws) kickWs(pl.ws);
  pl.ws = ws;
  pl.profile = profile;
  ws.userKey = userKey;
  ws.roomId = roomId;

  if (room.disconnectTimers?.has(userKey)) {
    clearTimeout(room.disconnectTimers.get(userKey));
    room.disconnectTimers.delete(userKey);
  }

  broadcastLobbyLists();

  if (room.state) {
    send(ws, { type:"state", state: sanitizeStateFor(room, pl.role, room.state) });
  } else {
    scheduleReadyCountdown(room);
    sendLobbyState(room);
  }
  sendBalanceToWs(ws, parseUserId(userKey));
  return true;
}

function deleteRoomIfEmpty(room, { broadcast = true } = {}) {
  const count = room.players.filter(p => !!p.userKey).length;
  if (count !== 0) return false;

  clearRoomTimer(room);
  clearReadyTimer(room);
  if (room.finishResetTimer) clearTimeout(room.finishResetTimer);
  rooms.delete(room.id);

  if (broadcast) broadcastLobbyLists();
  return true;
}

function leaveRoom(ws) {
  const roomId = ws.roomId;
  const userKey = ws.userKey;
  if (!roomId || !userKey) return;

  const room = rooms.get(roomId);
  if (!room) return;

  const pl = room.players.find(p => p.userKey === userKey);
  if (!pl) return;

  pl.ws = null;

  if (!room.disconnectTimers) room.disconnectTimers = new Map();

  if (room.disconnectTimers.has(userKey)) clearTimeout(room.disconnectTimers.get(userKey));

  if (room.state && room.state.phase !== "finished") {
    const t = setTimeout(() => {
      const still = room.players.find(p => p.userKey === userKey && !p.ws);
      if (!still) return;

      userToRoom.delete(userKey);

      const st = room.state;
      markHardLose(room, st, still.role);
      const next = applyAction(room, st, still.role, { kind:"resign" });
      if (next) room.state = next;

      room.players = room.players.filter(p => p.userKey !== userKey);

      if (room.players.length === 0) {
        deleteRoomIfEmpty(room);
        return;
      }

      broadcastGameState(room);
      broadcastLobbyLists();
    }, REJOIN_GRACE_MS);

    room.disconnectTimers.set(userKey, t);
    broadcastLobbyLists();
    return;
  }

  // lobby or finished state: remove immediately
  userToRoom.delete(userKey);
  if (room.state) refundSeatStake(room, pl.role);
  else forfeitSeatStake(room, pl.role);
  room.players = room.players.filter(p => p.userKey !== userKey);
  if (room.players.length === 0) {
    deleteRoomIfEmpty(room);
    return;
  }
  scheduleReadyCountdown(room);
  sendLobbyState(room);
  broadcastLobbyLists();
}

/** ======================
 *  Create / Join / Quick
 ======================= */

function createRoomForHost({ hostWs, userKey, profile, maxPlayers, winMode, allowTransfer, throwInMode, isPrivate, password, stake, currency }) {
  const roomId = randId(isPrivate ? "PR" : "PU");
  const roles = buildRoles(maxPlayers);

  const room = {
    id: roomId,
    isPrivate: !!isPrivate,
    password: isPrivate ? safeStr(password || "", 32) : "",
    maxPlayers,
    winMode,
    allowTransfer: !!allowTransfer,
    throwInMode: throwInMode || "all",
    stake: Number(stake || DEFAULT_STAKE),
    currency: normalizeCurrency(currency || DEFAULT_CURRENCY) || DEFAULT_CURRENCY,
    roles,
    createdAt: now(),
    startedAt: null,
    players: [],
    state: null,
    timer: null,
    disconnectTimers: new Map(),
    finishResetTimer: null,
    readyTimer: null,
    readyDeadlineTs: null,
    escrow: {}
  };

  const lock = lockSeatStake(room, "p1", userKey);
  if (!lock.ok) {
    send(hostWs, { type:"error", message: lock.error || "Недостаточно средств" });
    return null;
  }

  const host = { ws: hostWs, userKey, role:"p1", profile, ready:false };
  room.players.push(host);

  rooms.set(roomId, room);
  userToRoom.set(userKey, roomId);

  hostWs.roomId = roomId;
  hostWs.userKey = userKey;

  send(hostWs, { type:"lobby_created", roomId, youRole:"p1" });
  sendBalanceToWs(hostWs, parseUserId(userKey));
  scheduleReadyCountdown(room);
  sendLobbyState(room);
  broadcastLobbyLists();
  return room;
}

function joinRoom({ ws, userKey, profile, roomId, password }) {
  const room = rooms.get(roomId);
  if (!room) {
    send(ws, { type:"error", message:"Комната не найдена" });
    return null;
  }

  if (room.isPrivate) {
    const pass = safeStr(password || "", 32);
    if (!room.password || pass !== room.password) {
      send(ws, { type:"error", message:"Неверный пароль" });
      return null;
    }
  }

  // reconnect if exists
  const existing = room.players.find(p => p.userKey === userKey);
  if (existing) {
    if (existing.ws && existing.ws !== ws) kickWs(existing.ws);
    existing.ws = ws;
    existing.profile = profile;

    userToRoom.set(userKey, room.id);
    ws.roomId = room.id;
    ws.userKey = userKey;

    if (room.disconnectTimers.has(userKey)) {
      clearTimeout(room.disconnectTimers.get(userKey));
      room.disconnectTimers.delete(userKey);
    }

    send(ws, { type:"joined", roomId: room.id, youRole: existing.role });
    sendBalanceToWs(ws, parseUserId(userKey));
    if (room.state) send(ws, { type:"state", state: sanitizeStateFor(room, existing.role, room.state) });
    else {
      scheduleReadyCountdown(room);
      sendLobbyState(room);
    }

    broadcastLobbyLists();
    return room;
  }

  // Can't join while game active (hidden anyway), unless finished reset in progress: allow join if no active game
  if (room.state && room.state.phase !== "finished") {
    send(ws, { type:"error", message:"Игра уже началась" });
    return null;
  }

  // if finished but not yet reset -> allow join (he will see finished state then lobby)
  // find free role
  const used = new Set(room.players.map(p => p.role));
  const free = room.roles.find(r => !used.has(r));
  if (!free) {
    send(ws, { type:"error", message:"Комната заполнена" });
    return null;
  }

  room.players.push({ ws, userKey, role: free, profile, ready:false });
  const lock = lockSeatStake(room, free, userKey);
  if (!lock.ok) {
    room.players = room.players.filter(p => !(p.userKey === userKey && p.role === free));
    send(ws, { type:"error", message: lock.error || "Недостаточно средств" });
    return null;
  }
  userToRoom.set(userKey, room.id);
  ws.roomId = room.id;
  ws.userKey = userKey;

  send(ws, { type:"joined", roomId: room.id, youRole: free });
  sendBalanceToWs(ws, parseUserId(userKey));

  if (room.state) send(ws, { type:"state", state: sanitizeStateFor(room, free, room.state) });
  else {
    scheduleReadyCountdown(room);
    sendLobbyState(room);
  }

  broadcastLobbyLists();
  return room;
}

/** ======================
 *  WS handlers
 ======================= */

wss.on("connection", (ws) => {
  ws.userKey = null;
  ws.roomId = null;

  send(ws, { type:"hello_ack" });
  send(ws, { type:"lobbies", ...buildLobbyLists() });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // HELLO
    if (msg.type === "hello") {
      const userKey = safeStr(msg.userKey || "", 128);
      if (!userKey) return;
      const profile = sanitizeProfile(msg.profile);
      setUserMetaFromProfile(parseUserId(userKey), profile);

      if (attachToRoomByUserKey(ws, userKey, profile)) return;

      ws.userKey = userKey;
      sendBalanceToWs(ws, parseUserId(userKey));
      send(ws, { type:"lobbies", ...buildLobbyLists() });
      return;
    }

    if (msg.type === "profile_update") {
      const userKey = safeStr(ws.userKey || "", 128);
      if (!userKey) return;
      const profile = sanitizeProfile(msg.profile);
      setUserMetaFromProfile(parseUserId(userKey), profile);
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room) return;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) return;
      pl.profile = profile;
      if (room.state) broadcastGameState(room);
      else sendLobbyState(room);
      broadcastLobbyLists();
      return;
    }

    if (msg.type === "wallet_bind") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      const userId = parseUserId(userKey);
      if (!userId) return;
      const walletAddress = safeStr(msg.walletAddress || "", 128);
      ensureUserBalance(userId).walletAddress = walletAddress;
      saveStoreAtomic(balanceStore);
      pushBalanceToUser(userId);
      return;
    }

    if (msg.type === "list_lobbies") {
      send(ws, { type:"lobbies", ...buildLobbyLists() });
	  return;
  }

  // QUICK
  if (msg.type === "quick_start" || msg.type === "quick_match") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (!userKey) { send(ws, { type:"error", message:"Нет userKey" }); return; }

      const profile = sanitizeProfile(msg.profile);
      setUserMetaFromProfile(parseUserId(userKey), profile);

      const quickCurrency = normalizeCurrency(msg.currency || (Number(msg.stake) === 1 ? "ton" : "stars")) || "ton";
      const quickStake = Number(msg.stake || (quickCurrency === "ton" ? 1 : 50));
      if (quickCurrency === "ton" && (!Number.isInteger(quickStake) || quickStake < 1)) {
        send(ws, { type:"error", message:"Ставка TON: минимум 1, шаг 1" });
        return;
      }
      if (quickCurrency === "stars" && (!Number.isInteger(quickStake) || quickStake < 50 || quickStake % 50 !== 0)) {
        send(ws, { type:"error", message:"Ставка Stars: минимум 50, шаг 50" });
        return;
      }

      // Быстрая игра: переводной + подкид ото всех (стандарт)
      const winMode = randomChoice(["classic", "draw"]);
      const allowTransfer = true;
      const throwInMode = "all";

      // 1) ищем любое свободное публичное stars-лобби с той же ставкой
      let found = null;
      for (const r of rooms.values()) {
        if (!r || r.isPrivate) continue;
        if ((r.currency || DEFAULT_CURRENCY) !== quickCurrency) continue;
        if (Number(r.stake || 0) !== quickStake) continue;
        if (r.state?.phase && r.state.phase !== "lobby") continue; // игра уже идёт/закончена
        const playersCount = (r.players || []).length;
        if (playersCount >= (r.maxPlayers || 2)) continue;
        found = r;
        break;
      }

      if (found) {
        joinRoom({ ws, userKey, profile, roomId: found.id });
        send(ws, { type:"quick_status", searching:false, matched:true, roomId: found.id });
        return;
      }

      // 2) иначе — создать новое публичное лобби на 2
      const room = createRoomForHost({
        hostWs: ws,
        userKey,
        profile,
        maxPlayers: 2,
        winMode,
        allowTransfer,
        throwInMode,
        isPrivate: false,
        password: "",
        stake: quickStake,
        currency: quickCurrency
      });

      if (!room) return;

      send(ws, { type:"quick_status", searching:false, matched:true, roomId: room.id });
      return;
    }


    if (msg.type === "quick_cancel") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (quickQueue.waiting && quickQueue.waiting.userKey === userKey) quickQueue.waiting = null;
      send(ws, { type:"quick_status", searching:false });
      return;
    }
    // AVATAR EMOJI (broadcast)
    if (msg.type === "emoji") {
      const userKey = safeStr(ws.userKey || "", 128);
      if (!userKey) return;

      const emoji = safeStr(msg.emoji || "", 8);
      const allowed = new Set(["🤡","😎","😘"]);
      if (!allowed.has(emoji)) return;

      const room = getRoomByUserKey(userKey);
      if (!room) return;

      // role of this user in this room
      const pl = (room.players || []).find(p => p && p.userKey === userKey) || null;
      const role = pl?.role || "";
      if (!role) return;

      const seat = room.roles.indexOf(role);
      broadcastToRoom(room, { type:"emoji", seat, role, emoji });
      return;
    }

    // CREATE
    if (msg.type === "create_lobby") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (!userKey) { send(ws, { type:"error", message:"Нет userKey" }); return; }

      const profile = sanitizeProfile(msg.profile);
      setUserMetaFromProfile(parseUserId(userKey), profile);
      const requestedPlayers = Number(msg.maxPlayers);
      if (!Number.isInteger(requestedPlayers) || requestedPlayers < 2 || requestedPlayers > 6) {
        send(ws, { type:"error", message:"Некорректное количество игроков" });
        return;
      }
      const maxPlayers = requestedPlayers;
      const winMode = (msg.winMode === "draw") ? "draw" : "classic";
      const allowTransfer = !!msg.allowTransfer;
      const throwInMode = (msg.throwInMode === "neighbors" && maxPlayers >= 4) ? "neighbors" : "all";
      const password = safeStr(msg.password || "", 32);
      const isPrivate = password.length > 0;
      const currency = normalizeCurrency(msg.currency || DEFAULT_CURRENCY);
      const stake = Number(msg.stake);
      if (!currency) { send(ws, { type:"error", message:"Некорректная валюта" }); return; }
      if (!Number.isFinite(stake) || stake <= 0) { send(ws, { type:"error", message:"Некорректная ставка" }); return; }
      if (currency === "ton" && (!Number.isInteger(stake) || stake < 1 || stake % 1 !== 0)) {
        send(ws, { type:"error", message:"Ставка TON: минимум 1, шаг 1" });
        return;
      }
      if (currency === "stars" && (!Number.isInteger(stake) || stake < 50 || stake % 50 !== 0)) {
        send(ws, { type:"error", message:"Ставка Stars: минимум 50, шаг 50" });
        return;
      }

      createRoomForHost({ hostWs: ws, userKey, profile, maxPlayers, winMode, allowTransfer, throwInMode, isPrivate, password, stake, currency });
      return;
    }

    // JOIN
    if (msg.type === "join_lobby") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (!userKey) { send(ws, { type:"error", message:"Нет userKey" }); return; }

      const profile = sanitizeProfile(msg.profile);
      setUserMetaFromProfile(parseUserId(userKey), profile);
      const roomId = safeStr(msg.roomId || "", 64);
      const password = safeStr(msg.password || "", 32);

      joinRoom({ ws, userKey, profile, roomId, password });
      return;
    }

    if (msg.type === "leave_lobby") {
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room) { ws.roomId = null; return; }
      if (room.state && room.state.phase !== "finished") return;

      const userKey = ws.userKey;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) { ws.roomId = null; return; }

      if (room.disconnectTimers?.has(userKey)) {
        clearTimeout(room.disconnectTimers.get(userKey));
        room.disconnectTimers.delete(userKey);
      }

      userToRoom.delete(userKey);
      refundSeatStake(room, pl.role);
      room.players = room.players.filter(p => p.userKey !== userKey);
      ws.roomId = null;

      if (room.players.length === 0) {
        deleteRoomIfEmpty(room);
        return;
      }

      scheduleReadyCountdown(room);
      sendLobbyState(room);
      broadcastLobbyLists();
      return;
    }

    // READY
    if (msg.type === "ready") {
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room) return;
      if (room.state) return;

      const userKey = ws.userKey;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) return;

      if (playersCount(room) !== room.maxPlayers) return;

      pl.ready = !!msg.ready;
      scheduleReadyCountdown(room);
      sendLobbyState(room);
      tryStartGame(room);
      return;
    }

    // ACTION
    if (msg.type === "action") {
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room || !room.state) return;

      const userKey = ws.userKey;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) return;

      if (room.state.phase === "finished") return;

      const next = applyAction(room, room.state, pl.role, msg.action);
      if (!next) return;

      room.state = next;

      if (room.state.phase === "finished") {
        clearRoomTimer(room);
      } else {
        scheduleTurnTimer(room);
      }

      broadcastGameState(room);
      return;
    }
  });

  ws.on("close", () => {
    if (quickQueue.waiting && quickQueue.waiting.ws === ws) quickQueue.waiting = null;
    if (ws.kicked) return;
    if (ws.roomId && ws.userKey) leaveRoom(ws);
  });
});

server.on("upgrade", (req, socket, head) => {
  if (req.url !== "/ws") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

logEnvWarnings();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`🃏 Durak WS started on ws://0.0.0.0:${PORT}/ws`);
  if (process.env.BOT_TOKEN) {
    setInterval(() => { pollTelegramUpdates(); }, 2500);
    pollTelegramUpdates();
  }
});
