// Vercel serverless function: shared state for FPV Mission co-op rooms.
// Stores everything in Upstash Redis through its REST API (no npm packages needed).
const URL_ = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const TTL = 7200;          // a room disappears 2 hours after its last activity
const STALE_MS = 8000;     // a pilot who did not report for 8 s is dropped
const IDLE_RESET_MS = 60000; // a room with nobody active for 60 s restarts on next join
const MAX_PLAYERS = 6;

async function pipe(cmds) {
  const r = await fetch(URL_.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  const j = await r.json();
  return j.map((x) => {
    if (x && x.error) throw new Error(x.error);
    return x.result;
  });
}
const num = (x, a, b, d = 0) => (Number.isFinite(+x) ? Math.min(b, Math.max(a, +x)) : d);
const vec = (a, n, lim) => (Array.isArray(a) ? a.slice(0, n).map((x) => num(x, -lim, lim)) : new Array(n).fill(0));
function pairs(a) {
  const o = {};
  if (Array.isArray(a)) for (let i = 0; i + 1 < a.length; i += 2) o[a[i]] = a[i + 1];
  return o;
}
function freshCount(players, now) {
  let n = 0;
  for (const k in players) {
    try {
      if (now - JSON.parse(players[k]).ts < IDLE_RESET_MS) n++;
    } catch (e) { /* ignore broken rows */ }
  }
  return n;
}

export default async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') return res.status(405).json({ error: 'method' });
  if (!URL_ || !TOKEN) return res.status(500).json({ error: 'no_redis' });

  let b = req.body;
  if (typeof b === 'string') { try { b = JSON.parse(b); } catch (e) { b = null; } }
  if (!b || typeof b !== 'object') return res.status(400).json({ error: 'bad' });

  const code = String(b.code || '').toUpperCase();
  if (!/^[A-Z0-9]{4,8}$/.test(code)) return res.status(400).json({ error: 'code' });
  const id = String(b.id || '');
  if (!/^[a-z0-9]{4,24}$/i.test(id)) return res.status(400).json({ error: 'id' });
  const name = String(b.name || 'Pilot').replace(/[^\p{L}\p{N} _.-]/gu, '').slice(0, 14) || 'Pilot';

  const k = 'fpv:' + code, meta = k + ':meta', pl = k + ':p', dd = k + ':dead';
  const now = Date.now();

  try {
    let reset = !!b.reset;
    if (b.join || b.create) {
      const [exists, players] = await pipe([['EXISTS', meta], ['HGETALL', pl]]);
      const pmap = pairs(players);
      if (b.join && !exists) return res.status(404).json({ error: 'no_room' });
      const fresh = freshCount(pmap, now);
      if (b.create && exists && fresh > 0) return res.status(409).json({ error: 'exists' });
      if (b.join && !(id in pmap) && Object.keys(pmap).length >= MAX_PLAYERS && fresh >= MAX_PLAYERS) {
        return res.status(403).json({ error: 'full' });
      }
      if (fresh === 0) reset = true; // nobody around: start a clean mission
    }

    const seed = Math.floor(Math.random() * 1e9);
    const cmds = [];
    if (reset) cmds.push(['DEL', dd], ['HSET', meta, 't0', now, 'seed', seed]);
    cmds.push(['HSETNX', meta, 't0', now], ['HSETNX', meta, 'seed', seed]);
    if (b.leave) {
      cmds.push(['HDEL', pl, id]);
    } else {
      const player = {
        name,
        p: vec(b.p, 3, 5000),
        q: vec(b.q, 4, 1),
        v: vec(b.v, 3, 400),
        alive: !!b.alive,
        drone: String(b.drone || '').slice(0, 8),
        ts: now,
      };
      cmds.push(['HSET', pl, id, JSON.stringify(player)]);
    }
    const dead = (Array.isArray(b.dead) ? b.dead : []).filter((d) => /^[vs]\d{1,4}$/.test(String(d))).slice(0, 20);
    for (const d of dead) cmds.push(['HSET', dd, String(d), now]);
    cmds.push(['EXPIRE', meta, TTL], ['EXPIRE', pl, TTL], ['EXPIRE', dd, TTL]);
    cmds.push(['HGETALL', meta], ['HGETALL', pl], ['HGETALL', dd]);

    const out = await pipe(cmds);
    const m = pairs(out[out.length - 3]);
    const pm = pairs(out[out.length - 2]);
    const dm = pairs(out[out.length - 1]);

    const players = [], stale = [];
    for (const pid in pm) {
      try {
        const p = JSON.parse(pm[pid]);
        if (now - p.ts > STALE_MS) { stale.push(pid); continue; }
        players.push({ id: pid, ...p });
      } catch (e) { stale.push(pid); }
    }
    if (stale.length) await pipe([['HDEL', pl, ...stale]]);

    return res.status(200).json({
      now,
      t0: Number(m.t0),
      seed: Number(m.seed),
      players,
      dead: Object.keys(dm),
    });
  } catch (e) {
    return res.status(502).json({ error: 'upstream' });
  }
}
