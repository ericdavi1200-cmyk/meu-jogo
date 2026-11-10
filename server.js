'use strict';
/* Servidor online do Roleplay Imperial — até 200 jogadores.
   Fala o mesmo protocolo que o jogo já usa: hello, login/reg, ok, join, leave,
   pos/st, chat, save, inv/invr, give, voice, count, err. */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const MAX = +process.env.MAX_PLAYERS || 200;          // limite de jogadores online
const MAX_IP = +process.env.MAX_PER_IP || 20;         // conexões por IP
const TRUST_PROXY = process.env.TRUST_PROXY === '1';  // 1 se estiver atrás de proxy (Render, Nginx...)
const ADMINS = new Set((process.env.ADMINS || '').toLowerCase().split(',').map(s => s.trim()).filter(Boolean)); // quem pode usar "give"
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data', 'players.json');
const PUB = path.join(__dirname, 'public');
const NEAR2 = 300 * 300, VOICE2 = 80 * 80;

/* ---------- contas (senha com scrypt, nunca em texto puro) ---------- */
let db = {}, dirty = false;
try { db = JSON.parse(fs.readFileSync(DATA, 'utf8')); } catch (e) {}
function saveDb(sync) {
  if (!dirty) return; dirty = false;
  try { fs.mkdirSync(path.dirname(DATA), { recursive: true }); fs.writeFileSync(DATA + '.tmp', JSON.stringify(db)); fs.renameSync(DATA + '.tmp', DATA); }
  catch (e) { console.error('Erro ao salvar contas:', e.message); dirty = true; }
}
setInterval(saveDb, 5000);
const hash = (p, salt) => new Promise((ok, no) => crypto.scrypt(p, salt, 32, (e, k) => e ? no(e) : ok(k)));

/* ---------- estado ---------- */
const players = new Map(), byName = new Map(), ipCount = new Map(), ipTries = new Map();
let nextId = 1001;
const pub = q => ({ id: q.id, n: q.name, x: q.x, z: q.z, h: q.h, f: q.f });
const d2 = (a, b) => (a.x - b.x) ** 2 + (a.z - b.z) ** 2;
const raw = (ws, s) => { if (ws.readyState === 1 && ws.bufferedAmount < 1e6) ws.send(s); };
const send = (ws, o) => raw(ws, JSON.stringify(o));
const err = (ws, m) => send(ws, { t: 'err', m });
function bcast(o, except) { const s = JSON.stringify(o); for (const q of players.values()) if (q !== except) raw(q.ws, s); }

/* ---------- login / cadastro ---------- */
async function auth(ws, c, d) {
  if (c.P || c.busy) return;
  const now = Date.now(), tr = (ipTries.get(c.ip) || []).filter(t => now - t < 60000);
  if (tr.length >= 8) return err(ws, 'Muitas tentativas. Aguarde 1 minuto.');
  tr.push(now); ipTries.set(c.ip, tr);
  const u = String(d.u || '').trim(), p = String(d.p || '');
  if (!/^[A-Za-z0-9_.\-]{3,14}$/.test(u)) return err(ws, 'Usuário: de 3 a 14 letras, números, _ . ou -');
  if (p.length < 4 || p.length > 64) return err(ws, 'Senha: de 4 a 64 caracteres');
  if (players.size >= MAX) return err(ws, 'Servidor cheio (' + MAX + '/' + MAX + '). Tente de novo em instantes.');
  const key = u.toLowerCase(); c.busy = true;
  try {
    let a = db[key];
    if (d.t === 'reg') {
      if (a) return err(ws, 'Esse usuário já existe');
      const salt = crypto.randomBytes(16).toString('hex');
      a = db[key] = { name: u, salt, hash: (await hash(p, salt)).toString('hex'), money: 50, deliv: 0, hasGun: false, x: 0, z: 0, h: 0 };
      dirty = true;
    } else {
      if (!a) return err(ws, 'Usuário ou senha incorretos');
      const h = await hash(p, a.salt);
      if (!crypto.timingSafeEqual(h, Buffer.from(a.hash, 'hex'))) return err(ws, 'Usuário ou senha incorretos');
    }
    if (byName.has(key)) return err(ws, 'Esta conta já está online');
    if (players.size >= MAX) return err(ws, 'Servidor cheio (' + MAX + '/' + MAX + '). Tente de novo em instantes.');
    const P = { id: nextId++, ws, name: a.name, key, acc: a, x: a.x || 0, z: a.z || 0, h: a.h || 0, f: 1, s: 0, dirty: false, chatT: [], voiceT: 0 };
    c.P = P; players.set(P.id, P); byName.set(key, P);
    send(ws, { t: 'ok', id: P.id, n: P.name, money: a.money, deliv: a.deliv, hasGun: !!a.hasGun, count: players.size, players: [...players.values()].filter(q => q !== P).map(pub) });
    bcast({ t: 'join', ...pub(P) }, P);
    bcast({ t: 'count', n: players.size });
  } finally { c.busy = false; }
}

/* ---------- mensagens do jogo ---------- */
function onMsg(ws, c, d) {
  if (d.t === 'login' || d.t === 'reg') return auth(ws, c, d).catch(e => { console.error(e); err(ws, 'Erro no servidor'); });
  const P = c.P; if (!P) return;
  const now = Date.now();
  if (now - c.w > 1000) { c.w = now; c.n = 0; } if (++c.n > 80) return; // anti-flood
  switch (d.t) {
    case 'pos': {
      const x = +d.x, z = +d.z, h = +d.h, s = +d.s;
      if (![x, z, h, s].every(Number.isFinite) || Math.abs(x) > 1e5 || Math.abs(z) > 1e5) return;
      P.x = x; P.z = z; P.h = h; P.f = d.f ? 1 : 0; P.s = Math.max(0, Math.min(200, s)); P.dirty = true; break;
    }
    case 'chat': {
      const m = String(d.m || '').slice(0, 200); if (!m) return;
      P.chatT = P.chatT.filter(t => now - t < 5000); if (P.chatT.length >= 20) return; P.chatT.push(now);
      if (m.startsWith('~~dm~~')) {               // mensagem privada: só vai para o destinatário
        const T = byName.get(m.slice(6).split('|')[0].toLowerCase());
        if (T && T !== P) send(T.ws, { t: 'chat', m, n: P.name });
        return;
      }
      bcast({ t: 'chat', m, n: P.name }); break;  // chat global (inclui quem enviou)
    }
    case 'save': {
      const a = P.acc, m = Math.floor(+d.money), dl = Math.floor(+d.deliv);
      if (Number.isFinite(m)) a.money = Math.max(0, Math.min(1e12, m));
      if (Number.isFinite(dl)) a.deliv = Math.max(0, Math.min(1e9, dl));
      a.hasGun = !!d.hasGun; dirty = true; break;
    }
    case 'inv': case 'invr': {
      const T = byName.get(String(d.to || '').toLowerCase()); if (!T || T === P || typeof d.o !== 'string') return;
      const o = { t: d.t, from: P.name, o: d.o.slice(0, 12) }; if (d.t === 'invr') o.ok = d.ok ? 1 : 0;
      send(T.ws, o); break;
    }
    case 'give':                                   // só ADMs listados em ADMINS
      if (ADMINS.has(P.key)) bcast({ t: 'give', to: d.to, k: String(d.k || '').slice(0, 8), n: +d.n || 0, from: P.name }); break;
    case 'voice': {                                // voz por proximidade (80 m)
      if (typeof d.d !== 'string' || d.d.length > 4000 || now - P.voiceT < 40) return; P.voiceT = now;
      const s = JSON.stringify({ t: 'voice', s: d.s, d: d.d, n: P.name });
      for (const q of players.values()) if (q !== P && d2(P, q) < VOICE2) raw(q.ws, s); break;
    }
  }
}

/* ---------- posições: perto a cada 100 ms, todos a cada 2 s ---------- */
let tk = 0;
setInterval(() => {
  const full = ++tk % 20 === 0, list = [...players.values()], mv = list.filter(p => p.dirty || full);
  if (!mv.length) return;
  for (const q of list) {
    const arr = [];
    for (const p of mv) if (p !== q && (full || d2(p, q) < NEAR2)) arr.push([p.id, p.x, p.z, p.h, p.f, p.s]);
    if (arr.length) send(q.ws, { t: 'st', p: arr });
  }
  for (const p of mv) p.dirty = false;
}, 100);
setInterval(() => { const n = Date.now(); for (const [k, v] of ipTries) if (!v.some(t => n - t < 60000)) ipTries.delete(k); }, 60000);

/* ---------- HTTP (serve o jogo da pasta public) + WebSocket em /ws ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript', '.json': 'application/json', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.mp3': 'audio/mpeg' };
const server = http.createServer((req, res) => {
  let u; try { u = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, online: players.size, max: MAX })); }
  const ROOT_IDX = path.join(__dirname, 'index.html');   // aceita o index.html solto, ao lado do server.js
  const isIdx = u === '/' || u === '/index.html';
  const f = isIdx && !fs.existsSync(path.join(PUB, 'index.html')) ? ROOT_IDX : path.normalize(path.join(PUB, u === '/' ? 'index.html' : u));
  if (f !== ROOT_IDX && f !== PUB && !f.startsWith(PUB + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, b) => {
    if (e) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('Não encontrado. Coloque o index.html na pasta public.'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' }); res.end(b);
  });
});
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
wss.on('connection', (ws, req) => {
  const ip = (TRUST_PROXY && String(req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || '?';
  if ((ipCount.get(ip) || 0) >= MAX_IP) return ws.close(1008, 'limite por IP');
  ipCount.set(ip, (ipCount.get(ip) || 0) + 1);
  const c = { ip, P: null, busy: false, w: 0, n: 0 }; ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {});
  ws.on('message', b => { let d; try { d = JSON.parse(b); } catch (e) { return; } if (d && typeof d.t === 'string') onMsg(ws, c, d); });
  ws.on('close', () => {
    const n = (ipCount.get(ip) || 1) - 1; n > 0 ? ipCount.set(ip, n) : ipCount.delete(ip);
    const P = c.P; if (!P) return;
    players.delete(P.id); byName.delete(P.key); Object.assign(P.acc, { x: P.x, z: P.z, h: P.h }); dirty = true;
    bcast({ t: 'leave', id: P.id }); bcast({ t: 'count', n: players.size });
  });
  send(ws, { t: 'hello', n: players.size, max: MAX });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.alive) { ws.terminate(); continue; } ws.alive = false; ws.ping(); } }, 30000);

process.on('uncaughtException', e => console.error('Erro:', e));
for (const s of ['SIGINT', 'SIGTERM']) process.on(s, () => { dirty = true; saveDb(true); process.exit(0); });
server.listen(PORT, () => console.log('Roleplay Imperial online na porta ' + PORT + ' · limite ' + MAX + ' jogadores'));
