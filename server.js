const http = require('http'), crypto = require('crypto'), fs = require('fs'), path = require('path');
const PORT = process.env.PORT || 3000;
const MAX_ROOMS = 300, MAX_PEERS = 40, MAX_MSG = 16384, MAX_PRES = 12288;
const rooms = new Map();
const FILES = { '/': 'index.html', '/index.html': 'index.html', '/three.min.js': 'three.min.js', '/manifest.json': 'manifest.json', '/icon-192.png': 'icon-192.png', '/icon-512.png': 'icon-512.png' };
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'application/javascript; charset=utf-8', '.json': 'application/json', '.png': 'image/png' };

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, rooms: rooms.size })); }
  const f = FILES[u];
  const p = f && path.join(__dirname, f);
  if (!p || !fs.existsSync(p)) { res.writeHead(404); return res.end('Not found'); }
  res.writeHead(200, { 'Content-Type': TYPES[path.extname(p)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
  fs.createReadStream(p).pipe(res);
});

function frame(str, op = 1) {
  const d = Buffer.from(str), n = d.length;
  const h = n < 126 ? Buffer.from([128 | op, n]) : n < 65536 ? Buffer.from([128 | op, 126, n >> 8, n & 255]) : (() => { const b = Buffer.alloc(10); b[0] = 128 | op; b[1] = 127; b.writeBigUInt64BE(BigInt(n), 2); return b; })();
  return Buffer.concat([h, d]);
}
function send(c, obj) { if (c.destroyed || !c.writable) return; try { c.write(frame(typeof obj === 'string' ? obj : JSON.stringify(obj))); } catch (e) {} }

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = sock;
  c.buf = Buffer.alloc(0); c.alive = true; c.room = null; c.peer = null; c.frags = []; c.tokens = 200; c.tt = Date.now();
  c.on('data', d => { c.buf = Buffer.concat([c.buf, d]); try { parse(c); } catch (e) { c.destroy(); } });
  c.on('close', () => leave(c));
  c.on('error', () => leave(c));
});

function parse(c) {
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], fin = b0 & 128, op = b0 & 15, masked = b1 & 128;
    let len = b1 & 127, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) { c.destroy(); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (c.buf.length < need) return;
    let mask = null; if (masked) { mask = c.buf.subarray(off, off + 4); off += 4; }
    const data = Buffer.from(c.buf.subarray(off, off + len));
    if (mask) for (let i = 0; i < len; i++) data[i] ^= mask[i & 3];
    c.buf = c.buf.subarray(need);
    if (op === 8) { c.end(); return; }
    if (op === 9) { c.write(frame(data.toString(), 10)); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op === 1 || op === 0) {
      if (op === 1) c.frags = [];
      c.frags.push(data);
      if (fin) { const s = Buffer.concat(c.frags).toString(); c.frags = []; handle(c, s); }
    }
  }
}

function handle(c, s) {
  c.alive = true;
  const now = Date.now(); c.tokens = Math.min(200, c.tokens + (now - c.tt) * 0.15); c.tt = now;
  if (c.tokens < 1) return; c.tokens--;
  let m; try { m = JSON.parse(s); } catch (e) { return; }
  if (m.t === 'join' && !c.room) {
    const name = String(m.room || '');
    if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name)) return send(c, { t: 'err', m: 'bad room' });
    let r = rooms.get(name);
    if (!r) { if (rooms.size >= MAX_ROOMS) return send(c, { t: 'err', m: 'full' }); r = { name, peers: new Map() }; rooms.set(name, r); }
    if (r.peers.size >= MAX_PEERS) return send(c, { t: 'err', m: 'room full' });
    c.peer = crypto.randomBytes(6).toString('hex'); c.room = r; c.pres = {};
    r.peers.set(c.peer, c);
    send(c, { t: 'welcome', peer: c.peer });
    send(c, { t: 'peers', list: [...r.peers.values()].map(p => ({ peer: p.peer, presence: p.pres })) });
    bcast(r, c, { t: 'join', peer: c.peer });
  } else if (m.t === 'pr' && c.room && m.p && typeof m.p === 'object') {
    const next = { ...c.pres };
    for (const k of Object.keys(m.p)) { if (m.p[k] === null) delete next[k]; else next[k] = m.p[k]; }
    if (JSON.stringify(next).length > MAX_PRES) return;
    c.pres = next;
    bcast(c.room, c, { t: 'pr', peer: c.peer, p: m.p });
    send(c, { t: 'ack', id: m.id });
  } else if (m.t === 'ping') send(c, { t: 'pong', id: m.id });
}
function bcast(r, from, obj) { const s = JSON.stringify(obj); for (const p of r.peers.values()) if (p !== from) send(p, s); }
function leave(c) {
  const r = c.room; if (!r) return; c.room = null;
  r.peers.delete(c.peer); bcast(r, c, { t: 'left', peer: c.peer });
  if (!r.peers.size) rooms.delete(r.name);
}
setInterval(() => {
  for (const r of rooms.values()) for (const c of r.peers.values()) { if (!c.alive) { c.destroy(); continue; } c.alive = false; try { c.write(frame('', 9)); } catch (e) {} }
}, 20000);

server.listen(PORT, '0.0.0.0', () => {
  console.log('Server started on port ' + PORT);
});
