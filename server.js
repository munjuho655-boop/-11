// 셔틀 매치 서버: 게임 페이지를 보내 주고, 같은 방 코드로 들어온 사람끼리 상태를 실시간으로 전달해 줌
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
// 게임 파일: 서버 옆의 index.html (없으면 public/index.html)
const PAGE = fs.existsSync(path.join(__dirname, 'index.html')) ? path.join(__dirname, 'index.html') : path.join(__dirname, 'public', 'index.html');
const MAX_IN_ROOM = 16;          // 한 방 최대 인원 (2명 경기 + 관전)
const MAX_PRES = 16 * 1024;      // 한 사람이 들고 있을 수 있는 상태 크기

let page = '';
function loadPage() {
  // 게임 파일에 "서버 모드" 표시를 넣어서 보냄 → 게임이 이 서버로 연결함
  page = fs.readFileSync(PAGE, 'utf8').replace('<head>', '<head>\n<meta name="shuttle-server" content="1">');
}
loadPage();

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  if (url !== '/' && url !== '/index.html') { res.writeHead(302, { location: '/' }); res.end(); return; }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
  res.end(page);
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });
const rooms = new Map();          // 방 코드 → Map(사람 id → { ws, pres })
const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };
const toOthers = (room, from, m) => { const s = JSON.stringify(m); for (const [id, p] of room) if (id !== from && p.ws.readyState === 1) p.ws.send(s); };

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const code = String(q.get('room') || 'MAIN').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'MAIN';
  let room = rooms.get(code);
  if (!room) rooms.set(code, room = new Map());
  if (room.size >= MAX_IN_ROOM) { ws.close(4001, 'room full'); return; }

  const id = 'p' + crypto.randomBytes(5).toString('hex');
  const me = { ws, pres: {} };
  room.set(id, me);
  send(ws, { t: 'hello', you: id, peers: [...room].filter(([k]) => k !== id).map(([k, v]) => ({ peer: k, pres: v.pres })) });
  toOthers(room, id, { t: 'join', peer: id });

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', buf => {
    let m;
    try { m = JSON.parse(buf); } catch (_) { return; }
    if (m && m.t === 'ping' && typeof m.c === 'number') { send(ws, { t: 'pong', c: m.c }); return; }   // 핑 재기
    if (!m || m.t !== 'p' || !m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
    const next = { ...me.pres };
    for (const k in m.patch) { if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k]; }
    if (JSON.stringify(next).length > MAX_PRES) return;
    me.pres = next;
    toOthers(room, id, { t: 'p', peer: id, patch: m.patch });
  });
  ws.on('close', () => {
    room.delete(id);
    toOthers(room, id, { t: 'leave', peer: id });
    if (room.size === 0) rooms.delete(code);
  });
});

// 끊긴 연결 정리
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 15000);

server.listen(PORT, () => {
  console.log(`셔틀 매치 서버 켜짐: http://localhost:${PORT}`);
  for (const list of Object.values(os.networkInterfaces())) for (const a of list || []) {
    if (a.family === 'IPv4' && !a.internal) console.log(`  같은 와이파이 친구는: http://${a.address}:${PORT}`);
  }
});
