// 셔틀 매치 — Cloudflare Workers 버전
// 게임 페이지를 보내 주고, 방 코드마다 Durable Object 하나가 그 방 사람들의 상태를 실시간으로 전달함
import HTML from './index.html';
import { DurableObject } from 'cloudflare:workers';

const PAGE = HTML.replace('<head>', '<head>\n<meta name="shuttle-server" content="1">');
const MAX_IN_ROOM = 16;
const MAX_PRES = 16 * 1024;
const KEEP = ['nm', 'ch', 'seat', 'st', 'mm'];   // 서버가 잠들었다 깨도 남겨 둘 로비 정보 (작게)

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      const code = String(url.searchParams.get('room') || 'MAIN').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8) || 'MAIN';
      return env.ROOMS.get(env.ROOMS.idFromName(code)).fetch(req);
    }
    if (url.pathname === '/health') return new Response('ok');
    return new Response(PAGE, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' } });
  }
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.pres = new Map();   // 사람 id → 전체 상태 (메모리)
  }
  info(ws) {
    const a = ws.deserializeAttachment(); if (!a) return null;
    if (!this.pres.has(a.id)) this.pres.set(a.id, { ...(a.keep || {}) });
    return { id: a.id, pres: this.pres.get(a.id) };
  }
  send(ws, m) { try { ws.send(JSON.stringify(m)); } catch (_) {} }
  toOthers(from, m) { const s = JSON.stringify(m); for (const w of this.ctx.getWebSockets()) if (w !== from) { try { w.send(s); } catch (_) {} } }

  async fetch(req) {
    const others = this.ctx.getWebSockets();
    if (others.length >= MAX_IN_ROOM) return new Response('room full', { status: 503 });
    const pair = new WebSocketPair(), client = pair[0], server = pair[1];
    const id = 'p' + crypto.randomUUID().replace(/-/g, '').slice(0, 10);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ id, keep: {} });
    this.pres.set(id, {});
    this.send(server, { t: 'hello', you: id, peers: others.map(w => this.info(w)).filter(Boolean).map(p => ({ peer: p.id, pres: p.pres })) });
    this.toOthers(server, { t: 'join', peer: id });
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, data) {
    let m;
    try { m = JSON.parse(typeof data === 'string' ? data : new TextDecoder().decode(data)); } catch (_) { return; }
    if (m && m.t === 'ping' && typeof m.c === 'number') { this.send(ws, { t: 'pong', c: m.c }); return; }
    if (!m || m.t !== 'p' || !m.patch || typeof m.patch !== 'object' || Array.isArray(m.patch)) return;
    const me = this.info(ws); if (!me) return;
    const next = { ...me.pres };
    for (const k in m.patch) { if (m.patch[k] === null) delete next[k]; else next[k] = m.patch[k]; }
    if (JSON.stringify(next).length > MAX_PRES) return;
    this.pres.set(me.id, next);
    if (KEEP.some(k => k in m.patch)) {
      const keep = {}; for (const k of KEEP) if (k in next) keep[k] = next[k];
      try { ws.serializeAttachment({ id: me.id, keep }); } catch (_) {}
    }
    this.toOthers(ws, { t: 'p', peer: me.id, patch: m.patch });
  }

  bye(ws) {
    const a = ws.deserializeAttachment(); if (!a || a.gone) return;
    try { ws.serializeAttachment({ ...a, gone: 1 }); } catch (_) {}
    this.pres.delete(a.id);
    this.toOthers(ws, { t: 'leave', peer: a.id });
  }
  async webSocketClose(ws, code) { this.bye(ws); try { ws.close(code === 1005 ? 1000 : code, 'bye'); } catch (_) {} }
  async webSocketError(ws) { this.bye(ws); }
}
