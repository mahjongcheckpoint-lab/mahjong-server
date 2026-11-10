// 麻將 Checkpoint 連線伺服器
// 每個房號對應一個 Durable Object「房間」。伺服器只負責轉送訊息和記錄誰在房間裡；
// 洗牌、發牌、判斷都在房主的瀏覽器裡進行，每個人的手牌在送出前已經加密，伺服器也看不到。
// 使用 WebSocket Hibernation：沒有人說話時房間會休眠，不計運算時間，最省免費額度。
import { DurableObject } from "cloudflare:workers";

const ROOM_RE = /^\/room\/([a-z0-9][a-z0-9_.-]{0,47})$/;
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;
const MAX_MESSAGE = 8192;     // 單則訊息上限（位元組）
const MAX_PRESENCE = 4096;    // 每個人的狀態上限
const MAX_PEERS = 12;         // 一個房間最多同時幾個連線（四位玩家＋觀戰）

const text = (body, status = 200) => new Response(body, { status, headers: { "content-type": "text/plain; charset=utf-8" } });

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/") return text("麻將 Checkpoint 連線伺服器運作中");
    const m = url.pathname.match(ROOM_RE);
    if (!m) return text("Not found", 404);
    if (request.headers.get("Upgrade") !== "websocket") return text("請用 WebSocket 連線", 426);
    const allowed = String(env.ALLOWED_ORIGINS || "").split(",").map(s => s.trim()).filter(Boolean);
    const origin = request.headers.get("Origin") || "";
    if (allowed.length && !allowed.includes("*") && !allowed.includes(origin)) return text("這個網址不能連線到伺服器", 403);
    const stub = env.ROOMS.get(env.ROOMS.idFromName(m[1]));
    return stub.fetch(request);
  }
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // 心跳訊息由 Cloudflare 自動回覆，不會叫醒房間，也不計費
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"t":"ping"}', '{"t":"pong"}'));
  }

  live() {
    return this.ctx.getWebSockets().filter(ws => { const a = ws.deserializeAttachment(); return a && !a.gone; });
  }

  async fetch(request) {
    const others = this.live();
    if (others.length >= MAX_PEERS) return text("房間人數已滿", 503);
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    const peer = crypto.randomUUID().replace(/-/g, "").slice(0, 16);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ peer, presence: {} });
    server.send(JSON.stringify({ t: "hi", peer, peers: others.map(ws => ws.deserializeAttachment()) }));
    this.broadcast({ t: "pj", peer, presence: {} }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const ws of this.live()) {
      if (ws === except) continue;
      try { ws.send(s); } catch (e) {}
    }
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > MAX_MESSAGE) return;
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    const me = ws.deserializeAttachment();
    if (!me || me.gone || !m || typeof m !== "object") return;
    if (m.t === "p" && m.d && typeof m.d === "object" && !Array.isArray(m.d)) {
      // 更新自己的狀態（暱稱、座位等），合併後轉給所有人
      const merged = { ...me.presence };
      for (const [k, v] of Object.entries(m.d)) { if (v === null) delete merged[k]; else merged[k] = v; }
      if (JSON.stringify(merged).length > MAX_PRESENCE) return;
      me.presence = merged;
      ws.serializeAttachment(me);
      this.broadcast({ t: "pu", peer: me.peer, presence: merged });
    } else if (m.t === "e" && typeof m.topic === "string" && TOPIC_RE.test(m.topic)) {
      // 一般訊息：轉給房間裡的每個人（包含自己）
      this.broadcast({ t: "e", topic: m.topic, d: m.d === undefined ? null : m.d, peer: me.peer });
    }
  }

  async webSocketClose(ws, code) {
    this.leave(ws);
    try { ws.close(code === 1005 || code === 1006 ? 1000 : code, "bye"); } catch (e) {}
  }

  async webSocketError(ws) {
    this.leave(ws);
  }

  leave(ws) {
    const me = ws.deserializeAttachment();
    if (!me || me.gone) return;
    me.gone = true;
    ws.serializeAttachment(me);
    this.broadcast({ t: "pl", peer: me.peer }, ws);
  }
}
