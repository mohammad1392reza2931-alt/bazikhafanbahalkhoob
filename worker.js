// worker.js — Little Soldier backend
// Handles: PvP room relay (WebSocket, via Durable Object "Arena") + D1 leaderboard API

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "Content-Type": "application/json", ...CORS },
  });
}

async function handleSubmit(request, env) {
  let body;
  try { body = await request.json(); } catch (e) { return jsonResponse({ error: "invalid json" }, 400); }
  const username = String(body.username || "").trim().slice(0, 14);
  const score = Number.isFinite(body.score) ? Math.max(0, Math.floor(body.score)) : 0;
  if (!username) return jsonResponse({ error: "username required" }, 400);
  const now = Date.now();

  await env.DB.prepare(
    `INSERT INTO scores (username, best_score, updated_at)
     VALUES (?, ?, ?)
     ON CONFLICT(username) DO UPDATE SET
       best_score = MAX(scores.best_score, excluded.best_score),
       updated_at = excluded.updated_at`
  ).bind(username, score, now).run();

  return jsonResponse({ ok: true });
}

async function handleLeaderboard(env) {
  const { results } = await env.DB.prepare(
    "SELECT username, best_score FROM scores ORDER BY best_score DESC LIMIT 10"
  ).all();
  return jsonResponse({ leaderboard: results });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS" && url.pathname.startsWith("/api/")) {
      return new Response(null, { headers: CORS });
    }
    if (url.pathname === "/api/submit" && request.method === "POST") {
      return handleSubmit(request, env);
    }
    if (url.pathname === "/api/leaderboard" && request.method === "GET") {
      return handleLeaderboard(env);
    }

    // Everything else (the raw WebSocket connection from the game) goes to the Arena Durable Object.
    const id = env.ARENA.idFromName("global-arena-manager");
    const stub = env.ARENA.get(id);
    return stub.fetch(request);
  }
};

// ---------- Durable Object: manages all PvP rooms in memory ----------
export class Arena {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rooms = new Map(); // code -> { max, sockets: Map(playerId -> ws), started }
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket", { status: 400 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    server.addEventListener("message", (ev) => this.onMessage(server, ev));
    server.addEventListener("close", () => this.onClose(server));
    server.addEventListener("error", () => this.onClose(server));
    return new Response(null, { status: 101, webSocket: client });
  }

  genCode() {
    const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let code;
    do {
      code = "";
      for (let i = 0; i < 5; i++) code += chars[Math.floor(Math.random() * chars.length)];
    } while (this.rooms.has(code));
    return code;
  }

  broadcastRoom(room, msg, excludeId) {
    const str = JSON.stringify(msg);
    room.sockets.forEach((sock, id) => {
      if (id !== excludeId) { try { sock.send(str); } catch (e) {} }
    });
  }

  onMessage(ws, ev) {
    let msg;
    try { msg = JSON.parse(ev.data); } catch (e) { return; }

    if (msg.type === "host") {
      const code = this.genCode();
      const max = msg.max === 4 ? 4 : 2;
      const room = { max, sockets: new Map(), started: false };
      room.sockets.set(0, ws);
      ws._roomCode = code;
      ws._playerId = 0;
      this.rooms.set(code, room);
      ws.send(JSON.stringify({ type: "hosted", code, playerId: 0 }));
      return;
    }

    if (msg.type === "join") {
      const code = String(msg.code || "").toUpperCase().trim();
      const room = this.rooms.get(code);
      if (!room) { ws.send(JSON.stringify({ type: "error", reason: "not-found" })); return; }
      if (room.started || room.sockets.size >= room.max) {
        ws.send(JSON.stringify({ type: "error", reason: "full" }));
        return;
      }
      let playerId = 0;
      while (room.sockets.has(playerId)) playerId++;
      room.sockets.set(playerId, ws);
      ws._roomCode = code;
      ws._playerId = playerId;
      ws.send(JSON.stringify({ type: "joined", playerId, count: room.sockets.size, max: room.max }));
      this.broadcastRoom(room, { type: "playerJoined", playerId, count: room.sockets.size, max: room.max }, playerId);
      if (room.sockets.size === room.max) {
        room.started = true;
        const ids = Array.from(room.sockets.keys());
        this.broadcastRoom(room, { type: "start", players: ids }, -1);
      }
      return;
    }

    const room = this.rooms.get(ws._roomCode);
    if (!room) return;

    if (msg.type === "state" || msg.type === "shoot") {
      this.broadcastRoom(room, Object.assign({}, msg, { from: ws._playerId }), ws._playerId);
    } else if (msg.type === "hit") {
      const target = room.sockets.get(msg.target);
      if (target) target.send(JSON.stringify({ type: "hit", target: msg.target, dmg: msg.dmg, from: ws._playerId }));
    } else if (msg.type === "died") {
      this.broadcastRoom(room, { type: "died", playerId: ws._playerId }, ws._playerId);
    }
  }

  onClose(ws) {
    const room = this.rooms.get(ws._roomCode);
    if (!room) return;
    room.sockets.delete(ws._playerId);
    this.broadcastRoom(room, { type: "playerLeft", playerId: ws._playerId }, ws._playerId);
    if (room.sockets.size === 0) this.rooms.delete(ws._roomCode);
  }
}
