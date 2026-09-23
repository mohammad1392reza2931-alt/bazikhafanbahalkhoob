// worker.js — How to Fish backend
// Handles: co-op room relay (WebSocket, via Durable Object "Dock") + D1 leaderboard API
// + chat relay with profanity filtering + report/ban system for the admin "Reports Panel"

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

    // Everything else (the raw WebSocket connection from the game, or the admin panel) goes to the Dock Durable Object.
    const id = env.DOCK.idFromName("global-dock-manager");
    const stub = env.DOCK.get(id);
    return stub.fetch(request);
  }
};

// ---------- profanity filter (best-effort; normalizes common obfuscation tricks) ----------
const BAD_ROOTS = [
  "kir","kos","jende","jendeh","kosk","kunn","koon","gooh","goh","madar","pedar sag","kese kesh",
  "sag","harom","haram zade","lash","laashi","zenika","javad","kiri","kosde","kosmagz","kholi",
  "fuck","shit","bitch","asshole","dick","pussy","cunt","bastard","motherfucker","nigger","nigga",
  "whore","slut","faggot","retard"
];
function normalizeForFilter(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[\u200c\u200b\u200e\u200f\u064b-\u065f\u0670]/g, "")
    .replace(/[أإآا]/g, "ا").replace(/ي/g, "ی").replace(/ك/g, "ک").replace(/ة/g, "ه").replace(/ؤ/g, "و").replace(/ئ/g, "ی")
    .replace(/[.\-_*٬،,\s]+/g, "")
    .replace(/(.)\1{2,}/g, "$1$1");
}
function containsProfanity(text) {
  const n = normalizeForFilter(text);
  if (!n) return false;
  for (const root of BAD_ROOTS) {
    if (n.includes(normalizeForFilter(root))) return true;
  }
  return false;
}

// ---------- Durable Object: manages all co-op fishing rooms + chat/report/ban state ----------
export class Dock {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.rooms = new Map(); // code -> { max, sockets: Map(playerId -> ws), started, msgSeq, recentMsgs: Map(mid -> {pid,uname,text,at}) }
    this.adminSockets = new Set();
    this.banned = new Map(); // pid -> {uname, ip, reason, at}
    this.reports = []; // recent reports, newest first
    this.state.blockConcurrencyWhile(async () => {
      const b = await this.state.storage.get("banned");
      if (b) this.banned = new Map(Object.entries(b));
      const r = await this.state.storage.get("reports");
      if (r) this.reports = r;
    });
  }

  async persistBanned() {
    await this.state.storage.put("banned", Object.fromEntries(this.banned.entries()));
  }
  async persistReports() {
    await this.state.storage.put("reports", this.reports);
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket", { status: 400 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server._ip = request.headers.get("CF-Connecting-IP") || "";
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

  broadcastAdmins(msg) {
    const str = JSON.stringify(msg);
    this.adminSockets.forEach((sock) => { try { sock.send(str); } catch (e) {} });
  }

  findBan(pid, ip) {
    if (pid && this.banned.has(pid)) return this.banned.get(pid);
    if (ip) {
      for (const rec of this.banned.values()) { if (rec.ip && rec.ip === ip) return rec; }
    }
    return null;
  }

  forEachSocket(fn) {
    this.rooms.forEach((room) => room.sockets.forEach((sock) => fn(sock)));
  }

  async onMessage(ws, ev) {
    let msg;
    try { msg = JSON.parse(ev.data); } catch (e) { return; }

    if (msg.type === "adminAuth") {
      if (this.env.ADMIN_KEY && String(msg.key || "") === this.env.ADMIN_KEY) {
        ws._isAdmin = true;
        this.adminSockets.add(ws);
        ws.send(JSON.stringify({
          type: "adminAuthed",
          reports: this.reports,
          banned: Array.from(this.banned.entries()).map(([pid, b]) => ({ pid, ...b })),
        }));
      } else {
        ws.send(JSON.stringify({ type: "adminAuthFail" }));
      }
      return;
    }

    if (msg.type === "ban") {
      if (!ws._isAdmin) return;
      const pid = String(msg.pid || "");
      if (!pid) return;
      const rec = { uname: String(msg.uname || ""), ip: String(msg.ip || ""), reason: String(msg.reason || "").slice(0, 200), at: Date.now() };
      this.banned.set(pid, rec);
      await this.persistBanned();
      this.forEachSocket((sock) => {
        if (sock._pid === pid || (rec.ip && sock._ip === rec.ip)) {
          try { sock.send(JSON.stringify({ type: "kicked", reason: rec.reason || "رفتار نامناسب در چت" })); } catch (e) {}
          try { sock.close(); } catch (e) {}
        }
      });
      this.broadcastAdmins({ type: "banned", pid, ...rec });
      return;
    }

    if (msg.type === "unban") {
      if (!ws._isAdmin) return;
      const pid = String(msg.pid || "");
      this.banned.delete(pid);
      await this.persistBanned();
      this.broadcastAdmins({ type: "unbanned", pid });
      return;
    }

    if (msg.type === "host" || msg.type === "join") {
      const pid = String(msg.pid || "").slice(0, 64) || ("anon-" + Math.random().toString(36).slice(2));
      const uname = (String(msg.uname || "ماهیگیر").trim().slice(0, 14)) || "ماهیگیر";
      const ban = this.findBan(pid, ws._ip);
      if (ban) {
        ws.send(JSON.stringify({ type: "error", reason: "banned", message: ban.reason || "" }));
        try { ws.close(); } catch (e) {}
        return;
      }
      ws._pid = pid;
      ws._uname = uname;
    }

    if (msg.type === "host") {
      const code = this.genCode();
      const max = Math.max(2, Math.min(8, Number(msg.max) || 8));
      const room = { max, sockets: new Map(), started: false, msgSeq: 0, recentMsgs: new Map() };
      room.sockets.set(0, ws);
      ws._roomCode = code;
      ws._playerId = 0;
      this.rooms.set(code, room);
      ws.send(JSON.stringify({ type: "hosted", code, playerId: 0, max }));
      return;
    }

    if (msg.type === "join") {
      const code = String(msg.code || "").toUpperCase().trim();
      const room = this.rooms.get(code);
      if (!room) { ws.send(JSON.stringify({ type: "error", reason: "not-found" })); return; }
      if (room.sockets.size >= room.max) {
        ws.send(JSON.stringify({ type: "error", reason: "full" }));
        return;
      }
      let playerId = 0;
      while (room.sockets.has(playerId)) playerId++;
      room.sockets.set(playerId, ws);
      ws._roomCode = code;
      ws._playerId = playerId;
      ws.send(JSON.stringify({ type: "joined", playerId, count: room.sockets.size, max: room.max }));
      this.broadcastRoom(room, { type: "playerJoined", playerId, count: room.sockets.size, max: room.max, uname: ws._uname }, playerId);
      if (!room.started) {
        room.started = true;
        const names = {};
        room.sockets.forEach((sock, id) => { names[id] = sock._uname || ""; });
        const ids = Array.from(room.sockets.keys());
        this.broadcastRoom(room, { type: "start", players: ids, names }, -1);
        ws.send(JSON.stringify({ type: "start", players: ids, names }));
      }
      return;
    }

    const room = this.rooms.get(ws._roomCode);
    if (!room) return;

    if (msg.type === "chat") {
      const text = String(msg.m || "").slice(0, 140).trim();
      if (!text) return;
      if (containsProfanity(text)) {
        ws.send(JSON.stringify({ type: "chatRejected" }));
        return;
      }
      room.msgSeq = (room.msgSeq || 0) + 1;
      const mid = "m" + room.msgSeq;
      room.recentMsgs.set(mid, { pid: ws._pid, uname: ws._uname, text, at: Date.now() });
      if (room.recentMsgs.size > 200) {
        const firstKey = room.recentMsgs.keys().next().value;
        room.recentMsgs.delete(firstKey);
      }
      this.broadcastRoom(room, { type: "chat", from: ws._playerId, u: ws._uname, m: text, mid }, -1);
      return;
    }

    if (msg.type === "report") {
      const rec = room.recentMsgs.get(String(msg.mid || ""));
      if (!rec) return;
      let reportedIp = "";
      room.sockets.forEach((sock) => { if (sock._pid === rec.pid) reportedIp = sock._ip || ""; });
      const report = {
        id: "r" + Date.now() + Math.random().toString(36).slice(2, 7),
        mid: msg.mid, roomCode: ws._roomCode,
        pid: rec.pid, ip: reportedIp, uname: rec.uname, text: rec.text, at: rec.at,
        reporterUname: ws._uname, reporterPid: ws._pid, reportedAt: Date.now(),
      };
      this.reports.unshift(report);
      if (this.reports.length > 300) this.reports.length = 300;
      await this.persistReports();
      this.broadcastAdmins({ type: "newReport", report });
      ws.send(JSON.stringify({ type: "reportAck", mid: msg.mid }));
      return;
    }

    // Fishing co-op relay: live position/heading + catch announcements + the generic net-packet tunnel.
    if (msg.type === "state" || msg.type === "catch") {
      this.broadcastRoom(room, Object.assign({}, msg, { from: ws._playerId }), ws._playerId);
    }
  }

  onClose(ws) {
    this.adminSockets.delete(ws);
    const room = this.rooms.get(ws._roomCode);
    if (!room) return;
    room.sockets.delete(ws._playerId);
    this.broadcastRoom(room, { type: "playerLeft", playerId: ws._playerId }, ws._playerId);
    if (room.sockets.size === 0) this.rooms.delete(ws._roomCode);
  }
}
