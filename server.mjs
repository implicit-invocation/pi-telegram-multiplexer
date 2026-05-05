import { createServer } from "node:http";
import { randomBytes } from "node:crypto";
import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { basename, dirname, extname, isAbsolute, join, relative, resolve } from "node:path";
import { homedir } from "node:os";
import { spawn } from "node:child_process";
import { WebSocketServer } from "ws";

const PI_AGENT_DIR = join(homedir(), ".pi", "agent");
const CONFIG_PATH = join(PI_AGENT_DIR, "telemulti.json");
const SERVER_PATH = join(PI_AGENT_DIR, "telemulti-server.json");
const TEMP_DIR = join(PI_AGENT_DIR, "tmp", "telemulti");
const STATE_PATH = join(PI_AGENT_DIR, "telemulti-state.json");
const MAX_MESSAGE_LENGTH = 4096;
const HEARTBEAT_MS = 15_000;
const STALE_MS = 45_000;

const token = process.env.TELEMULTI_SERVER_TOKEN || randomBytes(24).toString("hex");
const packageRoot = dirname(new URL(import.meta.url).pathname);
const clients = new Map();
const workspaces = new Map();
const spawned = new Map();
let config = {};
let state = { approvedUsers: [], pendingUsers: [], chatWorkspaces: {}, createConfirmations: {}, lastUpdateId: undefined };
let serverPointer;
let telegramAbort = new AbortController();
let lastTelegramOkAt = Date.now();

async function ensureDirs() {
	await mkdir(PI_AGENT_DIR, { recursive: true });
	await mkdir(TEMP_DIR, { recursive: true });
}
async function readJson(path, fallback) { try { return JSON.parse(await readFile(path, "utf8")); } catch { return fallback; } }
async function writeJson(path, value) { await ensureDirs(); await writeFile(path, JSON.stringify(value, null, "\t") + "\n", "utf8"); }
function saveState() { return writeJson(STATE_PATH, state); }
function isApproved(userId) { return state.approvedUsers.includes(userId); }
function upsertPending(user) {
	if (isApproved(user.id)) return;
	const pending = state.pendingUsers.filter((u) => u.id !== user.id);
	pending.push({ id: user.id, username: user.username, first_name: user.first_name, last_name: user.last_name, requestedAt: Date.now() });
	state.pendingUsers = pending;
}
function chunkText(text) {
	const chunks = [];
	for (let i = 0; i < text.length; i += MAX_MESSAGE_LENGTH) chunks.push(text.slice(i, i + MAX_MESSAGE_LENGTH));
	return chunks.length ? chunks : [""];
}
function safeWorkspacePath(input) {
	const root = resolve(config.workspacesRoot || process.cwd());
	const target = isAbsolute(input) ? resolve(input) : resolve(root, input || ".");
	const rel = relative(root, target);
	if (!isAbsolute(input) && (rel.startsWith("..") || rel === ".." || isAbsolute(rel))) throw new Error("Workspace path escapes configured workspaces root");
	return target;
}
function workspaceKey(path) { return resolve(path); }
function publicWorkspaceList() { return [...workspaces.keys()].sort(); }
function send(ws, msg) { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg)); }
function broadcastServerStatus() {
	for (const client of clients.values()) send(client.ws, { type: "status", workspaces: publicWorkspaceList(), chats: state.chatWorkspaces });
}
async function callTelegram(method, body, signal = telegramAbort.signal) {
	if (!config.botToken) throw new Error("Telegram token missing");
	const response = await fetch(`https://api.telegram.org/bot${config.botToken}/${method}`, {
		method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body), signal,
	});
	const data = await response.json();
	if (!data.ok) throw new Error(data.description || `Telegram API ${method} failed`);
	lastTelegramOkAt = Date.now();
	return data.result;
}
async function callTelegramMultipart(method, fields, fileField, filePath, fileName) {
	if (!config.botToken) throw new Error("Telegram token missing");
	const form = new FormData();
	for (const [k, v] of Object.entries(fields)) form.set(k, String(v));
	const buffer = await readFile(filePath);
	form.set(fileField, new Blob([buffer]), fileName);
	const response = await fetch(`https://api.telegram.org/bot${config.botToken}/${method}`, { method: "POST", body: form });
	const data = await response.json();
	if (!data.ok) throw new Error(data.description || `Telegram API ${method} failed`);
	lastTelegramOkAt = Date.now();
	return data.result;
}
async function sendTelegramText(chatId, text) {
	for (const chunk of chunkText(text)) await callTelegram("sendMessage", { chat_id: chatId, text: chunk || " " }).catch(console.error);
}
async function sendTelegramFile(chatId, path) {
	const lower = path.toLowerCase();
	const isPhoto = [".jpg", ".jpeg", ".png", ".webp"].includes(extname(lower));
	await callTelegramMultipart(isPhoto ? "sendPhoto" : "sendDocument", { chat_id: chatId }, isPhoto ? "photo" : "document", path, basename(path)).catch(async (e) => sendTelegramText(chatId, `Failed to send ${path}: ${e.message}`));
}
function chatsForWorkspace(path) {
	const key = workspaceKey(path);
	return Object.entries(state.chatWorkspaces).filter(([, w]) => workspaceKey(w) === key).map(([chatId]) => Number(chatId));
}
async function broadcastWorkspace(path, text, files = []) {
	for (const chatId of chatsForWorkspace(path)) {
		if (text?.trim()) await sendTelegramText(chatId, text.trim());
		for (const file of files || []) await sendTelegramFile(chatId, file);
	}
}
function spawnPi(workspacePath) {
	const key = workspaceKey(workspacePath);
	if (workspaces.has(key) || spawned.has(key)) return;
	const command = process.env.TELEMULTI_PI_COMMAND || "pi";
	const child = spawn(command, ["--mode", "rpc", "-e", packageRoot], { cwd: key, detached: true, stdio: ["pipe", "ignore", "ignore"], env: { ...process.env, TELEMULTI_CHILD: "1" } });
	child.unref();
	spawned.set(key, { pid: child.pid, stdin: child.stdin, startedAt: Date.now() });
	setTimeout(() => spawned.delete(key), 30_000).unref();
}
async function connectChat(chatId, requestedPath) {
	const path = safeWorkspacePath(requestedPath || ".");
	try {
		const st = await stat(path);
		if (!st.isDirectory()) return sendTelegramText(chatId, `Not a directory: ${path}`);
		state.chatWorkspaces[String(chatId)] = path; await saveState();
		if (!workspaces.has(workspaceKey(path))) spawnPi(path);
		await sendTelegramText(chatId, `Connected this chat to ${path}${workspaces.has(workspaceKey(path)) ? "" : " (starting pi...)"}`);
		broadcastServerStatus();
	} catch {
		const id = randomBytes(4).toString("hex");
		state.createConfirmations[id] = { chatId, path, expiresAt: Date.now() + 10 * 60_000 };
		await saveState();
		await sendTelegramText(chatId, `Workspace does not exist: ${path}\nSend /confirm ${id} to create it and start pi.`);
	}
}
async function downloadTelegramFile(fileId, suggestedName) {
	const file = await callTelegram("getFile", { file_id: fileId });
	const target = join(TEMP_DIR, `${Date.now()}-${suggestedName.replace(/[^a-zA-Z0-9._-]+/g, "_")}`);
	const res = await fetch(`https://api.telegram.org/file/bot${config.botToken}/${file.file_path}`);
	if (!res.ok) throw new Error(`download failed ${res.status}`);
	await writeFile(target, Buffer.from(await res.arrayBuffer()));
	return target;
}
function guessExt(mime, fallback = "") {
	if (!mime) return fallback;
	if (mime === "image/jpeg") return ".jpg"; if (mime === "image/png") return ".png"; if (mime === "image/webp") return ".webp"; if (mime === "image/gif") return ".gif"; if (mime === "application/pdf") return ".pdf"; return fallback;
}
async function messageFiles(message) {
	const files = [];
	if (message.photo?.length) files.push({ id: message.photo.at(-1).file_id, name: `photo-${message.message_id}.jpg`, image: true, mime: "image/jpeg" });
	if (message.document) files.push({ id: message.document.file_id, name: message.document.file_name || `document-${message.message_id}${guessExt(message.document.mime_type)}`, image: message.document.mime_type?.startsWith("image/"), mime: message.document.mime_type });
	if (message.video) files.push({ id: message.video.file_id, name: message.video.file_name || `video-${message.message_id}.mp4`, image: false, mime: message.video.mime_type });
	if (message.audio) files.push({ id: message.audio.file_id, name: message.audio.file_name || `audio-${message.message_id}${guessExt(message.audio.mime_type, ".mp3")}`, image: false, mime: message.audio.mime_type });
	if (message.voice) files.push({ id: message.voice.file_id, name: `voice-${message.message_id}${guessExt(message.voice.mime_type, ".ogg")}`, image: false, mime: message.voice.mime_type });
	const out = [];
	for (const file of files) {
		const path = await downloadTelegramFile(file.id, file.name);
		out.push({ path, fileName: file.name, isImage: !!file.image, mimeType: file.mime });
	}
	return out;
}
async function forwardToWorkspace(message) {
	const workspacePath = state.chatWorkspaces[String(message.chat.id)];
	if (!workspacePath) return sendTelegramText(message.chat.id, "No workspace connected. Use /workspaces then /connect <workspace-path>.");
	const key = workspaceKey(workspacePath);
	if (!workspaces.has(key)) { spawnPi(key); return sendTelegramText(message.chat.id, `Workspace is not active yet; starting pi in ${key}. Please retry in a few seconds.`); }
	const files = await messageFiles(message);
	let text = `[telegram] chat ${message.chat.id}`;
	const raw = (message.text || message.caption || "").trim();
	if (raw) text += `\n${raw}`;
	if (files.length) text += `\n\nTelegram attachments saved locally:\n${files.map((f) => `- ${f.path}`).join("\n")}`;
	for (const client of clients.values()) if (client.workspace === key) send(client.ws, { type: "prompt", chatId: message.chat.id, text, files });
}
async function handleTelegramMessage(message) {
	if (!message.from || message.from.is_bot) return;
	const user = message.from;
	const text = (message.text || "").trim();
	if (text === "/start") {
		if (isApproved(user.id)) return sendTelegramText(message.chat.id, "approved. Use /workspaces or /connect <workspace-path>.");
		upsertPending(user); await saveState(); return sendTelegramText(message.chat.id, "pending approval");
	}
	if (!isApproved(user.id)) { upsertPending(user); await saveState(); return sendTelegramText(message.chat.id, "pending approval"); }
	if (text === "/workspaces") return sendTelegramText(message.chat.id, publicWorkspaceList().map((w) => `- ${w}`).join("\n") || "No active workspaces.");
	if (text.startsWith("/connect")) return connectChat(message.chat.id, text.replace(/^\/connect\s*/, "").trim() || ".");
	if (text.startsWith("/confirm")) {
		const id = text.replace(/^\/confirm\s*/, "").trim(); const item = state.createConfirmations[id];
		if (!item || item.chatId !== message.chat.id || item.expiresAt < Date.now()) return sendTelegramText(message.chat.id, "No pending confirmation with that id.");
		await mkdir(item.path, { recursive: true }); delete state.createConfirmations[id]; state.chatWorkspaces[String(message.chat.id)] = item.path; await saveState(); spawnPi(item.path); broadcastServerStatus(); return sendTelegramText(message.chat.id, `Created and connected to ${item.path}. Starting pi...`);
	}
	await forwardToWorkspace(message);
}
async function telegramLoop() {
	await callTelegram("deleteWebhook", { drop_pending_updates: false }).catch(() => {});
	while (!telegramAbort.signal.aborted) {
		try {
			const updates = await callTelegram("getUpdates", { offset: state.lastUpdateId === undefined ? undefined : state.lastUpdateId + 1, limit: 20, timeout: 30, allowed_updates: ["message", "edited_message"] });
			for (const update of updates) { state.lastUpdateId = update.update_id; await saveState(); const msg = update.message || update.edited_message; if (msg) await handleTelegramMessage(msg); }
		} catch (e) { if (!telegramAbort.signal.aborted) { console.error("telegram loop", e); await new Promise((r) => setTimeout(r, 3000)); } }
	}
}
async function main() {
	await ensureDirs(); config = await readJson(CONFIG_PATH, {}); state = { ...state, ...(await readJson(STATE_PATH, {})) };
	const http = createServer((req, res) => { res.writeHead(200); res.end("pi-telegram-multiplexer\n"); });
	const wss = new WebSocketServer({ server: http });
	wss.on("connection", (ws, req) => {
		const url = new URL(req.url || "/", "http://localhost");
		if (url.searchParams.get("token") !== token) { ws.close(1008, "bad token"); return; }
		const id = randomBytes(8).toString("hex"); const client = { id, ws, workspace: undefined, lastPong: Date.now() }; clients.set(id, client);
		ws.on("pong", () => { client.lastPong = Date.now(); });
		ws.on("message", async (raw) => {
			let msg; try { msg = JSON.parse(String(raw)); } catch { return; }
			if (msg.type === "hello") { client.workspace = workspaceKey(msg.workspace); workspaces.set(client.workspace, id); spawned.delete(client.workspace); send(ws, { type: "hello", ok: true, workspacesRoot: config.workspacesRoot }); broadcastServerStatus(); }
			if (msg.type === "assistant") await broadcastWorkspace(client.workspace, msg.text || "", msg.files || []);
			if (msg.type === "pending-list") send(ws, { type: "pending-list", pendingUsers: state.pendingUsers });
			if (msg.type === "approve") { const idNum = Number(msg.userId); if (!state.approvedUsers.includes(idNum)) state.approvedUsers.push(idNum); state.pendingUsers = state.pendingUsers.filter((u) => u.id !== idNum); await saveState(); send(ws, { type: "notice", text: `Approved ${idNum}` }); }
			if (msg.type === "reject") { const idNum = Number(msg.userId); state.pendingUsers = state.pendingUsers.filter((u) => u.id !== idNum); await saveState(); send(ws, { type: "notice", text: `Rejected ${idNum}` }); }
		});
		ws.on("close", () => { clients.delete(id); if (client.workspace && workspaces.get(client.workspace) === id) workspaces.delete(client.workspace); broadcastServerStatus(); });
	});
	setInterval(() => { for (const c of clients.values()) { if (Date.now() - c.lastPong > STALE_MS) c.ws.terminate(); else c.ws.ping(); } }, HEARTBEAT_MS).unref();
	setInterval(() => { if (Date.now() - lastTelegramOkAt > 5 * 60_000) { telegramAbort.abort(); telegramAbort = new AbortController(); lastTelegramOkAt = Date.now(); telegramLoop().catch(console.error); } }, 60_000).unref();
	http.listen(0, "127.0.0.1", async () => {
		const port = http.address().port; serverPointer = { url: `ws://127.0.0.1:${port}`, port, pid: process.pid, token, updatedAt: Date.now() }; await writeJson(SERVER_PATH, serverPointer); console.error(`telemulti server ${serverPointer.url}`); telegramLoop().catch(console.error);
	});
	async function shutdown() { telegramAbort.abort(); await rm(SERVER_PATH, { force: true }).catch(() => {}); process.exit(0); }
	process.on("SIGTERM", shutdown); process.on("SIGINT", shutdown);
}
main().catch((e) => { console.error(e); process.exit(1); });
