import { readFile, rm, stat } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import type { ImageContent, TextContent } from "@mariozechner/pi-ai";
import type { AgentMessage } from "@mariozechner/pi-agent-core";
import type { ExtensionAPI, ExtensionContext } from "@mariozechner/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { CONFIG_PATH, SERVER_PATH, TEMP_DIR, ensureDirs, guessMediaType, readConfig, readJson, writeConfig, type ServerPointer, type TeleMultiConfig } from "./shared.ts";

interface ServerMessage {
	type: string;
	[key: string]: unknown;
}

interface IncomingFile {
	path: string;
	fileName?: string;
	isImage?: boolean;
	mimeType?: string;
}

interface PendingTurn {
	chatId: number;
	text: string;
	files: IncomingFile[];
	queuedAttachments: string[];
}

interface PendingTelegramUser {
	id: number;
	username?: string;
	first_name?: string;
	last_name?: string;
	requestedAt?: number;
}

const SYSTEM_PROMPT_SUFFIX = `

Telegram multiplexer extension is active.
- Messages forwarded from Telegram are prefixed with "[telegram]" and may include local file paths for attachments.
- If a [telegram] user asks for a file or generated artifact, call telemulti_attach with the local path instead of only mentioning it.
- Replies from this pi instance are broadcast to Telegram chats connected to this workspace.`;

const RECONNECT_MIN_MS = 1_000;
const RECONNECT_MAX_MS = 15_000;
const CONNECT_TIMEOUT_MS = 2_500;
const MAX_ATTACHMENTS_PER_TURN = 10;
const SERVER_PROTOCOL_VERSION = 10;

export default function (pi: ExtensionAPI) {
	let config: TeleMultiConfig = {};
	let ws: WebSocket | undefined;
	let connected = false;
	let connecting = false;
	let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	let reconnectDelay = RECONNECT_MIN_MS;
	let setupInProgress = false;
	let currentCtx: ExtensionContext | undefined;
	let activeTurn: PendingTurn | undefined;
	let queuedTurns: PendingTurn[] = [];
	let currentAbort: (() => void) | undefined;
	let assistantBuffer = "";
	let shuttingDown = false;
	const pendingListResolvers = new Map<string, (users: PendingTelegramUser[]) => void>();

	function packageRoot(): string {
		return dirname(fileURLToPath(import.meta.url));
	}

	function updateStatus(ctx?: ExtensionContext, extra?: string): void {
		const target = ctx ?? currentCtx;
		if (!target) return;
		const theme = target.ui.theme;
		const label = theme.fg("accent", "telemulti");
		let value = "not configured";
		if (config.botToken) value = connected ? "connected" : "disconnected";
		if (extra) value += ` ${extra}`;
		target.ui.setStatus("telemulti", `${label} ${theme.fg(connected ? "success" : "muted", value)}`);
	}

	function send(message: ServerMessage): void {
		if (!ws || ws.readyState !== WebSocket.OPEN) return;
		ws.send(JSON.stringify(message));
	}

	function userLabel(user: PendingTelegramUser): string {
		const handle = user.username ? `@${user.username}` : [user.first_name, user.last_name].filter(Boolean).join(" ");
		return `${user.id}${handle ? ` — ${handle}` : ""}`;
	}

	async function requestPendingUsers(ctx: ExtensionContext): Promise<PendingTelegramUser[]> {
		if (!connected) await ensureServerAndConnect(ctx);
		if (!connected) throw new Error("Telegram multiplexer server is not connected");
		const requestId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
		const users = await new Promise<PendingTelegramUser[]>((resolve, reject) => {
			const timer = setTimeout(() => {
				pendingListResolvers.delete(requestId);
				reject(new Error("Timed out waiting for pending Telegram users"));
			}, 3_000);
			pendingListResolvers.set(requestId, (value) => {
				clearTimeout(timer);
				resolve(value);
			});
			send({ type: "pending-list", requestId });
		});
		return users;
	}

	async function validateToken(token: string): Promise<TeleMultiConfig | undefined> {
		const response = await fetch(`https://api.telegram.org/bot${token}/getMe`);
		const data = (await response.json()) as { ok: boolean; result?: { id: number; username?: string }; description?: string };
		if (!data.ok || !data.result) throw new Error(data.description || "Invalid Telegram bot token");
		return { ...config, botToken: token, botId: data.result.id, botUsername: data.result.username, wizardDone: true };
	}

	async function runSetupWizard(ctx: ExtensionContext): Promise<void> {
		if (!ctx.hasUI || setupInProgress) return;
		setupInProgress = true;
		try {
			const token = (await ctx.ui.input("Telegram bot token (leave empty to skip)", config.botToken || ""))?.trim();
			if (!token) {
				config = { ...config, wizardDone: true };
				await writeConfig(config);
				ctx.ui.notify("Telegram multiplexer setup skipped. Run /telemulti-setup to configure later.", "info");
				updateStatus(ctx);
				return;
			}

			ctx.ui.setStatus("telemulti", `${ctx.ui.theme.fg("accent", "telemulti")} ${ctx.ui.theme.fg("warning", "validating Telegram token...")}`);
			ctx.ui.setWidget("telemulti-setup", ["⏳ Validating Telegram bot token..."]);
			const next = await validateToken(token);
			if (!next) return;
			ctx.ui.setStatus("telemulti", `${ctx.ui.theme.fg("accent", "telemulti")} ${ctx.ui.theme.fg("warning", "waiting for workspace folder selection")}`);
			ctx.ui.setWidget("telemulti-setup", ["✅ Telegram token validated.", "Choose the workspaces folder to finish setup."]);
			const currentFolderChoice = `current folder (${ctx.cwd})`;
			const parentFolderChoice = `parent folder (${dirname(ctx.cwd)})`;
			const typeChoice = "type";
			const choice = await ctx.ui.select("Workspaces folder", [parentFolderChoice, currentFolderChoice, typeChoice]);
			let root = dirname(ctx.cwd);
			if (choice === currentFolderChoice) root = ctx.cwd;
			if (choice === typeChoice) {
				const typed = (await ctx.ui.input("Absolute or relative workspaces folder", config.workspacesRoot || ctx.cwd))?.trim();
				if (typed) root = typed.startsWith("/") ? typed : join(ctx.cwd, typed);
			}
			config = { ...next, workspacesRoot: root, wizardDone: true };
			await writeConfig(config);
			ctx.ui.notify(`Telegram multiplexer configured for @${config.botUsername ?? "bot"}; workspaces root: ${root}`, "info");
			await ensureServerAndConnect(ctx);
		} catch (error) {
			ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
		} finally {
			setupInProgress = false;
			ctx.ui.setWidget("telemulti-setup", []);
			updateStatus(ctx);
		}
	}

	async function pointerAlive(pointer: ServerPointer): Promise<boolean> {
		try {
			const response = await fetch(pointer.url.replace(/^ws:/, "http:"), { signal: AbortSignal.timeout(1000) });
			return response.ok;
		} catch {
			return false;
		}
	}

	async function startServer(): Promise<ServerPointer> {
		await ensureDirs();
		const serverPath = join(packageRoot(), "server.mjs");
		const child = spawn(process.execPath, [serverPath], {
			detached: true,
			stdio: "ignore",
			env: { ...process.env, TELEMULTI_SERVER_TOKEN: Math.random().toString(36).slice(2) + Date.now().toString(36) },
		});
		child.unref();
		const deadline = Date.now() + 8_000;
		let last: ServerPointer | undefined;
		while (Date.now() < deadline) {
			await new Promise((resolve) => setTimeout(resolve, 250));
			last = await readJson<ServerPointer | undefined>(SERVER_PATH, undefined);
			if (last && last.pid === child.pid && (await pointerAlive(last))) return last;
		}
		throw new Error("Timed out starting telemulti local server");
	}

	async function killServerPointer(): Promise<void> {
		const pointer = await readJson<ServerPointer | undefined>(SERVER_PATH, undefined);
		if (pointer?.pid && pointer.pid !== process.pid) {
			try {
				process.kill(pointer.pid, "SIGTERM");
			} catch {
				// already gone or not owned by us
			}
		}
		await rm(SERVER_PATH, { force: true }).catch(() => undefined);
	}

	async function getServerPointer(forceRestart = false): Promise<ServerPointer> {
		if (!forceRestart) {
			const pointer = await readJson<ServerPointer | undefined>(SERVER_PATH, undefined);
			if (pointer && (await pointerAlive(pointer))) return pointer;
		}
		if (forceRestart) await killServerPointer();
		return startServer();
	}

	async function connectToPointer(pointer: ServerPointer, ctx: ExtensionContext): Promise<void> {
		if (connecting) return;
		connecting = true;
		try {
			ws?.close();
			await new Promise<void>((resolve, reject) => {
				const socket = new WebSocket(`${pointer.url}?token=${encodeURIComponent(pointer.token)}`);
				const timer = setTimeout(() => {
					socket.close();
					reject(new Error("WebSocket connect timeout"));
				}, CONNECT_TIMEOUT_MS);
				socket.onopen = () => {
					clearTimeout(timer);
					ws = socket;
					connected = true;
					reconnectDelay = RECONNECT_MIN_MS;
					updateStatus(ctx);
					send({ type: "hello", workspace: ctx.cwd, pid: process.pid, session: ctx.sessionManager.getSessionFile() });
					resolve();
				};
				socket.onerror = () => {
					clearTimeout(timer);
					reject(new Error("WebSocket connection failed"));
				};
				socket.onclose = () => {
					connected = false;
					if (shuttingDown) return;
					updateStatus();
					scheduleReconnect(ctx);
				};
				socket.onmessage = (event) => void handleServerMessage(String(event.data), ctx);
			});
		} finally {
			connecting = false;
		}
	}

	function scheduleReconnect(ctx: ExtensionContext): void {
		if (reconnectTimer || !config.botToken) return;
		reconnectTimer = setTimeout(() => {
			reconnectTimer = undefined;
			void ensureServerAndConnect(ctx).catch((error) => {
				updateStatus(ctx, error instanceof Error ? error.message : String(error));
				reconnectDelay = Math.min(RECONNECT_MAX_MS, reconnectDelay * 2);
				scheduleReconnect(ctx);
			});
		}, reconnectDelay);
	}

	async function ensureServerAndConnect(ctx: ExtensionContext, forceRestart = false): Promise<void> {
		if (!config.botToken) return;
		const pointer = await getServerPointer(forceRestart);
		await connectToPointer(pointer, ctx).catch(async () => {
			const fresh = await getServerPointer(true);
			await connectToPointer(fresh, ctx);
		});
	}

	function isAssistantMessage(message: AgentMessage): boolean {
		return (message as unknown as { role?: string }).role === "assistant";
	}

	function getMessageText(message: AgentMessage): string {
		const content = ((message as unknown as { content?: unknown[] }).content ?? []) as Array<{ type?: string; text?: string }>;
		return content.filter((block) => block.type === "text" && typeof block.text === "string").map((block) => block.text).join("");
	}

	async function buildContent(turn: PendingTurn): Promise<Array<TextContent | ImageContent>> {
		const content: Array<TextContent | ImageContent> = [{ type: "text", text: turn.text }];
		for (const file of turn.files) {
			if (!file.isImage) continue;
			const mediaType = file.mimeType || guessMediaType(file.path);
			if (!mediaType) continue;
			const buffer = await readFile(file.path);
			content.push({ type: "image", data: buffer.toString("base64"), mimeType: mediaType } as unknown as ImageContent);
		}
		return content;
	}

	async function dispatchNext(ctx: ExtensionContext): Promise<void> {
		if (!ctx.isIdle() || activeTurn || queuedTurns.length === 0) return;
		const next = queuedTurns.shift();
		if (!next) return;
		activeTurn = next;
		assistantBuffer = "";
		pi.sendUserMessage(await buildContent(next));
	}

	async function handleServerMessage(raw: string, ctx: ExtensionContext): Promise<void> {
		let msg: ServerMessage;
		try { msg = JSON.parse(raw) as ServerMessage; } catch { return; }
		if (msg.type === "hello") {
			if (msg.serverVersion !== SERVER_PROTOCOL_VERSION) {
				ctx.ui.notify("Old Telegram multiplexer server detected; restarting local server...", "info");
				ws?.close();
				ws = undefined;
				connected = false;
				await killServerPointer();
				await ensureServerAndConnect(ctx, true).catch((error) => updateStatus(ctx, error instanceof Error ? error.message : String(error)));
			}
			return;
		}
		if (msg.type === "prompt") {
			queuedTurns.push({ chatId: Number(msg.chatId), text: String(msg.text || ""), files: (msg.files as IncomingFile[]) || [], queuedAttachments: [] });
			await dispatchNext(ctx);
		}
		if (msg.type === "pending-list") {
			const users = (msg.pendingUsers as PendingTelegramUser[] | undefined) ?? [];
			const requestId = typeof msg.requestId === "string" ? msg.requestId : undefined;
			if (requestId && pendingListResolvers.has(requestId)) {
				const resolve = pendingListResolvers.get(requestId);
				pendingListResolvers.delete(requestId);
				resolve?.(users);
				return;
			}
			ctx.ui.notify(users.length ? users.map(userLabel).join(" | ") : "No pending Telegram users", "info");
		}
		if (msg.type === "notice") ctx.ui.notify(String(msg.text || ""), "info");
	}

	pi.registerTool({
		name: "telemulti_attach",
		label: "Telegram Multi Attach",
		description: "Queue local files to be sent to Telegram chats connected to this workspace with the next reply.",
		promptSnippet: "Queue local files to be sent with the next Telegram multiplexer reply.",
		promptGuidelines: ["When handling a [telegram] message and the user asked for a file or generated artifact, call telemulti_attach with the local path instead of only mentioning it."],
		parameters: Type.Object({ paths: Type.Array(Type.String(), { minItems: 1, maxItems: MAX_ATTACHMENTS_PER_TURN }) }),
		async execute(_toolCallId, params) {
			if (!activeTurn) throw new Error("telemulti_attach can only be used while replying to a Telegram multiplexer turn");
			for (const path of params.paths) {
				const s = await stat(path);
				if (!s.isFile()) throw new Error(`Not a file: ${path}`);
				activeTurn.queuedAttachments.push(path);
			}
			return { content: [{ type: "text", text: `Queued ${params.paths.length} Telegram attachment(s).` }], details: { paths: params.paths } };
		},
	});

	pi.registerCommand("telemulti-setup", { description: "Run Telegram multiplexer setup wizard", handler: async (_args, ctx) => { config = await readConfig(); await runSetupWizard(ctx); } });
	pi.registerCommand("telemulti-status", { description: "Show Telegram multiplexer status", handler: async (_args, ctx) => ctx.ui.notify(`config: ${CONFIG_PATH} | server: ${connected ? "connected" : "disconnected"} | workspace: ${ctx.cwd}`, "info") });
	pi.registerCommand("telemulti-disconnect", { description: "Disconnect this pi instance from the Telegram multiplexer server", handler: async (_args, ctx) => { ws?.close(); ws = undefined; connected = false; updateStatus(ctx); } });
	pi.registerCommand("telemulti-reset", {
		description: "Clear Telegram multiplexer settings, approvals, and chat connections",
		handler: async (_args, ctx) => {
			const ok = await ctx.ui.confirm("Reset Telegram multiplexer?", "This clears the bot token, setup state, approvals, and chat/workspace connections.");
			if (!ok) return;
			send({ type: "reset" });
			await killServerPointer();
			config = {};
			await writeConfig(config);
			queuedTurns = [];
			activeTurn = undefined;
			ws?.close();
			ws = undefined;
			connected = false;
			updateStatus(ctx);
			ctx.ui.notify("Telegram multiplexer local settings reset. Run /telemulti-setup to configure again.", "info");
		},
	});
	pi.registerCommand("telemulti-pending", {
		description: "Review pending Telegram accounts in the TUI and approve or reject one",
		handler: async (_args, ctx) => {
			try {
				const users = await requestPendingUsers(ctx);
				if (users.length === 0) {
					ctx.ui.notify("No pending Telegram users", "info");
					return;
				}
				const selected = await ctx.ui.select("Pending Telegram users", users.map(userLabel));
				if (!selected) return;
				const user = users.find((candidate) => userLabel(candidate) === selected);
				if (!user) return;
				const action = await ctx.ui.select(`Handle ${userLabel(user)}`, ["approve", "reject", "cancel"]);
				if (action === "approve") send({ type: "approve", userId: user.id });
				if (action === "reject") send({ type: "reject", userId: user.id });
			} catch (error) {
				ctx.ui.notify(error instanceof Error ? error.message : String(error), "error");
			}
		},
	});
	pi.registerCommand("telemulti-approve", { description: "Approve a pending Telegram user id", handler: async (args, ctx) => { const id = args.trim(); if (!id) return ctx.ui.notify("Usage: /telemulti-approve <telegram-user-id>", "error"); send({ type: "approve", userId: id }); } });
	pi.registerCommand("telemulti-reject", { description: "Reject a pending Telegram user id", handler: async (args, ctx) => { const id = args.trim(); if (!id) return ctx.ui.notify("Usage: /telemulti-reject <telegram-user-id>", "error"); send({ type: "reject", userId: id }); } });

	pi.on("session_start", async (_event, ctx) => {
		currentCtx = ctx;
		await ensureDirs();
		config = await readConfig();
		updateStatus(ctx);
		if (!config.wizardDone) await runSetupWizard(ctx);
		if (config.botToken) await ensureServerAndConnect(ctx).catch((error) => updateStatus(ctx, error instanceof Error ? error.message : String(error)));
	});

	pi.on("session_shutdown", async () => {
		shuttingDown = true;
		currentCtx = undefined;
		if (reconnectTimer) clearTimeout(reconnectTimer);
		reconnectTimer = undefined;
		ws?.close();
		ws = undefined;
		connected = false;
		queuedTurns = [];
		activeTurn = undefined;
		currentAbort = undefined;
	});

	pi.on("before_agent_start", async (event) => ({ systemPrompt: event.systemPrompt + SYSTEM_PROMPT_SUFFIX }));
	pi.on("agent_start", async (_event, ctx) => { currentAbort = () => ctx.abort(); if (!activeTurn && queuedTurns.length > 0) activeTurn = queuedTurns.shift(); assistantBuffer = ""; updateStatus(ctx); });
	pi.on("message_update", async (event) => { if (!activeTurn || !isAssistantMessage(event.message)) return; assistantBuffer = getMessageText(event.message); });
	pi.on("agent_end", async (event, ctx) => {
		currentAbort = undefined;
		const turn = activeTurn;
		activeTurn = undefined;
		if (turn) {
			let finalText = assistantBuffer.trim();
			for (let i = event.messages.length - 1; i >= 0; i--) {
				const m = event.messages[i];
				if (isAssistantMessage(m)) { finalText = getMessageText(m).trim() || finalText; break; }
			}
			send({ type: "assistant", chatId: turn.chatId, text: finalText, files: turn.queuedAttachments });
		}
		await dispatchNext(ctx);
		updateStatus(ctx);
	});
}
