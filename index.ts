import { readFile, stat } from "node:fs/promises";
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

const SYSTEM_PROMPT_SUFFIX = `

Telegram multiplexer extension is active.
- Messages forwarded from Telegram are prefixed with "[telegram]" and may include local file paths for attachments.
- If a [telegram] user asks for a file or generated artifact, call telemulti_attach with the local path instead of only mentioning it.
- Replies from this pi instance are broadcast to Telegram chats connected to this workspace.`;

const RECONNECT_MIN_MS = 1_000;
const RECONNECT_MAX_MS = 15_000;
const CONNECT_TIMEOUT_MS = 2_500;
const MAX_ATTACHMENTS_PER_TURN = 10;

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

			const next = await validateToken(token);
			if (!next) return;
			const choice = await ctx.ui.select("Workspaces folder", ["current folder", "parent folder", "type"]);
			let root = ctx.cwd;
			if (choice === "parent folder") root = dirname(ctx.cwd);
			if (choice === "type") {
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

	async function getServerPointer(forceRestart = false): Promise<ServerPointer> {
		if (!forceRestart) {
			const pointer = await readJson<ServerPointer | undefined>(SERVER_PATH, undefined);
			if (pointer && (await pointerAlive(pointer))) return pointer;
		}
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
					send({ type: "hello", workspace: ctx.cwd, session: ctx.sessionManager.getSessionFile() });
					resolve();
				};
				socket.onerror = () => {
					clearTimeout(timer);
					reject(new Error("WebSocket connection failed"));
				};
				socket.onclose = () => {
					connected = false;
					updateStatus(ctx);
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
		if (msg.type === "prompt") {
			queuedTurns.push({ chatId: Number(msg.chatId), text: String(msg.text || ""), files: (msg.files as IncomingFile[]) || [], queuedAttachments: [] });
			await dispatchNext(ctx);
		}
		if (msg.type === "pending-list") {
			const users = (msg.pendingUsers as Array<{ id: number; username?: string; first_name?: string }> | undefined) ?? [];
			ctx.ui.notify(users.length ? users.map((u) => `${u.id} ${u.username ? `@${u.username}` : u.first_name || ""}`).join(" | ") : "No pending Telegram users", "info");
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
	pi.registerCommand("telemulti-pending", { description: "List pending Telegram accounts awaiting approval", handler: async () => send({ type: "pending-list" }) });
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
