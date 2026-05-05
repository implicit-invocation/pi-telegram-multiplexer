import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { homedir } from "node:os";

export interface TeleMultiConfig {
	botToken?: string;
	botUsername?: string;
	botId?: number;
	workspacesRoot?: string;
	wizardDone?: boolean;
}

export interface ServerPointer {
	url: string;
	port: number;
	pid: number;
	token: string;
	updatedAt: number;
}

export const PI_AGENT_DIR = join(homedir(), ".pi", "agent");
export const CONFIG_PATH = join(PI_AGENT_DIR, "telemulti.json");
export const SERVER_PATH = join(PI_AGENT_DIR, "telemulti-server.json");
export const TEMP_DIR = join(PI_AGENT_DIR, "tmp", "telemulti");

export async function ensureDirs(): Promise<void> {
	await mkdir(PI_AGENT_DIR, { recursive: true });
	await mkdir(TEMP_DIR, { recursive: true });
}

export async function readJson<T>(path: string, fallback: T): Promise<T> {
	try {
		return JSON.parse(await readFile(path, "utf8")) as T;
	} catch {
		return fallback;
	}
}

export async function writeJson(path: string, value: unknown): Promise<void> {
	await mkdir(dirname(path), { recursive: true });
	await writeFile(path, JSON.stringify(value, null, "\t") + "\n", "utf8");
}

export async function readConfig(): Promise<TeleMultiConfig> {
	await ensureDirs();
	return readJson<TeleMultiConfig>(CONFIG_PATH, {});
}

export async function writeConfig(config: TeleMultiConfig): Promise<void> {
	await ensureDirs();
	await writeFile(CONFIG_PATH, JSON.stringify(config, null, "\t") + "\n", "utf8");
}

export function sanitizeFileName(name: string): string {
	return name.replace(/[^a-zA-Z0-9._-]+/g, "_");
}

export function guessExtensionFromMime(mimeType: string | undefined, fallback = ""): string {
	if (!mimeType) return fallback;
	const normalized = mimeType.toLowerCase();
	if (normalized === "image/jpeg") return ".jpg";
	if (normalized === "image/png") return ".png";
	if (normalized === "image/webp") return ".webp";
	if (normalized === "image/gif") return ".gif";
	if (normalized === "audio/ogg") return ".ogg";
	if (normalized === "audio/mpeg") return ".mp3";
	if (normalized === "video/mp4") return ".mp4";
	if (normalized === "application/pdf") return ".pdf";
	return fallback;
}

export function guessMediaType(path: string): string | undefined {
	const lower = path.toLowerCase();
	if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
	if (lower.endsWith(".png")) return "image/png";
	if (lower.endsWith(".webp")) return "image/webp";
	if (lower.endsWith(".gif")) return "image/gif";
	return undefined;
}
