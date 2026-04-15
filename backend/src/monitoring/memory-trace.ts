// Periodic memory diagnostics. Writes one JSON line per sample to a dedicated
// trace file (configurable) and, if enabled, also stdout. Captures:
//   - process.memoryUsage() — rss, heapTotal, heapUsed, external, arrayBuffers
//   - Libp2p counters — connected peers, peerstore size, open streams
//   - Internal collections that previously leaked — dcutrPeers, bootstrapPeerIDs,
//     _lastPeerCounts, topicHandlers, peer-tracker entries/cumulativeBytes/subscriptions
//   - Downloader fleet — count, per-downloader queue/peers/retry counters
// Goal: correlate RSS growth with specific collection growth to pinpoint leaks.

import { appendFile } from 'node:fs/promises';

export type MemTraceSource = () => Record<string, unknown>;

const sources = new Map<string, MemTraceSource>();
let timer: ReturnType<typeof setInterval> | null = null;
let logPath: string | null = null;
let logToStdout = true;

export function registerMemTraceSource(name: string, fn: MemTraceSource): void {
	sources.set(name, fn);
}

export function unregisterMemTraceSource(name: string): void {
	sources.delete(name);
}

function toMB(bytes: number): number {
	return +(bytes / 1048576).toFixed(1);
}

function collect(): Record<string, unknown> {
	const mem = process.memoryUsage();
	const rssBytes = mem.rss;
	const snap: Record<string, unknown> = {
		ts: new Date().toISOString(),
		uptime_s: Math.round(process.uptime()),
		pid: process.pid,
		rss_mb: toMB(rssBytes),
		heap_used_mb: toMB(mem.heapUsed),
		heap_total_mb: toMB(mem.heapTotal),
		external_mb: toMB(mem.external),
		array_buffers_mb: toMB(mem.arrayBuffers ?? 0),
	};
	for (const [name, fn] of sources) {
		try {
			const data = fn();
			for (const [k, v] of Object.entries(data)) snap[`${name}.${k}`] = v;
		} catch (err) {
			snap[`${name}.error`] = (err as Error)?.message ?? String(err);
		}
	}
	return snap;
}

async function writeSample(): Promise<void> {
	const snap = collect();
	const line = JSON.stringify(snap);
	if (logToStdout) console.log(`[MEM-TRACE] ${line}`);
	if (logPath) {
		try {
			await appendFile(logPath, line + '\n');
		} catch (err) {
			console.error('[MEM-TRACE] write failed:', (err as Error).message);
		}
	}
}

export function startMemoryTrace(opts: { filePath?: string; intervalMs?: number; stdout?: boolean } = {}): void {
	if (timer) return;
	logPath = opts.filePath ?? null;
	logToStdout = opts.stdout ?? true;
	const interval = opts.intervalMs ?? 30_000;
	// Kick an immediate baseline sample.
	void writeSample();
	timer = setInterval(() => void writeSample(), interval);
	if (typeof (timer as any).unref === 'function') (timer as any).unref();
	console.log(`[MEM-TRACE] started (interval=${interval}ms, file=${logPath ?? 'none'}, stdout=${logToStdout})`);
}

export function stopMemoryTrace(): void {
	if (timer) clearInterval(timer);
	timer = null;
}
