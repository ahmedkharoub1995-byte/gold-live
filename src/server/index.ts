import { DurableObject } from "cloudflare:workers";

const SYMBOL = "XAU/USD";
const TIMEZONE = "Africa/Cairo";
const HISTORICAL_WORKER_URL = "https://odd-tree-f8e9.ahmed-kharoub1995.workers.dev";

const HEARTBEAT_MS = 10_000;
const RECONNECT_MS = 5_000;

// نخزن آخر 180 شمعة دقيقة مؤقتة = 3 ساعات
const MAX_STORED_CANDLES = 180;

const REST_INTERVALS = [
	"1min",
	"5min",
	"15min",
	"30min",
	"1h",
	"4h",
	"1day",
	"1week",
	"1month",
] as const;

type RestInterval = (typeof REST_INTERVALS)[number];

type StoredHistoricalCandle = {
	timeframe: RestInterval;
	datetime: string;
	open: number;
	high: number;
	low: number;
	close: number;
	source: "historical_rest";
	provisional: false;
	confirmed: true;
	stored_at_ms: number;
};

type HistoricalMeta = {
	timeframe: RestInterval;
	last_sync_ms: number;
	last_sync_time: string;
	latest_datetime: string | null;
	oldest_datetime: string | null;
	last_request_count: number;
	last_requested_outputsize: number;
	next_before: string | null;
	source: "historical_rest";
};

type HistoricalWorkerPayload = {
	status?: string;
	symbol?: string;
	interval?: string;
	count?: number;
	requested_size?: number;
	newest?: string;
	oldest?: string;
	next_before?: string | null;
	columns?: string[];
	candles?: unknown[][];
	error?: unknown;
};

type LiveEnv = {
	TWELVEDATA_API_KEY: string;
	Chat: DurableObjectNamespace;
};

type ProvisionalCandle = {
	start_ms: number;
	datetime: string;

	open: number;
	high: number;
	low: number;
	close: number;

	tick_count: number;

	first_tick_ms: number;
	last_tick_ms: number;

	source: "websocket_ticks";
	provisional: true;
	confirmed: false;
};

function json(data: unknown, status = 200) {
	return new Response(JSON.stringify(data), {
		status,
		headers: {
			"Content-Type": "application/json; charset=utf-8",
			"Access-Control-Allow-Origin": "*",
			"Cache-Control": "no-store",
		},
	});
}

function normalizeTimestamp(value: unknown) {
	const n = Number(value);

	if (!Number.isFinite(n)) {
		return Date.now();
	}

	// لو Twelve Data رجعت Unix seconds نحولها ms
	return n < 1_000_000_000_000 ? n * 1000 : n;
}

function minuteStart(ms: number) {
	return Math.floor(ms / 60_000) * 60_000;
}

function cairoTime(ms: number) {
	const parts = new Intl.DateTimeFormat("en-CA", {
		timeZone: TIMEZONE,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hourCycle: "h23",
	}).formatToParts(new Date(ms));

	const get = (type: string) =>
		parts.find((p) => p.type === type)?.value ?? "";

	return (
		`${get("year")}-${get("month")}-${get("day")} ` +
		`${get("hour")}:${get("minute")}:${get("second")}`
	);
}

function isRestInterval(value: string): value is RestInterval {
	return (REST_INTERVALS as readonly string[]).includes(value);
}

function historicalCandleKey(interval: RestInterval, datetime: string) {
	return `hist:${interval}:${datetime}`;
}

function historicalPrefix(interval: RestInterval) {
	return `hist:${interval}:`;
}

function historicalMetaKey(interval: RestInterval) {
	return `meta:${interval}`;
}

function parseFiniteNumber(value: unknown) {
	const n = Number(value);
	return Number.isFinite(n) ? n : null;
}

export class Chat extends DurableObject<LiveEnv> {
	private ws: WebSocket | null = null;
	private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

	private enabled = true;
	private connectionStatus = "starting";

	private lastPrice: number | null = null;
	private lastTickMs: number | null = null;

	private tickCount = 0;
	private reconnectCount = 0;

	private currentCandle: ProvisionalCandle | null = null;
	private candles: ProvisionalCandle[] = [];

	private lastError: string | null = null;
	private subscribeStatus: unknown = null;

	constructor(ctx: DurableObjectState, env: LiveEnv) {
		super(ctx, env);

		ctx.blockConcurrencyWhile(async () => {
			const saved = await ctx.storage.get<{
				enabled?: boolean;
				lastPrice?: number | null;
				lastTickMs?: number | null;
				tickCount?: number;
				reconnectCount?: number;
				currentCandle?: ProvisionalCandle | null;
				candles?: ProvisionalCandle[];
				lastError?: string | null;
				subscribeStatus?: unknown;
			}>("live_state");

			if (saved) {
				this.enabled = saved.enabled ?? true;
				this.lastPrice = saved.lastPrice ?? null;
				this.lastTickMs = saved.lastTickMs ?? null;
				this.tickCount = saved.tickCount ?? 0;
				this.reconnectCount = saved.reconnectCount ?? 0;
				this.currentCandle = saved.currentCandle ?? null;
				this.candles = saved.candles ?? [];
				this.lastError = saved.lastError ?? null;
				this.subscribeStatus = saved.subscribeStatus ?? null;
			}
		});
	}

	async fetch(request: Request) {
		const url = new URL(request.url);

		if (url.pathname === "/start") {
			this.enabled = true;

			await this.persist();
			await this.ensureConnection();

			return json(this.getState());
		}

		if (url.pathname === "/stop") {
			this.enabled = false;

			this.stopReconnectTimer();
			this.closeSocket("manual stop");

			await this.persist();

			return json({
				status: "stopped",
			});
		}

		if (url.pathname === "/price") {
			await this.ensureConnection();

			return json({
				status: "ok",
				symbol: SYMBOL,

				connection_status: this.connectionStatus,

				price: this.lastPrice,

				tick_timestamp: this.lastTickMs,

				tick_time:
					this.lastTickMs !== null
						? cairoTime(this.lastTickMs)
						: null,

				age_seconds:
					this.lastTickMs !== null
						? Math.floor((Date.now() - this.lastTickMs) / 1000)
						: null,
			});
		}

		if (url.pathname === "/candles") {
			await this.ensureConnection();

			let limit = Number(url.searchParams.get("limit") ?? 10);

			if (!Number.isInteger(limit) || limit < 1) {
				limit = 10;
			}

			limit = Math.min(limit, MAX_STORED_CANDLES);

			return json({
				status: "ok",

				symbol: SYMBOL,
				timeframe: "1min",

				source: "websocket_ticks",
				confirmed: false,

				note:
					"Tick-built candles are provisional. REST historical candles are authoritative.",

				closed_candles: this.candles.slice(-limit),

				current_candle: this.currentCandle,

				last_price: this.lastPrice,

				last_tick_time:
					this.lastTickMs !== null
						? cairoTime(this.lastTickMs)
						: null,
			});
		}

		if (url.pathname === "/sync") {
			const requestedInterval =
				url.searchParams.get("interval") ?? "1h";

			let outputsize = Number(
				url.searchParams.get("outputsize") ?? 100,
			);

			if (!Number.isInteger(outputsize) || outputsize < 1) {
				outputsize = 100;
			}

			outputsize = Math.min(outputsize, 1150);

			if (requestedInterval === "all") {
				const results = [];

				for (const interval of REST_INTERVALS) {
					results.push(
						await this.syncHistoricalInterval(
							interval,
							outputsize,
							null,
						),
					);
				}

				return json({
					status: "ok",
					mode: "all",
					symbol: SYMBOL,
					timezone: TIMEZONE,
					note:
						"Persistent raw REST storage only. No FVG, liquidity, sweep, mitigation, or market-structure analysis is performed in the Worker.",
					results,
				});
			}

			if (!isRestInterval(requestedInterval)) {
				return json(
					{
						status: "error",
						error: "Unsupported interval",
						supported_intervals: REST_INTERVALS,
						note:
							"3min is intentionally not requested from Twelve Data. It will be derived later from valid 1min data.",
					},
					400,
				);
			}

			const before = url.searchParams.get("before");

			const result = await this.syncHistoricalInterval(
				requestedInterval,
				outputsize,
				before,
			);

			return json(result, result.status === "ok" ? 200 : 502);
		}

		if (url.pathname === "/data") {
			const interval =
				url.searchParams.get("interval") ?? "1h";

			if (!isRestInterval(interval)) {
				return json(
					{
						status: "error",
						error: "Unsupported interval",
						supported_intervals: REST_INTERVALS,
					},
					400,
				);
			}

			let limit = Number(url.searchParams.get("limit") ?? 100);

			if (!Number.isInteger(limit) || limit < 1) {
				limit = 100;
			}

			limit = Math.min(limit, 1150);

			const rows = await this.ctx.storage.list<StoredHistoricalCandle>({
				prefix: historicalPrefix(interval),
				reverse: true,
				limit,
			});

			const candles = Array.from(rows.values()).sort((a, b) =>
				b.datetime.localeCompare(a.datetime),
			);

			const meta =
				(await this.ctx.storage.get<HistoricalMeta>(
					historicalMetaKey(interval),
				)) ?? null;

			return json({
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval,
				layer: "raw_rest_persistent",
				count: candles.length,
				meta,
				columns: [
					"datetime",
					"open",
					"high",
					"low",
					"close",
					"confirmed",
					"source",
				],
				candles: candles.map((c) => [
					c.datetime,
					c.open,
					c.high,
					c.low,
					c.close,
					c.confirmed,
					c.source,
				]),
			});
		}

		if (url.pathname === "/storage-state") {
			const frames = [];

			for (const interval of REST_INTERVALS) {
				const meta =
					(await this.ctx.storage.get<HistoricalMeta>(
						historicalMetaKey(interval),
					)) ?? null;

				const newest = await this.ctx.storage.list<StoredHistoricalCandle>({
					prefix: historicalPrefix(interval),
					reverse: true,
					limit: 1,
				});

				const oldest = await this.ctx.storage.list<StoredHistoricalCandle>({
					prefix: historicalPrefix(interval),
					limit: 1,
				});

				const newestCandle =
					Array.from(newest.values())[0] ?? null;

				const oldestCandle =
					Array.from(oldest.values())[0] ?? null;

				frames.push({
					interval,
					has_data: newestCandle !== null,
					newest_datetime: newestCandle?.datetime ?? null,
					oldest_datetime: oldestCandle?.datetime ?? null,
					meta,
				});
			}

			return json({
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				storage_layer: "raw_rest_persistent",
				note:
					"3min storage is not created yet. Session filtering, synthetic gap candles, and derived timeframe logic will be added in the next stage.",
				frames,
			});
		}

		if (url.pathname === "/historical") {
			const interval = url.searchParams.get("interval") ?? "1h";
			const before = url.searchParams.get("before");

			let outputsize = Number(
				url.searchParams.get("outputsize") ?? 10,
			);

			if (!Number.isInteger(outputsize) || outputsize < 1) {
				outputsize = 10;
			}

			outputsize = Math.min(outputsize, 1150);

			if (!HISTORICAL_WORKER_URL) {
				return json(
					{
						status: "error",
						error: "HISTORICAL_WORKER_URL is missing",
					},
					500,
				);
			}

			try {
				const upstream = new URL(
					HISTORICAL_WORKER_URL,
				);

				upstream.searchParams.set("symbol", SYMBOL);
				upstream.searchParams.set("interval", interval);
				upstream.searchParams.set(
					"outputsize",
					String(outputsize),
				);
				upstream.searchParams.set("timezone", TIMEZONE);

				if (before) {
					upstream.searchParams.set("before", before);
				}

				const response = await fetch(upstream.toString(), {
					headers: {
						Accept: "application/json",
					},
				});

				const body = await response.text();

				return new Response(body, {
					status: response.status,
					headers: {
						"Content-Type":
							response.headers.get("Content-Type") ??
							"application/json; charset=utf-8",
						"Access-Control-Allow-Origin": "*",
						"Cache-Control": "no-store",
					},
				});
			} catch (error) {
				return json(
					{
						status: "error",
						error:
							error instanceof Error
								? error.message
								: String(error),
					},
					502,
				);
			}
		}

		if (
			url.pathname === "/state" ||
			url.pathname === "/health"
		) {
			await this.ensureConnection();

			return json(this.getState());
		}

		return json({
			status: "ok",

			service: "XAU/USD Live Service",

			endpoints: {
				start: "/start",
				stop: "/stop",
				price: "/price",
				state: "/state",
				health: "/health",
				candles: "/candles?limit=10",
				historical:
					"/historical?interval=1h&outputsize=10",
				sync:
					"/sync?interval=1h&outputsize=100",
				sync_all:
					"/sync?interval=all&outputsize=100",
				data:
					"/data?interval=1h&limit=100",
				storage_state: "/storage-state",
			},

			data_policy: {
				live_ticks: "provisional",
				historical_rest: "authoritative",
				persistent_storage: "raw REST candles by timeframe",
				analysis:
					"ChatGPT only — Worker does not detect FVG, liquidity, sweeps, mitigation, or market structure",
			},

			historical_worker_configured:
				Boolean(HISTORICAL_WORKER_URL),
		});
	}

	private async ensureConnection() {
		if (!this.enabled) return;

		if (
			this.ws &&
			(
				this.ws.readyState === WebSocket.OPEN ||
				this.ws.readyState === WebSocket.CONNECTING
			)
		) {
			return;
		}

		await this.connect();
	}

	private async connect() {
		if (!this.enabled) return;

		if (!this.env.TWELVEDATA_API_KEY) {
			this.connectionStatus = "error";
			this.lastError = "TWELVEDATA_API_KEY is missing";

			await this.persist();
			return;
		}

		this.connectionStatus = "connecting";
		this.lastError = null;

		const endpoint =
			"wss://ws.twelvedata.com/v1/quotes/price" +
			"?apikey=" +
			encodeURIComponent(this.env.TWELVEDATA_API_KEY);

		try {
			const socket = new WebSocket(endpoint);

			this.ws = socket;

			socket.addEventListener("open", () => {
				if (this.ws !== socket) return;

				this.connectionStatus = "connected";

				socket.send(
					JSON.stringify({
						action: "subscribe",
						params: {
							symbols: SYMBOL,
						},
					}),
				);

				this.startHeartbeat(socket);

				this.ctx.waitUntil(this.persist());
			});

			socket.addEventListener("message", (event) => {
				if (this.ws !== socket) return;

				this.ctx.waitUntil(this.handleMessage(event.data));
			});

			socket.addEventListener("close", (event) => {
				if (this.ws === socket) {
					this.ws = null;
				}

				this.stopHeartbeat();

				this.connectionStatus = "disconnected";

				this.lastError =
					`WebSocket closed: ${event.code} ${event.reason || ""}`;

				if (this.enabled) {
					this.reconnectCount++;
					this.scheduleReconnect();
				}

				this.ctx.waitUntil(this.persist());
			});

			socket.addEventListener("error", () => {
				this.connectionStatus = "error";
				this.lastError = "WebSocket connection error";

				this.ctx.waitUntil(this.persist());
			});
		} catch (error) {
			this.connectionStatus = "error";
			this.lastError =
				error instanceof Error
					? error.message
					: String(error);

			this.scheduleReconnect();

			await this.persist();
		}
	}

	private async handleMessage(raw: string | ArrayBuffer) {
		let data: any;

		try {
			if (typeof raw === "string") {
				data = JSON.parse(raw);
			} else {
				data = JSON.parse(
					new TextDecoder().decode(raw),
				);
			}
		} catch {
			return;
		}

		const eventType =
			data.event ??
			data.type ??
			data.event_type;

		if (eventType === "subscribe-status") {
			this.subscribeStatus = data;

			if (
				data.status === "error" ||
				data.success === false
			) {
				this.lastError = JSON.stringify(data);
			}

			await this.persist();
			return;
		}

		if (
			data.status === "error" ||
			eventType === "error"
		) {
			this.lastError = JSON.stringify(data);

			await this.persist();
			return;
		}

		if (eventType !== "price") {
			return;
		}

		if (data.symbol && data.symbol !== SYMBOL) {
			return;
		}

		const price = Number(data.price);

		if (!Number.isFinite(price)) {
			return;
		}

		const tickMs =
			normalizeTimestamp(data.timestamp);

		this.processTick(price, tickMs);
	}

	private processTick(price: number, tickMs: number) {
		const bucket = minuteStart(tickMs);

		this.lastPrice = price;
		this.lastTickMs = tickMs;
		this.tickCount++;

		if (!this.currentCandle) {
			this.currentCandle =
				this.createCandle(bucket, price, tickMs);

			this.ctx.waitUntil(this.persist());
			return;
		}

		// نفس شمعة الدقيقة
		if (bucket === this.currentCandle.start_ms) {
			this.currentCandle.high =
				Math.max(this.currentCandle.high, price);

			this.currentCandle.low =
				Math.min(this.currentCandle.low, price);

			this.currentCandle.close = price;
			this.currentCandle.tick_count++;

			this.currentCandle.last_tick_ms = tickMs;

			return;
		}

		// Tick قديم خارج الترتيب
		if (bucket < this.currentCandle.start_ms) {
			return;
		}

		// قفل الشمعة المؤقتة القديمة
		this.candles.push({
			...this.currentCandle,
		});

		if (this.candles.length > MAX_STORED_CANDLES) {
			this.candles =
				this.candles.slice(-MAX_STORED_CANDLES);
		}

		this.currentCandle =
			this.createCandle(bucket, price, tickMs);

		this.ctx.waitUntil(this.persist());
	}

	private createCandle(
		startMs: number,
		price: number,
		tickMs: number,
	): ProvisionalCandle {
		return {
			start_ms: startMs,
			datetime: cairoTime(startMs),

			open: price,
			high: price,
			low: price,
			close: price,

			tick_count: 1,

			first_tick_ms: tickMs,
			last_tick_ms: tickMs,

			source: "websocket_ticks",

			provisional: true,
			confirmed: false,
		};
	}

	private startHeartbeat(socket: WebSocket) {
		this.stopHeartbeat();

		this.heartbeatTimer = setInterval(() => {
			if (
				this.ws === socket &&
				socket.readyState === WebSocket.OPEN
			) {
				try {
					socket.send(
						JSON.stringify({
							action: "heartbeat",
						}),
					);
				} catch {}
			}
		}, HEARTBEAT_MS);
	}

	private stopHeartbeat() {
		if (this.heartbeatTimer !== null) {
			clearInterval(this.heartbeatTimer);
			this.heartbeatTimer = null;
		}
	}

	private scheduleReconnect() {
		if (!this.enabled) return;

		this.stopReconnectTimer();

		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;

			this.ctx.waitUntil(this.ensureConnection());
		}, RECONNECT_MS);
	}

	private stopReconnectTimer() {
		if (this.reconnectTimer !== null) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
	}

	private closeSocket(reason: string) {
		this.stopHeartbeat();

		if (this.ws) {
			try {
				this.ws.close(1000, reason);
			} catch {}

			this.ws = null;
		}

		this.connectionStatus = "stopped";
	}

	private async syncHistoricalInterval(
		interval: RestInterval,
		outputsize: number,
		before: string | null,
	) {
		try {
			const upstream = new URL(HISTORICAL_WORKER_URL);

			upstream.searchParams.set("symbol", SYMBOL);
			upstream.searchParams.set("interval", interval);
			upstream.searchParams.set(
				"outputsize",
				String(outputsize),
			);
			upstream.searchParams.set("timezone", TIMEZONE);

			if (before) {
				upstream.searchParams.set("before", before);
			}

			const response = await fetch(upstream.toString(), {
				headers: {
					Accept: "application/json",
				},
			});

			const payload =
				(await response.json()) as HistoricalWorkerPayload;

			if (
				!response.ok ||
				payload.status !== "ok" ||
				!Array.isArray(payload.candles)
			) {
				return {
					status: "error",
					interval,
					http_status: response.status,
					error:
						payload.error ??
						"Historical Worker returned an invalid payload",
				};
			}

			const now = Date.now();
			const validRows: StoredHistoricalCandle[] = [];

			for (const row of payload.candles) {
				if (!Array.isArray(row) || row.length < 5) {
					continue;
				}

				const datetime = String(row[0] ?? "");
				const open = parseFiniteNumber(row[1]);
				const high = parseFiniteNumber(row[2]);
				const low = parseFiniteNumber(row[3]);
				const close = parseFiniteNumber(row[4]);

				if (
					!datetime ||
					open === null ||
					high === null ||
					low === null ||
					close === null
				) {
					continue;
				}

				validRows.push({
					timeframe: interval,
					datetime,
					open,
					high,
					low,
					close,
					source: "historical_rest",
					provisional: false,
					confirmed: true,
					stored_at_ms: now,
				});
			}

			for (let i = 0; i < validRows.length; i += 100) {
				const batch = validRows.slice(i, i + 100);
				const entries: Record<string, StoredHistoricalCandle> = {};

				for (const candle of batch) {
					entries[
						historicalCandleKey(
							interval,
							candle.datetime,
						)
					] = candle;
				}

				await this.ctx.storage.put(entries);
			}

			const existingMeta =
				(await this.ctx.storage.get<HistoricalMeta>(
					historicalMetaKey(interval),
				)) ?? null;

			const datetimes = validRows.map((c) => c.datetime).sort();

			const requestOldest =
				datetimes.length > 0 ? datetimes[0] : null;

			const requestNewest =
				datetimes.length > 0
					? datetimes[datetimes.length - 1]
					: null;

			const oldestDatetime = [
				existingMeta?.oldest_datetime ?? null,
				requestOldest,
			]
				.filter((v): v is string => v !== null)
				.sort()[0] ?? null;

			const newestCandidates = [
				existingMeta?.latest_datetime ?? null,
				requestNewest,
			].filter((v): v is string => v !== null);

			const latestDatetime =
				newestCandidates.sort().reverse()[0] ?? null;

			const meta: HistoricalMeta = {
				timeframe: interval,
				last_sync_ms: now,
				last_sync_time: cairoTime(now),
				latest_datetime: latestDatetime,
				oldest_datetime: oldestDatetime,
				last_request_count: validRows.length,
				last_requested_outputsize: outputsize,
				next_before:
					typeof payload.next_before === "string"
						? payload.next_before
						: requestOldest,
				source: "historical_rest",
			};

			await this.ctx.storage.put(
				historicalMetaKey(interval),
				meta,
			);

			return {
				status: "ok",
				symbol: SYMBOL,
				interval,
				requested_outputsize: outputsize,
				rows_received: payload.candles.length,
				rows_validated_and_stored: validRows.length,
				request_newest: requestNewest,
				request_oldest: requestOldest,
				stored_latest_datetime: latestDatetime,
				stored_oldest_datetime: oldestDatetime,
				next_before: meta.next_before,
				dedupe_key: "interval + datetime",
				storage_layer: "raw_rest_persistent",
				analysis_performed: false,
			};
		} catch (error) {
			return {
				status: "error",
				interval,
				error:
					error instanceof Error
						? error.message
						: String(error),
			};
		}
	}

	private getState() {
		return {
			status: "ok",

			symbol: SYMBOL,
			timezone: TIMEZONE,

			enabled: this.enabled,

			connection_status:
				this.connectionStatus,

			websocket_open:
				Boolean(
					this.ws &&
					this.ws.readyState === WebSocket.OPEN,
				),

			last_price: this.lastPrice,

			last_tick_timestamp:
				this.lastTickMs,

			last_tick_time:
				this.lastTickMs !== null
					? cairoTime(this.lastTickMs)
					: null,

			last_tick_age_seconds:
				this.lastTickMs !== null
					? Math.floor(
							(Date.now() - this.lastTickMs) / 1000,
						)
					: null,

			ticks_received:
				this.tickCount,

			reconnect_count:
				this.reconnectCount,

			stored_closed_1m_candles:
				this.candles.length,

			current_1m_candle:
				this.currentCandle,

			recent_1m_candles:
				this.candles.slice(-5),

			subscribe_status:
				this.subscribeStatus,

			last_error:
				this.lastError,

			data_policy: {
				websocket:
					"PROVISIONAL",

				historical_rest:
					"AUTHORITATIVE",

				rule:
					"If REST historical data differs from tick-built candles, REST replaces the provisional data.",
			},
		};
	}

	private async persist() {
		await this.ctx.storage.put("live_state", {
			enabled:
				this.enabled,

			lastPrice:
				this.lastPrice,

			lastTickMs:
				this.lastTickMs,

			tickCount:
				this.tickCount,

			reconnectCount:
				this.reconnectCount,

			currentCandle:
				this.currentCandle,

			candles:
				this.candles,

			lastError:
				this.lastError,

			subscribeStatus:
				this.subscribeStatus,
		});
	}
}


// Public Worker
export default {
	async fetch(
		request: Request,
		env: LiveEnv,
	) {
		const id =
			env.Chat.idFromName("XAUUSD");

		const stub =
			env.Chat.get(id);

		return stub.fetch(request);
	},
};function cairoTime(ms: number) {
	const parts = new Intl.DateTimeFormat("en-CA", {
		timeZone: TIMEZONE,
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hourCycle: "h23",
	}).formatToParts(new Date(ms));

	const get = (type: string) =>
		parts.find((p) => p.type === type)?.value ?? "";

	return (
		`${get("year")}-${get("month")}-${get("day")} ` +
		`${get("hour")}:${get("minute")}:${get("second")}`
	);
}

export class Chat extends DurableObject<LiveEnv> {
	private ws: WebSocket | null = null;
	private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

	private enabled = true;
	private connectionStatus = "starting";

	private lastPrice: number | null = null;
	private lastTickMs: number | null = null;

	private tickCount = 0;
	private reconnectCount = 0;

	private currentCandle: ProvisionalCandle | null = null;
	private candles: ProvisionalCandle[] = [];

	private lastError: string | null = null;
	private subscribeStatus: unknown = null;

	constructor(ctx: DurableObjectState, env: LiveEnv) {
		super(ctx, env);

		ctx.blockConcurrencyWhile(async () => {
			const saved = await ctx.storage.get<{
				enabled?: boolean;
				lastPrice?: number | null;
				lastTickMs?: number | null;
				tickCount?: number;
				reconnectCount?: number;
				currentCandle?: ProvisionalCandle | null;
				candles?: ProvisionalCandle[];
				lastError?: string | null;
				subscribeStatus?: unknown;
			}>("live_state");

			if (saved) {
				this.enabled = saved.enabled ?? true;
				this.lastPrice = saved.lastPrice ?? null;
				this.lastTickMs = saved.lastTickMs ?? null;
				this.tickCount = saved.tickCount ?? 0;
				this.reconnectCount = saved.reconnectCount ?? 0;
				this.currentCandle = saved.currentCandle ?? null;
				this.candles = saved.candles ?? [];
				this.lastError = saved.lastError ?? null;
				this.subscribeStatus = saved.subscribeStatus ?? null;
			}
		});
	}

	async fetch(request: Request) {
		const url = new URL(request.url);

		if (url.pathname === "/start") {
			this.enabled = true;

			await this.persist();
			await this.ensureConnection();

			return json(this.getState());
		}

		if (url.pathname === "/stop") {
			this.enabled = false;

			this.stopReconnectTimer();
			this.closeSocket("manual stop");

			await this.persist();

			return json({
				status: "stopped",
			});
		}

		if (url.pathname === "/price") {
			await this.ensureConnection();

			return json({
				status: "ok",
				symbol: SYMBOL,

				connection_status: this.connectionStatus,

				price: this.lastPrice,

				tick_timestamp: this.lastTickMs,

				tick_time:
					this.lastTickMs !== null
						? cairoTime(this.lastTickMs)
						: null,

				age_seconds:
					this.lastTickMs !== null
						? Math.floor((Date.now() - this.lastTickMs) / 1000)
						: null,
			});
		}

		if (url.pathname === "/candles") {
			await this.ensureConnection();

			let limit = Number(url.searchParams.get("limit") ?? 10);

			if (!Number.isInteger(limit) || limit < 1) {
				limit = 10;
			}

			limit = Math.min(limit, MAX_STORED_CANDLES);

			return json({
				status: "ok",

				symbol: SYMBOL,
				timeframe: "1min",

				source: "websocket_ticks",
				confirmed: false,

				note:
					"Tick-built candles are provisional. REST historical candles are authoritative.",

				closed_candles: this.candles.slice(-limit),

				current_candle: this.currentCandle,

				last_price: this.lastPrice,

				last_tick_time:
					this.lastTickMs !== null
						? cairoTime(this.lastTickMs)
						: null,
			});
		}

		if (url.pathname === "/historical") {
			const interval = url.searchParams.get("interval") ?? "1h";
			const before = url.searchParams.get("before");

			let outputsize = Number(
				url.searchParams.get("outputsize") ?? 10,
			);

			if (!Number.isInteger(outputsize) || outputsize < 1) {
				outputsize = 10;
			}

			outputsize = Math.min(outputsize, 1150);

			if (!HISTORICAL_WORKER_URL) {
				return json(
					{
						status: "error",
						error: "HISTORICAL_WORKER_URL is missing",
					},
					500,
				);
			}

			try {
				const upstream = new URL(
					HISTORICAL_WORKER_URL,
				);

				upstream.searchParams.set("symbol", SYMBOL);
				upstream.searchParams.set("interval", interval);
				upstream.searchParams.set(
					"outputsize",
					String(outputsize),
				);
				upstream.searchParams.set("timezone", TIMEZONE);

				if (before) {
					upstream.searchParams.set("before", before);
				}

				const response = await fetch(upstream.toString(), {
					headers: {
						Accept: "application/json",
					},
				});

				const body = await response.text();

				return new Response(body, {
					status: response.status,
					headers: {
						"Content-Type":
							response.headers.get("Content-Type") ??
							"application/json; charset=utf-8",
						"Access-Control-Allow-Origin": "*",
						"Cache-Control": "no-store",
					},
				});
			} catch (error) {
				return json(
					{
						status: "error",
						error:
							error instanceof Error
								? error.message
								: String(error),
					},
					502,
				);
			}
		}

		if (
			url.pathname === "/state" ||
			url.pathname === "/health"
		) {
			await this.ensureConnection();

			return json(this.getState());
		}

		return json({
			status: "ok",

			service: "XAU/USD Live Service",

			endpoints: {
				start: "/start",
				stop: "/stop",
				price: "/price",
				state: "/state",
				health: "/health",
				candles: "/candles?limit=10",
				historical:
					"/historical?interval=1h&outputsize=10",
			},

			data_policy: {
				live_ticks: "provisional",
				historical_rest: "authoritative",
			},

			historical_worker_configured:
				Boolean(HISTORICAL_WORKER_URL),
		});
	}

	private async ensureConnection() {
		if (!this.enabled) return;

		if (
			this.ws &&
			(
				this.ws.readyState === WebSocket.OPEN ||
				this.ws.readyState === WebSocket.CONNECTING
			)
		) {
			return;
		}

		await this.connect();
	}

	private async connect() {
		if (!this.enabled) return;

		if (!this.env.TWELVEDATA_API_KEY) {
			this.connectionStatus = "error";
			this.lastError = "TWELVEDATA_API_KEY is missing";

			await this.persist();
			return;
		}

		this.connectionStatus = "connecting";
		this.lastError = null;

		const endpoint =
			"wss://ws.twelvedata.com/v1/quotes/price" +
			"?apikey=" +
			encodeURIComponent(this.env.TWELVEDATA_API_KEY);

		try {
			const socket = new WebSocket(endpoint);

			this.ws = socket;

			socket.addEventListener("open", () => {
				if (this.ws !== socket) return;

				this.connectionStatus = "connected";

				socket.send(
					JSON.stringify({
						action: "subscribe",
						params: {
							symbols: SYMBOL,
						},
					}),
				);

				this.startHeartbeat(socket);

				this.ctx.waitUntil(this.persist());
			});

			socket.addEventListener("message", (event) => {
				if (this.ws !== socket) return;

				this.ctx.waitUntil(this.handleMessage(event.data));
			});

			socket.addEventListener("close", (event) => {
				if (this.ws === socket) {
					this.ws = null;
				}

				this.stopHeartbeat();

				this.connectionStatus = "disconnected";

				this.lastError =
					`WebSocket closed: ${event.code} ${event.reason || ""}`;

				if (this.enabled) {
					this.reconnectCount++;
					this.scheduleReconnect();
				}

				this.ctx.waitUntil(this.persist());
			});

			socket.addEventListener("error", () => {
				this.connectionStatus = "error";
				this.lastError = "WebSocket connection error";

				this.ctx.waitUntil(this.persist());
			});
		} catch (error) {
			this.connectionStatus = "error";
			this.lastError =
				error instanceof Error
					? error.message
					: String(error);

			this.scheduleReconnect();

			await this.persist();
		}
	}

	private async handleMessage(raw: string | ArrayBuffer) {
		let data: any;

		try {
			if (typeof raw === "string") {
				data = JSON.parse(raw);
			} else {
				data = JSON.parse(
					new TextDecoder().decode(raw),
				);
			}
		} catch {
			return;
		}

		const eventType =
			data.event ??
			data.type ??
			data.event_type;

		if (eventType === "subscribe-status") {
			this.subscribeStatus = data;

			if (
				data.status === "error" ||
				data.success === false
			) {
				this.lastError = JSON.stringify(data);
			}

			await this.persist();
			return;
		}

		if (
			data.status === "error" ||
			eventType === "error"
		) {
			this.lastError = JSON.stringify(data);

			await this.persist();
			return;
		}

		if (eventType !== "price") {
			return;
		}

		if (data.symbol && data.symbol !== SYMBOL) {
			return;
		}

		const price = Number(data.price);

		if (!Number.isFinite(price)) {
			return;
		}

		const tickMs =
			normalizeTimestamp(data.timestamp);

		this.processTick(price, tickMs);
	}

	private processTick(price: number, tickMs: number) {
		const bucket = minuteStart(tickMs);

		this.lastPrice = price;
		this.lastTickMs = tickMs;
		this.tickCount++;

		if (!this.currentCandle) {
			this.currentCandle =
				this.createCandle(bucket, price, tickMs);

			this.ctx.waitUntil(this.persist());
			return;
		}

		// نفس شمعة الدقيقة
		if (bucket === this.currentCandle.start_ms) {
			this.currentCandle.high =
				Math.max(this.currentCandle.high, price);

			this.currentCandle.low =
				Math.min(this.currentCandle.low, price);

			this.currentCandle.close = price;
			this.currentCandle.tick_count++;

			this.currentCandle.last_tick_ms = tickMs;

			return;
		}

		// Tick قديم خارج الترتيب
		if (bucket < this.currentCandle.start_ms) {
			return;
		}

		// قفل الشمعة المؤقتة القديمة
		this.candles.push({
			...this.currentCandle,
		});

		if (this.candles.length > MAX_STORED_CANDLES) {
			this.candles =
				this.candles.slice(-MAX_STORED_CANDLES);
		}

		this.currentCandle =
			this.createCandle(bucket, price, tickMs);

		this.ctx.waitUntil(this.persist());
	}

	private createCandle(
		startMs: number,
		price: number,
		tickMs: number,
	): ProvisionalCandle {
		return {
			start_ms: startMs,
			datetime: cairoTime(startMs),

			open: price,
			high: price,
			low: price,
			close: price,

			tick_count: 1,

			first_tick_ms: tickMs,
			last_tick_ms: tickMs,

			source: "websocket_ticks",

			provisional: true,
			confirmed: false,
		};
	}

	private startHeartbeat(socket: WebSocket) {
		this.stopHeartbeat();

		this.heartbeatTimer = setInterval(() => {
			if (
				this.ws === socket &&
				socket.readyState === WebSocket.OPEN
			) {
				try {
					socket.send(
						JSON.stringify({
							action: "heartbeat",
						}),
					);
				} catch {}
			}
		}, HEARTBEAT_MS);
	}

	private stopHeartbeat() {
		if (this.heartbeatTimer !== null) {
			clearInterval(this.heartbeatTimer);
			this.heartbeatTimer = null;
		}
	}

	private scheduleReconnect() {
		if (!this.enabled) return;

		this.stopReconnectTimer();

		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;

			this.ctx.waitUntil(this.ensureConnection());
		}, RECONNECT_MS);
	}

	private stopReconnectTimer() {
		if (this.reconnectTimer !== null) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
	}

	private closeSocket(reason: string) {
		this.stopHeartbeat();

		if (this.ws) {
			try {
				this.ws.close(1000, reason);
			} catch {}

			this.ws = null;
		}

		this.connectionStatus = "stopped";
	}

	private getState() {
		return {
			status: "ok",

			symbol: SYMBOL,
			timezone: TIMEZONE,

			enabled: this.enabled,

			connection_status:
				this.connectionStatus,

			websocket_open:
				Boolean(
					this.ws &&
					this.ws.readyState === WebSocket.OPEN,
				),

			last_price: this.lastPrice,

			last_tick_timestamp:
				this.lastTickMs,

			last_tick_time:
				this.lastTickMs !== null
					? cairoTime(this.lastTickMs)
					: null,

			last_tick_age_seconds:
				this.lastTickMs !== null
					? Math.floor(
							(Date.now() - this.lastTickMs) / 1000,
						)
					: null,

			ticks_received:
				this.tickCount,

			reconnect_count:
				this.reconnectCount,

			stored_closed_1m_candles:
				this.candles.length,

			current_1m_candle:
				this.currentCandle,

			recent_1m_candles:
				this.candles.slice(-5),

			subscribe_status:
				this.subscribeStatus,

			last_error:
				this.lastError,

			data_policy: {
				websocket:
					"PROVISIONAL",

				historical_rest:
					"AUTHORITATIVE",

				rule:
					"If REST historical data differs from tick-built candles, REST replaces the provisional data.",
			},
		};
	}

	private async persist() {
		await this.ctx.storage.put("live_state", {
			enabled:
				this.enabled,

			lastPrice:
				this.lastPrice,

			lastTickMs:
				this.lastTickMs,

			tickCount:
				this.tickCount,

			reconnectCount:
				this.reconnectCount,

			currentCandle:
				this.currentCandle,

			candles:
				this.candles,

			lastError:
				this.lastError,

			subscribeStatus:
				this.subscribeStatus,
		});
	}
}


// Public Worker
export default {
	async fetch(
		request: Request,
		env: LiveEnv,
	) {
		const id =
			env.Chat.idFromName("XAUUSD");

		const stub =
			env.Chat.get(id);

		return stub.fetch(request);
	},
};
