import { DurableObject } from "cloudflare:workers";

const SYMBOL = "XAU/USD";
const TIMEZONE = "Africa/Cairo";
const TWELVE_DATA_REST_URL = "https://api.twelvedata.com/time_series";

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

const NORMALIZED_REST_INTERVALS = [
	"1min",
	"5min",
	"15min",
	"30min",
	"1h",
] as const;

type NormalizedRestInterval =
	(typeof NORMALIZED_REST_INTERVALS)[number];

const NORMALIZED_INTERVAL_MINUTES: Record<
	NormalizedRestInterval,
	number
> = {
	"1min": 1,
	"5min": 5,
	"15min": 15,
	"30min": 30,
	"1h": 60,
};

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

type NormalizedCandle = {
	timeframe: NormalizedRestInterval;
	datetime: string;
	open_time: string;
	expected_close_time: string;
	open: number;
	high: number;
	low: number;
	close: number;
	status: "OPEN" | "CLOSED";
	source: "historical_rest" | "synthetic_gap";
	provisional: false;
	confirmed: true;
	synthetic_gap: boolean;
	stored_at_ms: number;
};

type NormalizedMeta = {
	timeframe: NormalizedRestInterval;
	last_normalized_ms: number;
	last_normalized_time: string;
	latest_datetime: string | null;
	oldest_datetime: string | null;
	raw_rows_seen: number;
	valid_raw_rows: number;
	filtered_closed_rows: number;
	synthetic_gap_rows: number;
	pending_closed_period: boolean;
	layer: "analysis_normalized";
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

type TwelveDataValue = {
	datetime?: string;
	open?: string | number;
	high?: string | number;
	low?: string | number;
	close?: string | number;
};

type TwelveDataResponse = {
	status?: string;
	code?: number;
	message?: string;
	meta?: {
		symbol?: string;
		interval?: string;
		timezone?: string;
	};
	values?: TwelveDataValue[];
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

function isNormalizedRestInterval(
	value: string,
): value is NormalizedRestInterval {
	return (
		NORMALIZED_REST_INTERVALS as readonly string[]
	).includes(value);
}

function normalizedCandleKey(
	interval: NormalizedRestInterval,
	datetime: string,
) {
	return `norm:${interval}:${datetime}`;
}

function normalizedPrefix(
	interval: NormalizedRestInterval,
) {
	return `norm:${interval}:`;
}

function normalizedMetaKey(
	interval: NormalizedRestInterval,
) {
	return `normmeta:${interval}`;
}

function normalizedIntervalMinutes(
	interval: NormalizedRestInterval,
) {
	return NORMALIZED_INTERVAL_MINUTES[interval];
}


function parseCairoDatetimeParts(datetime: string) {
	const match = datetime.match(
		/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/,
	);

	if (!match) return null;

	const [, y, mo, d, h, mi, s] = match;

	return {
		year: Number(y),
		month: Number(mo),
		day: Number(d),
		hour: Number(h),
		minute: Number(mi),
		second: Number(s),
	};
}

function cairoDatetimeToMs(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return null;

	// Project rule: Cairo is treated as fixed GMT+3.
	return (
		Date.UTC(
			parts.year,
			parts.month - 1,
			parts.day,
			parts.hour,
			parts.minute,
			parts.second,
		) -
		3 * 60 * 60 * 1000
	);
}

function addMinutesToCairoDatetime(
	datetime: string,
	minutes: number,
) {
	const ms = cairoDatetimeToMs(datetime);
	if (ms === null) return datetime;
	return cairoTime(ms + minutes * 60_000);
}

function cairoDayOfWeek(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return null;

	return new Date(
		Date.UTC(parts.year, parts.month - 1, parts.day),
	).getUTCDay();
}

function isClosedMarketCairoDatetime(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	const day = cairoDayOfWeek(datetime);

	if (!parts || day === null) return false;

	// Weekend closure: Saturday 00:00 -> Monday 01:00 Cairo.
	if (day === 6 || day === 0) return true;
	if (day === 1 && parts.hour < 1) return true;

	// Daily closure: 00:00 -> 01:00 Cairo.
	return parts.hour < 1;
}

function statusFromExpectedClose(expectedCloseTime: string) {
	const closeMs = cairoDatetimeToMs(expectedCloseTime);

	if (closeMs === null) {
		return "CLOSED" as const;
	}

	return Date.now() >= closeMs
		? ("CLOSED" as const)
		: ("OPEN" as const);
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

		if (url.pathname === "/normalize") {
			const requestedInterval =
				url.searchParams.get("interval") ?? "1h";

			if (requestedInterval === "all") {
				const results = [];

				for (const interval of NORMALIZED_REST_INTERVALS) {
					results.push(
						await this.normalizeStoredInterval(interval),
					);
				}

				return json({
					status: "ok",
					mode: "all",
					symbol: SYMBOL,
					timezone: TIMEZONE,
					layer: "analysis_normalized",
					results,
					analysis_performed: false,
				});
			}

			if (!isNormalizedRestInterval(requestedInterval)) {
				return json(
					{
						status: "error",
						error:
							"Normalization currently supports 1min, 5min, 15min, 30min, and 1h only.",
						supported_intervals:
							NORMALIZED_REST_INTERVALS,
						note:
							"4h and higher are intentionally deferred until native candle boundary behavior is validated.",
					},
					400,
				);
			}

			const result =
				await this.normalizeStoredInterval(requestedInterval);

			return json(
				result,
				result.status === "ok" ? 200 : 500,
			);
		}

		if (url.pathname === "/normalized-data") {
			const interval =
				url.searchParams.get("interval") ?? "1h";

			if (!isNormalizedRestInterval(interval)) {
				return json(
					{
						status: "error",
						error:
							"Normalized storage currently supports 1min, 5min, 15min, 30min, and 1h only.",
						supported_intervals:
							NORMALIZED_REST_INTERVALS,
					},
					400,
				);
			}

			let limit = Number(url.searchParams.get("limit") ?? 100);

			if (!Number.isInteger(limit) || limit < 1) {
				limit = 100;
			}

			limit = Math.min(limit, 1000);

			const rows =
				await this.ctx.storage.list<NormalizedCandle>({
					prefix: normalizedPrefix(interval),
					reverse: true,
					limit,
				});

			const candles = Array.from(rows.values()).sort((a, b) =>
				b.datetime.localeCompare(a.datetime),
			);

			const meta =
				(await this.ctx.storage.get<NormalizedMeta>(
					normalizedMetaKey(interval),
				)) ?? null;

			return json({
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval,
				layer: "analysis_normalized",
				count: candles.length,
				meta,
				columns: [
					"datetime",
					"open_time",
					"expected_close_time",
					"open",
					"high",
					"low",
					"close",
					"status",
					"source",
					"synthetic_gap",
				],
				candles: candles.map((c) => [
					c.datetime,
					c.open_time,
					c.expected_close_time,
					c.open,
					c.high,
					c.low,
					c.close,
					statusFromExpectedClose(
						c.expected_close_time,
					),
					c.source,
					c.synthetic_gap,
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

		if (url.pathname === "/purge") {
			const intervalParam = url.searchParams.get("interval");
			const confirm = url.searchParams.get("confirm");

			if (!intervalParam || !isRestInterval(intervalParam)) {
				return json(
					{
						status: "error",
						error: "A valid interval is required",
						allowed_intervals: REST_INTERVALS,
					},
					400,
				);
			}

			if (confirm !== "yes") {
				return json(
					{
						status: "error",
						error: "Purge requires confirm=yes",
					},
					400,
				);
			}

			const result = await this.purgeHistoricalInterval(
				intervalParam,
			);

			return json(
				result,
				result.status === "ok" ? 200 : 500,
			);
		}

		if (url.pathname === "/historical") {
			const intervalParam =
				url.searchParams.get("interval") ?? "1h";

			if (!isRestInterval(intervalParam)) {
				return json(
					{
						status: "error",
						error: "Unsupported interval",
						allowed_intervals: REST_INTERVALS,
					},
					400,
				);
			}

			const before = url.searchParams.get("before");

			let outputsize = Number(
				url.searchParams.get("outputsize") ?? 10,
			);

			if (!Number.isInteger(outputsize) || outputsize < 1) {
				outputsize = 10;
			}

			outputsize = Math.min(outputsize, 1150);

			const result = await this.fetchHistoricalDirect(
				intervalParam,
				outputsize,
				before,
			);

			return json(
				result,
				result.status === "ok" ? 200 : 502,
			);
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
 				normalize:
					"/normalize?interval=1min",
				normalize_all_supported:
					"/normalize?interval=all",
				normalized_data:
					"/normalized-data?interval=1min&limit=100",
				storage_state: "/storage-state",
				purge:
					"/purge?interval=1min&confirm=yes",
			},

			data_policy: {
				live_ticks: "provisional",
				historical_rest: "authoritative",
				persistent_storage: "raw REST candles by timeframe",
				normalized_storage:
					"session-filtered candles + synthetic gap candles; no market-analysis logic",
				analysis:
					"ChatGPT only — Worker does not detect FVG, liquidity, sweeps, mitigation, or market structure",
			},

			historical_source:
				"direct_twelve_data_rest",
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

	private async fetchHistoricalDirect(
		interval: RestInterval,
		outputsize: number,
		before: string | null,
	): Promise<HistoricalWorkerPayload> {
		if (!this.env.TWELVEDATA_API_KEY) {
			return {
				status: "error",
				interval,
				error: "TWELVEDATA_API_KEY is missing",
			};
		}

		try {
			const upstream = new URL(TWELVE_DATA_REST_URL);

			upstream.searchParams.set("symbol", SYMBOL);
			upstream.searchParams.set("interval", interval);
			upstream.searchParams.set(
				"outputsize",
				String(outputsize),
			);
			upstream.searchParams.set("timezone", TIMEZONE);
			upstream.searchParams.set(
				"apikey",
				this.env.TWELVEDATA_API_KEY,
			);

			if (before) {
				upstream.searchParams.set("end_date", before);
			}

			const response = await fetch(upstream.toString(), {
				headers: {
					Accept: "application/json",
				},
			});

			const payload =
				(await response.json()) as TwelveDataResponse;

			if (
				!response.ok ||
				payload.status === "error" ||
				!Array.isArray(payload.values)
			) {
				return {
					status: "error",
					interval,
					error:
						payload.message ??
						"Twelve Data returned an invalid payload",
				};
			}

			const returnedInterval = payload.meta?.interval;

			if (returnedInterval !== interval) {
				return {
					status: "error",
					interval,
					requested_size: outputsize,
					error:
						`Interval mismatch: requested ${interval}, returned ${returnedInterval ?? "unknown"}`,
				};
			}

			const candles: unknown[][] = payload.values
				.map((row) => [
					row.datetime ?? "",
					row.open ?? "",
					row.high ?? "",
					row.low ?? "",
					row.close ?? "",
				])
				.filter((row) => Boolean(row[0]));

			const datetimes = candles
				.map((row) => String(row[0]))
				.sort();

			const oldest =
				datetimes.length > 0 ? datetimes[0] : undefined;
			const newest =
				datetimes.length > 0
					? datetimes[datetimes.length - 1]
					: undefined;

			return {
				status: "ok",
				symbol: payload.meta?.symbol ?? SYMBOL,
				interval,
				count: candles.length,
				requested_size: outputsize,
				newest,
				oldest,
				next_before: oldest ?? null,
				columns: [
					"datetime",
					"open",
					"high",
					"low",
					"close",
				],
				candles,
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

	private async purgeHistoricalInterval(
		interval: RestInterval,
	) {
		try {
			let deleted = 0;

			while (true) {
				const page =
					await this.ctx.storage.list<StoredHistoricalCandle>({
						prefix: historicalPrefix(interval),
						limit: 500,
					});

				if (page.size === 0) {
					break;
				}

				const keys = Array.from(page.keys());

				for (let i = 0; i < keys.length; i += 100) {
					const batch = keys.slice(i, i + 100);
					await this.ctx.storage.delete(batch);
					deleted += batch.length;
				}
			}

			await this.ctx.storage.delete(
				historicalMetaKey(interval),
			);

			let deletedNormalized = 0;

			if (isNormalizedRestInterval(interval)) {
				while (true) {
					const normalizedPage =
						await this.ctx.storage.list<NormalizedCandle>({
							prefix: normalizedPrefix(interval),
							limit: 500,
						});

					if (normalizedPage.size === 0) {
						break;
					}

					const normalizedKeys =
						Array.from(normalizedPage.keys());

					for (
						let i = 0;
						i < normalizedKeys.length;
						i += 100
					) {
						const batch =
							normalizedKeys.slice(i, i + 100);
						await this.ctx.storage.delete(batch);
						deletedNormalized += batch.length;
					}

					await this.ctx.storage.delete(
						normalizedMetaKey(interval),
					);
				}
			}

			return {
				status: "ok",
				interval,
				deleted_candles: deleted,
				deleted_normalized_candles: deletedNormalized,
				meta_deleted: true,
				storage_layer: "raw_rest_persistent",
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

	private async syncHistoricalInterval(
		interval: RestInterval,
		outputsize: number,
		before: string | null,
	) {
		try {
			const payload = await this.fetchHistoricalDirect(
				interval,
				outputsize,
				before,
			);

			if (
				payload.status !== "ok" ||
				!Array.isArray(payload.candles)
			) {
				return {
					status: "error",
					interval,
					error:
						payload.error ??
						"Direct Twelve Data request failed",
				};
			}

			const now = Date.now();
			const validRows: StoredHistoricalCandle[] = [];

			const rowsToStore = payload.candles.slice(0, outputsize);

			for (const row of rowsToStore) {
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
				rows_considered_after_requested_limit: rowsToStore.length,
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

	private async normalizeStoredInterval(
		interval: NormalizedRestInterval,
	) {
		try {
			const raw: StoredHistoricalCandle[] = [];
			let startAfter: string | undefined;

			while (true) {
				const page =
					await this.ctx.storage.list<StoredHistoricalCandle>({
						prefix: historicalPrefix(interval),
						limit: 1000,
						...(startAfter ? { startAfter } : {}),
					});

				if (page.size === 0) {
					break;
				}

				raw.push(...page.values());

				if (page.size < 1000) {
					break;
				}

				const keys = Array.from(page.keys()) as string[];
				startAfter = keys[keys.length - 1];
			}

			raw.sort((a, b) =>
				a.datetime.localeCompare(b.datetime),
			);

			if (raw.length === 0) {
				return {
					status: "error",
					interval,
					error:
						`No stored ${interval} raw REST candles found`,
				};
			}

			const durationMinutes =
				normalizedIntervalMinutes(interval);

			const normalized: NormalizedCandle[] = [];
			let filteredClosedRows = 0;
			let syntheticGapRows = 0;
			let pendingClosedPeriod = false;

			let lastValid: StoredHistoricalCandle | null = null;
			let gapOpen: {
				start_datetime: string;
				previous_close: number;
			} | null = null;

			for (const candle of raw) {
				const closedMarket =
					isClosedMarketCairoDatetime(candle.datetime);

				if (closedMarket) {
					filteredClosedRows++;

					if (lastValid && !gapOpen) {
						gapOpen = {
							start_datetime:
								addMinutesToCairoDatetime(
									lastValid.datetime,
									durationMinutes,
								),
							previous_close: lastValid.close,
						};
					}

					continue;
				}

				if (gapOpen) {
					const gapClose = candle.open;
					const gapExpectedClose = candle.datetime;

					normalized.push({
						timeframe: interval,
						datetime: gapOpen.start_datetime,
						open_time: gapOpen.start_datetime,
						expected_close_time: gapExpectedClose,
						open: gapOpen.previous_close,
						high: Math.max(
							gapOpen.previous_close,
							gapClose,
						),
						low: Math.min(
							gapOpen.previous_close,
							gapClose,
						),
						close: gapClose,
						status:
							statusFromExpectedClose(
								gapExpectedClose,
							),
						source: "synthetic_gap",
						provisional: false,
						confirmed: true,
						synthetic_gap: true,
						stored_at_ms: Date.now(),
					});

					syntheticGapRows++;
					gapOpen = null;
				}

				const expectedCloseTime =
					addMinutesToCairoDatetime(
						candle.datetime,
						durationMinutes,
					);

				normalized.push({
					timeframe: interval,
					datetime: candle.datetime,
					open_time: candle.datetime,
					expected_close_time: expectedCloseTime,
					open: candle.open,
					high: candle.high,
					low: candle.low,
					close: candle.close,
					status:
						statusFromExpectedClose(
							expectedCloseTime,
						),
					source: "historical_rest",
					provisional: false,
					confirmed: true,
					synthetic_gap: false,
					stored_at_ms: Date.now(),
				});

				lastValid = candle;
			}

			if (gapOpen) {
				pendingClosedPeriod = true;
			}

			// Rebuild only this normalized timeframe.
			while (true) {
				const oldPage =
					await this.ctx.storage.list<NormalizedCandle>({
						prefix: normalizedPrefix(interval),
						limit: 500,
					});

				if (oldPage.size === 0) {
					break;
				}

				const keys = Array.from(oldPage.keys());

				for (let i = 0; i < keys.length; i += 100) {
					await this.ctx.storage.delete(
						keys.slice(i, i + 100),
					);
				}
			}

			for (let i = 0; i < normalized.length; i += 100) {
				const batch = normalized.slice(i, i + 100);
				const entries: Record<
					string,
					NormalizedCandle
				> = {};

				for (const candle of batch) {
					entries[
						normalizedCandleKey(
							interval,
							candle.datetime,
						)
					] = candle;
				}

				await this.ctx.storage.put(entries);
			}

			const datetimes = normalized
				.map((c) => c.datetime)
				.sort();

			const now = Date.now();

			const meta: NormalizedMeta = {
				timeframe: interval,
				last_normalized_ms: now,
				last_normalized_time: cairoTime(now),
				latest_datetime:
					datetimes.length > 0
						? datetimes[datetimes.length - 1]
						: null,
				oldest_datetime:
					datetimes.length > 0
						? datetimes[0]
						: null,
				raw_rows_seen: raw.length,
				valid_raw_rows:
					raw.length - filteredClosedRows,
				filtered_closed_rows: filteredClosedRows,
				synthetic_gap_rows: syntheticGapRows,
				pending_closed_period: pendingClosedPeriod,
				layer: "analysis_normalized",
			};

			await this.ctx.storage.put(
				normalizedMetaKey(interval),
				meta,
			);

			return {
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval,
				interval_minutes: durationMinutes,
				layer: "analysis_normalized",
				raw_rows_seen: raw.length,
				pagination_complete: true,
				valid_raw_rows: meta.valid_raw_rows,
				filtered_closed_rows: filteredClosedRows,
				synthetic_gap_rows: syntheticGapRows,
				pending_closed_period: pendingClosedPeriod,
				normalized_rows_stored: normalized.length,
				oldest_datetime: meta.oldest_datetime,
				latest_datetime: meta.latest_datetime,
				analysis_performed: false,
				note:
					"Worker only filters closed-market rows, inserts synthetic gap candles, and adds candle timing/status metadata. FVG, liquidity, sweeps, mitigation, and structure remain ChatGPT-only.",
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
};
