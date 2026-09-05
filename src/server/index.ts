import { DurableObject } from "cloudflare:workers";

const SYMBOL = "XAU/USD";
const TIMEZONE = "Africa/Cairo";

const HEARTBEAT_MS = 10_000;
const RECONNECT_MS = 5_000;

// نخزن آخر 180 شمعة دقيقة مؤقتة = 3 ساعات
const MAX_STORED_CANDLES = 180;

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
			},

			data_policy: {
				live_ticks: "provisional",
				historical_rest: "authoritative",
			},
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
