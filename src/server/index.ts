import { DurableObject } from "cloudflare:workers";

const SYMBOL = "XAU/USD";
const TIMEZONE = "Africa/Cairo";
const TWELVE_DATA_REST_URL = "https://api.twelvedata.com/time_series";
const BUILD_VERSION = "v14.6-quota-guard-controlled-recovery-2026-09-09";
const PROD_OBJECT_NAME = "XAUUSD_V14_6_PROD_20260909";
const LEGACY_OBJECT_NAME = "XAUUSD";
const RETIRED_OBJECT_NAMES = ["XAUUSD", "XAUUSD_V14_5_PROD_20260909"] as const;

// Recovery writes are intentionally budgeted far below Cloudflare's Free-tier
// daily row-write ceiling. Normal live/current storage is NOT paused by this
// budget; only bootstrap/backfill/recovery work is paused.
const RECOVERY_BUDGET_KEY = "recovery_write_budget_v1";
const RECOVERY_SOFT_LIMIT_ROWS = 25_000;
const RECOVERY_HARD_CEILING_ROWS = 60_000;
const RECOVERY_MAX_MANUAL_ADD_ROWS = 25_000;
const RECOVERY_COST_MULTIPLIER = 3;

const HEARTBEAT_MS = 10_000;
const RECONNECT_MS = 5_000;

// Keep enough provisional 1-minute candles to bridge every 4H refresh window.
const MAX_STORED_CANDLES = 480;

const AUTO_STATE_KEY = "auto_refresh_state";
const AUTO_ALARM_MS = 60_000;
const AUTO_RATE_WINDOW_MS = 60_000;
// Twelve Data Basic allows 8 API credits/minute. Keep one credit spare
// for a manual request and process the rest automatically on the next alarm.
const AUTO_MAX_REQUESTS_PER_WINDOW = 7;
const AUTO_INCREMENTAL_OUTPUTSIZE = 12;
const AUTO_BOOTSTRAP_OUTPUTSIZE = 1150;

// Self-healing scheduler / continuity recovery.
const AUTO_WATCHDOG_STALE_MS = 3 * 60_000;
const RECOVERY_BUFFER_ROWS = 40;
const RECOVERY_MAX_OUTPUTSIZE = 1150;
const CONTINUITY_SCAN_CONFIRMED_LIMIT = 1000;
const AUTO_RECOVERY_AUDIT_INTERVAL_MS = 5 * 60_000;
const RECOVERY_INTERVALS = [
	"1min",
	"5min",
	"15min",
	"30min",
	"1h",
] as const;
type RecoveryInterval = (typeof RECOVERY_INTERVALS)[number];

// One-time normalized-storage migration marker.
// v8 fixes Twelve Data date-only higher-timeframe timestamps and
// correct weekly close boundaries without refetching raw history.
const STORAGE_MIGRATION_KEY = "storage_migration_version";
const STORAGE_MIGRATION_VERSION = "v9-higher-native-date-repair";

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

const BOOTSTRAP_OUTPUTSIZE_BY_INTERVAL: Record<RestInterval, number> = {
	"1min": 1150,
	"5min": 1150,
	"15min": 1000,
	"30min": 800,
	"1h": 800,
	"4h": 500,
	"1day": 500,
	"1week": 300,
	"1month": 180,
};

const NORMALIZED_REST_INTERVALS = [
	"1min",
	"5min",
	"15min",
	"30min",
	"1h",
] as const;

type NormalizedRestInterval =
	(typeof NORMALIZED_REST_INTERVALS)[number];

const HIGHER_NATIVE_INTERVALS = [
	"1day",
	"1week",
	"1month",
] as const;

type HigherNativeInterval =
	(typeof HIGHER_NATIVE_INTERVALS)[number];

type NativeNormalizedInterval =
	| NormalizedRestInterval
	| HigherNativeInterval;

type AnalysisNormalizedInterval =
	| NativeNormalizedInterval
	| "3min"
	| "4h";

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
	timeframe: AnalysisNormalizedInterval;
	datetime: string;
	open_time: string;
	expected_close_time: string;
	open: number;
	high: number;
	low: number;
	close: number;
	status: "OPEN" | "CLOSED";
	source: string;
	provisional: boolean;
	confirmed: boolean;
	// Compatibility field. Standalone gap candles are forbidden.
	synthetic_gap: boolean;
	// The real reopening candle owns the price discontinuity.
	gap_adjusted: boolean;
	stored_at_ms: number;
};

type NormalizedMeta = {
	timeframe: NativeNormalizedInterval;
	last_normalized_ms: number;
	last_normalized_time: string;
	latest_datetime: string | null;
	oldest_datetime: string | null;
	raw_rows_seen: number;
	valid_raw_rows: number;
	filtered_closed_rows: number;
	gap_adjusted_rows: number;
	synthetic_gap_rows: 0;
	pending_closed_period: boolean;
	layer: "analysis_normalized";
};

type Derived3MinMeta = {
	timeframe: "3min";
	last_normalized_ms: number;
	last_normalized_time: string;
	latest_datetime: string | null;
	oldest_datetime: string | null;
	source_interval: "1min";
	source_rows_seen: number;
	source_regular_rows: number;
	source_gap_adjusted_rows: number;
	source_synthetic_gap_rows: number;
	complete_3min_buckets: number;
	incomplete_3min_buckets: number;
	gap_containing_3min_buckets: number;
	derived_rows_stored: number;
	pending_closed_period: boolean;
	layer: "analysis_normalized";
};

type Derived4HMeta = {
	timeframe: "4h";
	last_normalized_ms: number;
	last_normalized_time: string;
	latest_datetime: string | null;
	oldest_datetime: string | null;
	source_interval: "1h";
	source_rows_seen: number;
	source_regular_rows: number;
	source_gap_adjusted_rows: number;
	source_synthetic_gap_rows: number;
	complete_4h_buckets: number;
	incomplete_4h_buckets: number;
	gap_absorbed_buckets: number;
	absorbed_synthetic_gap_rows: number;
	derived_rows_stored: number;
	pending_closed_period: boolean;
	layer: "analysis_normalized";
};


function sameStoredHistoricalCandle(
	a: StoredHistoricalCandle | null | undefined,
	b: StoredHistoricalCandle,
) {
	return Boolean(
		a &&
			a.timeframe === b.timeframe &&
			a.datetime === b.datetime &&
			a.open === b.open &&
			a.high === b.high &&
			a.low === b.low &&
			a.close === b.close &&
			a.source === b.source &&
			a.provisional === b.provisional &&
			a.confirmed === b.confirmed
	);
}

function sameNormalizedCandle(
	a: NormalizedCandle | null | undefined,
	b: NormalizedCandle,
) {
	return Boolean(
		a &&
			a.timeframe === b.timeframe &&
			a.datetime === b.datetime &&
			a.open_time === b.open_time &&
			a.expected_close_time === b.expected_close_time &&
			a.open === b.open &&
			a.high === b.high &&
			a.low === b.low &&
			a.close === b.close &&
			a.status === b.status &&
			a.source === b.source &&
			a.provisional === b.provisional &&
			a.confirmed === b.confirmed &&
			a.synthetic_gap === b.synthetic_gap &&
			a.gap_adjusted === b.gap_adjusted
	);
}

type AutoQueueItem = {
	interval: RestInterval;
	outputsize: number;
	reason: string;
	enqueued_ms: number;
	attempts: number;
	// Optional targeted REST end_date cursor used by continuity backfill.
	before?: string | null;
};

type ContinuityGap = {
	interval: AnalysisNormalizedInterval;
	before_datetime: string;
	missing_from: string;
	missing_to: string;
	after_datetime: string;
	missing_buckets: number;
	missing_market_minutes: number;
};

type ContinuityAudit = {
	interval: AnalysisNormalizedInterval;
	latest_effective_datetime: string | null;
	latest_confirmed_datetime: string | null;
	current_provisional_datetime: string | null;
	gap: ContinuityGap | null;
	// A known provider/session-boundary omission that is recorded but does not
	// block analysis readiness. We never synthesize a candle for it.
	boundary_omission: ContinuityGap | null;
	effective_fresh: boolean;
};

type AutoRefreshState = {
	enabled: boolean;
	queue: AutoQueueItem[];
	last_5m_key: string;
	last_15m_key: string;
	last_hour_key: string;
	last_4h_key: string;
	last_day_key: string;
	last_week_close_key: string | null;
	last_month_key: string;
	rate_window_start_ms: number;
	rate_requests: number;
	last_alarm_ms: number | null;
	last_success_ms: number | null;
	last_error: string | null;
	bootstrap_pending: boolean;
	last_recovery_audit_ms?: number | null;
};

type RecoveryBudgetState = {
	utc_day: string;
	estimated_used_rows: number;
	soft_limit_rows: number;
	manual_extra_rows: number;
	hard_ceiling_rows: number;
	last_update_ms: number;
	last_reason: string | null;
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
	gap_adjusted: boolean;
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
	// Project rule: Cairo is always treated as fixed GMT+3.
	// Do not use Africa/Cairo DST rules here.
	const shifted = new Date(ms + 3 * 60 * 60 * 1000);
	return shifted.toISOString().slice(0, 19).replace("T", " ");
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

function isHigherNativeInterval(
	value: string,
): value is HigherNativeInterval {
	return (HIGHER_NATIVE_INTERVALS as readonly string[]).includes(value);
}

function isNativeNormalizedInterval(
	value: string,
): value is NativeNormalizedInterval {
	return isNormalizedRestInterval(value) || isHigherNativeInterval(value);
}

function isAnalysisNormalizedInterval(
	value: string,
): value is AnalysisNormalizedInterval {
	return (
		value === "3min" ||
		value === "4h" ||
		isNativeNormalizedInterval(value)
	);
}

function normalizedCandleKey(
	interval: AnalysisNormalizedInterval,
	datetime: string,
) {
	return `norm:${interval}:${datetime}`;
}

function normalizedPrefix(
	interval: AnalysisNormalizedInterval,
) {
	return `norm:${interval}:`;
}

function normalizedMetaKey(
	interval: AnalysisNormalizedInterval,
) {
	return `normmeta:${interval}`;
}

function normalizedIntervalMinutes(
	interval: NormalizedRestInterval,
) {
	return NORMALIZED_INTERVAL_MINUTES[interval];
}


function parseCairoDatetimeParts(datetime: string) {
	// Twelve Data returns intraday rows as "YYYY-MM-DD HH:mm:ss"
	// but higher native intervals may arrive as date-only "YYYY-MM-DD".
	// Canonicalize date-only higher-timeframe rows to midnight Cairo.
	const match = datetime.match(
		/^(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2}):(\d{2}))?$/,
	);

	if (!match) return null;

	const [, y, mo, d, h = "00", mi = "00", s = "00"] = match;

	return {
		year: Number(y),
		month: Number(mo),
		day: Number(d),
		hour: Number(h),
		minute: Number(mi),
		second: Number(s),
	};
}

function canonicalCairoDatetime(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return null;
	return (
		`${parts.year}-${pad2(parts.month)}-${pad2(parts.day)} ` +
		`${pad2(parts.hour)}:${pad2(parts.minute)}:${pad2(parts.second)}`
	);
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

function threeMinuteBucketStart(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return null;

	const bucketMinute = Math.floor(parts.minute / 3) * 3;

	const pad = (n: number) => String(n).padStart(2, "0");

	return (
		`${parts.year}-${pad(parts.month)}-${pad(parts.day)} ` +
		`${pad(parts.hour)}:${pad(bucketMinute)}:00`
	);
}

function fourHourBucketStart(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return null;

	const bucketHour = Math.floor(parts.hour / 4) * 4;
	const pad = (n: number) => String(n).padStart(2, "0");

	return (
		`${parts.year}-${pad(parts.month)}-${pad(parts.day)} ` +
		`${pad(bucketHour)}:00:00`
	);
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

function pad2(value: number) {
	return String(value).padStart(2, "0");
}

function dayStartCairo(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return datetime;
	return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)} 00:00:00`;
}

function intradayBucketStart(datetime: string, minutes: number) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return null;

	if (minutes === 60) {
		return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)} ${pad2(parts.hour)}:00:00`;
	}

	const bucketMinute = Math.floor(parts.minute / minutes) * minutes;
	return `${parts.year}-${pad2(parts.month)}-${pad2(parts.day)} ${pad2(parts.hour)}:${pad2(bucketMinute)}:00`;
}

function weekStartMondayCairo(datetime: string) {
	const start = dayStartCairo(datetime);
	const ms = cairoDatetimeToMs(start);
	const day = cairoDayOfWeek(start);
	if (ms === null || day === null) return start;

	const daysSinceMonday = (day + 6) % 7;
	return cairoTime(ms - daysSinceMonday * 24 * 60 * 60 * 1000);
}

function monthStartCairo(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return datetime;
	return `${parts.year}-${pad2(parts.month)}-01 00:00:00`;
}

function nextMonthStartCairo(datetime: string) {
	const parts = parseCairoDatetimeParts(datetime);
	if (!parts) return datetime;
	const next = new Date(Date.UTC(parts.year, parts.month, 1, 0, 0, 0));
	return `${next.getUTCFullYear()}-${pad2(next.getUTCMonth() + 1)}-01 00:00:00`;
}

function expectedCloseForHigherInterval(
	interval: HigherNativeInterval,
	datetime: string,
) {
	if (interval === "1day") {
		return addMinutesToCairoDatetime(datetime, 24 * 60);
	}

	if (interval === "1week") {
		// Weekly candles are keyed from Monday 00:00 Cairo but the trading
		// week is complete at Saturday 00:00, when the weekend closure begins.
		return addMinutesToCairoDatetime(datetime, 5 * 24 * 60);
	}

	return nextMonthStartCairo(datetime);
}

function isTradingDayCairo(datetime: string) {
	const day = cairoDayOfWeek(datetime);
	return day !== null && day >= 1 && day <= 5;
}

function hasDeclaredClosureBetween(
	previousExpectedClose: string,
	currentOpen: string,
) {
	const startMs = cairoDatetimeToMs(previousExpectedClose);
	const endMs = cairoDatetimeToMs(currentOpen);
	if (startMs === null || endMs === null || endMs <= startMs) {
		return false;
	}

	for (let ms = startMs; ms < endMs; ms += 60_000) {
		if (isClosedMarketCairoDatetime(cairoTime(ms))) {
			return true;
		}
	}

	return false;
}

function scheduleBucketKey(datetime: string, minutes: number) {
	return intradayBucketStart(datetime, minutes) ?? datetime;
}

function fourHourScheduleKey(datetime: string) {
	return fourHourBucketStart(datetime) ?? datetime;
}

function dateKey(datetime: string) {
	return datetime.slice(0, 10);
}

function monthKey(datetime: string) {
	return datetime.slice(0, 7);
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
	private autoState: AutoRefreshState | null = null;

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
				this.tickCount = saved.tickCount ?? 0;
				this.reconnectCount = saved.reconnectCount ?? 0;
				this.lastError = saved.lastError ?? null;
				this.subscribeStatus = saved.subscribeStatus ?? null;

				// Remove any live candles produced by older versions during
				// a known closed-market period. They must never enter analysis.
				this.candles = (saved.candles ?? [])
					.filter(
						(candle) =>
							!isClosedMarketCairoDatetime(
								candle.datetime,
							),
					)
					.map((candle) => ({
						...candle,
						gap_adjusted: candle.gap_adjusted ?? false,
					}));

				const restoredCurrent = saved.currentCandle ?? null;
				this.currentCandle =
					restoredCurrent &&
					!isClosedMarketCairoDatetime(restoredCurrent.datetime)
						? {
							...restoredCurrent,
							gap_adjusted:
								restoredCurrent.gap_adjusted ?? false,
						}
						: null;

				const lastValidLive =
					this.currentCandle ??
					this.candles[this.candles.length - 1] ??
					null;

				this.lastPrice = lastValidLive?.close ?? null;
				this.lastTickMs = lastValidLive?.last_tick_ms ?? null;
			}

			const now = Date.now();
			const nowCairo = cairoTime(now);
			const savedAuto =
				(await ctx.storage.get<AutoRefreshState>(AUTO_STATE_KEY)) ?? null;

			if (savedAuto) {
				this.autoState = savedAuto;
			} else {
				// v14.6 controlled activation: deploy alone never starts bootstrap.
				this.autoState = this.createInitialAutoState(nowCairo, now);
				this.autoState.enabled = false;
				this.autoState.queue = [];
				this.autoState.bootstrap_pending = false;
			}

			// Constructor stays strictly read-only: no put(), no setAlarm().
		});
	}

	async fetch(request: Request) {
		const url = new URL(request.url);

		// Zero-write instance probe. This route is used to distinguish an
		// account/platform quota problem from one poisoned/stuck Durable Object
		// instance. The constructor is read-only in v14.3+.
		if (url.pathname === "/instance-probe") {
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				instance_probe: true,
				storage_write_attempted: false,
				connection_status: this.connectionStatus,
				last_error: this.lastError,
			});
		}

		if (url.pathname === "/retire-instance") {
			return json(await this.retireThisInstance());
		}

		if (this.isRetiredInstance()) {
			return json(
				{
					status: "retired",
					build_version: BUILD_VERSION,
					object_name: this.objectName(),
					note:
						"Legacy Durable Object is retired. Production uses the v14.6 object.",
				},
				410,
			);
		}

		if (
			url.pathname === "/sync" ||
			url.pathname === "/normalize" ||
			url.pathname === "/purge" ||
			url.pathname === "/auto/run"
		) {
			return json(
				{
					status: "blocked",
					build_version: BUILD_VERSION,
					error:
						"Direct heavy maintenance is disabled in v14.6. Use guarded recovery.",
				},
				403,
			);
		}

		if (url.pathname === "/activate") {
			if (!this.isProductionInstance()) {
				return json(
					{
						status: "error",
						error: "Activation is allowed only on the v14.6 production object.",
						object_name: this.objectName(),
					},
					409,
				);
			}

			this.ensureAutoState();
			const state = this.autoState!;
			state.enabled = true;
			state.last_error = null;

			if (state.queue.length === 0) {
				this.enqueueBootstrapPlan();
			}
			state.bootstrap_pending = state.queue.some((item) =>
				item.reason.includes("bootstrap"),
			);

			const persisted = await this.persistAutoState();
			const alarmScheduled = await this.scheduleAutoAlarm(1_000);
			return json({
				status:
					persisted && alarmScheduled ? "ok" : "degraded",
				build_version: BUILD_VERSION,
				action: "activate",
				object_name: this.objectName(),
				auto: this.publicAutoState(),
				recovery_budget: await this.recoveryBudgetStatus(),
			});
		}

		if (url.pathname === "/maintenance/recovery-budget") {
			const addParam = url.searchParams.get("add");
			if (addParam === null) {
				return json({
					status: "ok",
					build_version: BUILD_VERSION,
					recovery_budget: await this.recoveryBudgetStatus(),
				});
			}

			const add = Number(addParam);
			if (!Number.isInteger(add) || add <= 0) {
				return json(
					{
						status: "error",
						error: "add must be a positive integer number of estimated recovery rows.",
					},
					400,
				);
			}

			const budget = await this.addRecoveryBudget(add);
			if (this.autoState?.enabled) {
				await this.scheduleAutoAlarm(1_000);
			}
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				add_requested_rows: Math.min(
					add,
					RECOVERY_MAX_MANUAL_ADD_ROWS,
				),
				recovery_budget: budget,
			});
		}

		// Ordinary reads never attempt a watchdog write before their route.

		if (url.pathname === "/wake") {
			this.ensureAutoState();
			if (!this.autoState!.enabled) {
				return json(
					{
						status: "inactive",
						build_version: BUILD_VERSION,
						action: "wake",
						note:
							"Production is intentionally inactive. Call /activate once.",
						auto: this.publicAutoState(),
					},
					409,
				);
			}

			this.lastError = null;
			this.autoState!.last_error = null;
			const statePersisted = await this.persistAutoState();
			const alarmScheduled = await this.scheduleAutoAlarm(1_000);
			const scheduledAlarmMs = await this.ctx.storage.getAlarm();

			return json({
				status:
					statePersisted && alarmScheduled ? "ok" : "degraded",
				build_version: BUILD_VERSION,
				action: "wake",
				state_persisted: statePersisted,
				alarm_scheduled: alarmScheduled,
				scheduled_alarm_ms: scheduledAlarmMs,
				last_error: this.lastError,
				auto: this.publicAutoState(),
				recovery_budget: await this.recoveryBudgetStatus(),
			});
		}

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
					"Tick-built candles are provisional. Closed-market ticks are excluded. On reopening, the first real candle opens at the previous valid close. REST historical candles remain authoritative.",

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
				this.ensureAutoState();
				this.enqueueAutoIntervals(
					REST_INTERVALS,
					outputsize,
					"manual_sync_all",
				);
				await this.persistAutoState();
				await this.processAutoQueue();
				await this.scheduleAutoAlarm();

				return json({
					status: "ok",
					mode: "queued_all",
					symbol: SYMBOL,
					timezone: TIMEZONE,
					note:
						"All REST intervals were queued and are processed automatically within the Twelve Data rate budget. 3min is derived locally and never requested.",
					auto: this.publicAutoState(),
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

			if (result.status === "ok") {
				await this.postAutoRefresh({
					interval: requestedInterval,
					outputsize,
					reason: before ? "manual_backfill" : "manual_sync",
					enqueued_ms: Date.now(),
					attempts: 0,
				});
			}

			return json(
				{
					...result,
					reconciled_normalized_layer: result.status === "ok",
				},
				result.status === "ok" ? 200 : 502,
			);
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

				results.push(await this.deriveThreeMinuteFromOneMinute());
				results.push(await this.deriveFourHourFromOneHour());

				for (const interval of HIGHER_NATIVE_INTERVALS) {
					results.push(
						await this.normalizeHigherNativeInterval(interval),
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

			if (requestedInterval === "3min") {
				const result =
					await this.deriveThreeMinuteFromOneMinute();

				return json(
					result,
					result.status === "ok" ? 200 : 500,
				);
			}

			if (requestedInterval === "4h") {
				const result =
					await this.deriveFourHourFromOneHour();

				return json(
					result,
					result.status === "ok" ? 200 : 500,
				);
			}

			if (isHigherNativeInterval(requestedInterval)) {
				const result =
					await this.normalizeHigherNativeInterval(requestedInterval);

				return json(
					result,
					result.status === "ok" ? 200 : 500,
				);
			}

			if (!isNormalizedRestInterval(requestedInterval)) {
				return json(
					{
						status: "error",
						error: "Unsupported normalized interval",
						supported_intervals: [
							"1min",
							"3min",
							"5min",
							"15min",
							"30min",
							"1h",
							"4h",
							"1day",
							"1week",
							"1month",
						],
						note:
							"No standalone gap candles are created. 3min is derived from 1min. 4h is the gap-aware view derived from normalized 1h. Daily, weekly, and monthly confirmed history remains native Twelve Data REST.",
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

			// v14.6: normalized-data is strictly read-only. Higher native
			// normalization is populated by guarded bootstrap/recovery.

			if (!isAnalysisNormalizedInterval(interval)) {
				return json(
					{
						status: "error",
						error: "Unsupported normalized interval",
						supported_intervals: [
							"1min",
							"3min",
							"5min",
							"15min",
							"30min",
							"1h",
							"4h",
							"1day",
							"1week",
							"1month",
						],
					},
					400,
				);
			}

			let limit = Number(url.searchParams.get("limit") ?? 100);

			if (!Number.isInteger(limit) || limit < 1) {
				limit = 100;
			}

			// Keep individual Action responses compact enough for GPT Actions.
			// Larger analytical windows are read through the cache cursor
			// (`next_before`) without making any Twelve Data REST request.
			limit = Math.min(limit, 200);

			const beforeParam = url.searchParams.get("before");
			const before =
				beforeParam && beforeParam.trim().length > 0
					? canonicalCairoDatetime(beforeParam.trim())
					: null;

			if (beforeParam && !before) {
				return json(
					{
						status: "error",
						error:
							"Invalid before cursor. Use the exact next_before value returned by the previous page.",
					},
					400,
				);
			}

			const listOptions: Record<string, unknown> = {
				prefix: normalizedPrefix(interval),
				reverse: true,
				// One extra confirmed row lets the endpoint advertise whether
				// another cache page exists without a second storage read.
				limit: limit + 1,
			};

			if (before) {
				// Durable Object list `end` is exclusive. Since normalized
				// keys end with canonical datetime, this returns rows strictly
				// older than the cursor and prevents boundary duplication.
				listOptions.end =
					normalizedCandleKey(interval, before);
			}

			const rows =
				await this.ctx.storage.list<NormalizedCandle>(
					listOptions,
				);

			const confirmedWindow = Array.from(rows.values()).sort(
				(a, b) => b.datetime.localeCompare(a.datetime),
			);

			// The provisional tail belongs only on the newest page.
			// Historical cursor pages must never repeat the live/current tail.
			const provisionalTail = before
				? []
				: await this.buildProvisionalTail(interval);

			const currentProvisional =
				!before && provisionalTail.length > 0
					? provisionalTail[provisionalTail.length - 1]
					: null;

			const merged = new Map<string, NormalizedCandle>();
			for (const candle of provisionalTail) {
				merged.set(candle.datetime, candle);
			}
			// Confirmed REST/derived-confirmed history wins on overlap.
			for (const candle of confirmedWindow) {
				merged.set(candle.datetime, candle);
			}

			const mergedSorted = Array.from(merged.values()).sort(
				(a, b) => b.datetime.localeCompare(a.datetime),
			);

			const candles = mergedSorted.slice(0, limit);
			const oldestReturned =
				candles.length > 0
					? candles[candles.length - 1].datetime
					: null;

			const returnedConfirmedDatetimes = new Set(
				candles
					.filter((c) => c.confirmed === true)
					.map((c) => c.datetime),
			);

			const hasMoreConfirmed =
				oldestReturned !== null &&
				confirmedWindow.some(
					(c) =>
						c.datetime.localeCompare(oldestReturned) < 0 ||
						!returnedConfirmedDatetimes.has(c.datetime),
				);

			const nextBefore =
				hasMoreConfirmed && oldestReturned
					? oldestReturned
					: null;

			const confirmedReturnedCount = candles.filter(
				(c) => c.confirmed === true,
			).length;
			const provisionalReturnedCount = candles.filter(
				(c) => c.provisional === true,
			).length;

			const meta =
				(await this.ctx.storage.get<
					NormalizedMeta | Derived3MinMeta | Derived4HMeta
				>(
					normalizedMetaKey(interval),
				)) ?? null;

			return json({
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval,
				layer: "analysis_normalized_effective",
				count: candles.length,
				confirmed_count: confirmedReturnedCount,
				provisional_tail_count: provisionalReturnedCount,
				current_provisional: currentProvisional,
				effective_last_price:
					!before
						? currentProvisional?.close ??
							candles[0]?.close ??
							this.lastPrice ??
							null
						: candles[0]?.close ?? null,
				pagination: {
					page_limit: limit,
					requested_before: before,
					has_more: hasMoreConfirmed,
					next_before: nextBefore,
					source: "durable_object_effective_cache",
					twelve_data_request_per_page: 0,
				},
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
					"gap_adjusted",
					"provisional",
					"confirmed",
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
					false,
					c.gap_adjusted ?? false,
					c.provisional,
					c.confirmed,
				]),
			});
		}

		if (url.pathname === "/auto/status") {
			this.ensureAutoState();
			return json({
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				auto: this.publicAutoState(),
			});
		}

		if (url.pathname === "/auto/start") {
			return json(
				{
					status: "redirect",
					build_version: BUILD_VERSION,
					note:
						"Use /activate once for controlled bootstrap, or /wake after activation.",
					activate_path: "/activate",
				},
				409,
			);
		}

		if (url.pathname === "/auto/stop") {
			this.ensureAutoState();
			this.autoState!.enabled = false;
			await this.persistAutoState();
			await this.ctx.storage.deleteAlarm();
			return json({
				status: "ok",
				auto: this.publicAutoState(),
			});
		}

		if (url.pathname === "/auto/run") {
			this.ensureAutoState();
			const scope = url.searchParams.get("scope") ?? "all";
			let outputsize = Number(
				url.searchParams.get("outputsize") ?? AUTO_INCREMENTAL_OUTPUTSIZE,
			);
			if (!Number.isInteger(outputsize) || outputsize < 1) {
				outputsize = AUTO_INCREMENTAL_OUTPUTSIZE;
			}
			outputsize = Math.min(outputsize, 1150);

			if (scope === "all") {
				this.enqueueAutoIntervals(
					REST_INTERVALS,
					outputsize,
					"manual_auto_run",
				);
			} else if (isRestInterval(scope)) {
				this.enqueueAutoIntervals(
					[scope],
					outputsize,
					"manual_auto_run",
				);
			} else {
				return json(
					{
						status: "error",
						error: "scope must be all or a supported REST interval",
					},
					400,
				);
			}

			await this.persistAutoState();
			await this.processAutoQueue();
			await this.scheduleAutoAlarm();

			return json({
				status: "ok",
				auto: this.publicAutoState(),
			});
		}

		if (url.pathname === "/ensure-data-ready") {
			this.ensureAutoState();
			if (!this.autoState!.enabled) {
				return json(
					{
						status: "inactive",
						build_version: BUILD_VERSION,
						analysis_ready: false,
						repair_requested: false,
						repair_status: "inactive_until_activate",
						auto: this.publicAutoState(),
						recovery_budget: await this.recoveryBudgetStatus(),
					},
					409,
				);
			}

			const result = await this.ensureGoldDataReady();
			return json(
				{
					...result,
					recovery_budget: await this.recoveryBudgetStatus(),
				},
				result.status === "error" ? 500 : 200,
			);
		}

		if (url.pathname === "/system-check") {
			let migration: unknown;
			try {
				const storedMigrationVersion =
					(await this.ctx.storage.get<string>(STORAGE_MIGRATION_KEY)) ?? null;
				migration = {
					status:
						storedMigrationVersion === STORAGE_MIGRATION_VERSION
							? "ok"
							: "pending_or_unknown",
					version: storedMigrationVersion,
					expected_version: STORAGE_MIGRATION_VERSION,
					read_only_check: true,
				};
			} catch (error) {
				migration = {
					status: "error",
					error: error instanceof Error ? error.message : String(error),
					read_only_check: true,
				};
			}
			const frames = [];
			for (const interval of [
				"1min",
				"3min",
				"5min",
				"15min",
				"30min",
				"1h",
				"4h",
				"1day",
				"1week",
				"1month",
			] as AnalysisNormalizedInterval[]) {
				const latest =
					await this.ctx.storage.list<NormalizedCandle>({
						prefix: normalizedPrefix(interval),
						reverse: true,
						limit: 1,
					});
				const confirmed = Array.from(latest.values())[0] ?? null;
				const provisional =
					await this.buildCurrentProvisional(interval);

				frames.push({
					interval,
					latest_confirmed_datetime: confirmed?.datetime ?? null,
					latest_confirmed_close: confirmed?.close ?? null,
					current_provisional_datetime: provisional?.datetime ?? null,
					current_provisional_close: provisional?.close ?? null,
					current_source: provisional?.source ?? null,
				});
			}

			const scheduledAlarmMs = await this.ctx.storage.getAlarm();

			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				symbol: SYMBOL,
				timezone: TIMEZONE,
				object_name: this.objectName(),
				object_role:
					this.isProductionInstance()
						? "production"
						: this.isRetiredInstance()
							? "retired"
							: "other",
				market_closed: isClosedMarketCairoDatetime(cairoTime(Date.now())),
				last_live_price: this.lastPrice,
				storage_migration: migration,
				scheduled_alarm_ms: scheduledAlarmMs,
				auto: this.publicAutoState(),
				recovery_budget: await this.recoveryBudgetStatus(),
				frames,
				analysis_performed: false,
				read_only_route: true,
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
					"Normalized layers are maintained separately from raw REST. Closed-period rows are removed and reopening gaps are contained inside the first real candle after reopening. 3min is derived from normalized 1min; 4h is derived gap-aware from normalized 1h.",
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
			build_version: BUILD_VERSION,

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
				derive_3min:
					"/normalize?interval=3min",
				derive_4h:
					"/normalize?interval=4h",
				normalize_all_supported:
					"/normalize?interval=all",
				normalized_data:
					"/normalized-data?interval=1min&limit=100",
				normalized_3min:
					"/normalized-data?interval=3min&limit=100",
				normalized_4h:
					"/normalized-data?interval=4h&limit=100",
				normalized_daily:
					"/normalized-data?interval=1day&limit=100",
				normalized_weekly:
					"/normalized-data?interval=1week&limit=100",
				normalized_monthly:
					"/normalized-data?interval=1month&limit=100",
				auto_status: "/auto/status",
				auto_start: "/auto/start",
				auto_stop: "/auto/stop",
				auto_run: "/auto/run?scope=all",
				system_check: "/system-check",
				storage_state: "/storage-state",
				purge:
					"/purge?interval=1min&confirm=yes",
			},

			data_policy: {
				live_ticks: "provisional; closed-market ticks excluded; reopening candle gap-adjusted",
				historical_rest: "authoritative",
				persistent_storage: "raw REST candles by timeframe",
				normalized_storage:
					"session-filtered candles with reopening gaps absorbed into the first real candle; no standalone gap rows and no market-analysis logic",
				current_effective_chain:
					"WebSocket -> 1min -> intraday provisional -> 4h from effective 15min -> daily from effective 1h -> weekly/monthly from effective daily",
				auto_refresh:
					"Durable Object alarm queue, rate-budgeted and write-efficient; overlapping REST/normalized rows use diff-upsert and derived layers are updated incrementally",
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

		await this.processTick(price, tickMs);
	}

	private async processTick(price: number, tickMs: number) {
		const bucket = minuteStart(tickMs);
		const bucketDatetime = cairoTime(bucket);

		// Provider snapshots/ticks inside the declared closed-market
		// windows are invalid for analysis and are ignored completely.
		if (isClosedMarketCairoDatetime(bucketDatetime)) {
			return;
		}

		this.lastPrice = price;
		this.lastTickMs = tickMs;
		this.tickCount++;

		if (!this.currentCandle) {
			const previousMinuteWasClosed =
				isClosedMarketCairoDatetime(
					cairoTime(bucket - 60_000),
				);

			let anchoredOpen: number | null = null;

			if (previousMinuteWasClosed) {
				anchoredOpen =
					await this.latestNormalizedOneMinuteClose();
			}

			this.currentCandle =
				this.createCandle(
					bucket,
					price,
					tickMs,
					anchoredOpen,
					anchoredOpen !== null,
				);

			this.ctx.waitUntil(this.persist());
			return;
		}

		// Same provisional minute candle.
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

		// Old/out-of-order tick.
		if (bucket < this.currentCandle.start_ms) {
			return;
		}

		const previousMinuteWasClosed =
			isClosedMarketCairoDatetime(
				cairoTime(bucket - 60_000),
			);

		const previousValidClose =
			this.currentCandle.close;

		// Close the previous provisional real candle. Missing/closed
		// minutes are never fabricated as candles in between.
		this.candles.push({
			...this.currentCandle,
		});

		if (this.candles.length > MAX_STORED_CANDLES) {
			this.candles =
				this.candles.slice(-MAX_STORED_CANDLES);
		}

		this.currentCandle =
			this.createCandle(
				bucket,
				price,
				tickMs,
				previousMinuteWasClosed
					? previousValidClose
					: null,
				previousMinuteWasClosed,
			);

		this.ctx.waitUntil(this.persist());
	}

	private async latestNormalizedOneMinuteClose() {
		const latest =
			await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix("1min"),
				reverse: true,
				limit: 1,
			});

		const candle = Array.from(latest.values())[0] ?? null;
		return candle?.close ?? null;
	}

	private createCandle(
		startMs: number,
		price: number,
		tickMs: number,
		openOverride: number | null = null,
		gapAdjusted = false,
	): ProvisionalCandle {
		const open = openOverride ?? price;

		return {
			start_ms: startMs,
			datetime: cairoTime(startMs),

			open,
			high: Math.max(open, price),
			low: Math.min(open, price),
			close: price,

			tick_count: 1,

			first_tick_ms: tickMs,
			last_tick_ms: tickMs,

			source: "websocket_ticks",

			provisional: true,
			confirmed: false,
			gap_adjusted: gapAdjusted,
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

			if (isNormalizedRestInterval(interval) || isHigherNativeInterval(interval)) {
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
				if (!Array.isArray(row) || row.length < 5) continue;

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

			// v14 write-efficiency: REST pages heavily overlap by design. Compare
			// deterministic candle content and write only genuinely new/changed rows.
			const changedEntries: Record<string, StoredHistoricalCandle> = {};
			let unchangedRows = 0;
			for (const candle of validRows) {
				const key = historicalCandleKey(interval, candle.datetime);
				const existing =
					(await this.ctx.storage.get<StoredHistoricalCandle>(key)) ?? null;
				if (sameStoredHistoricalCandle(existing, candle)) {
					unchangedRows++;
					continue;
				}
				changedEntries[key] = candle;
			}

			const changedKeys = Object.keys(changedEntries);
			for (let i = 0; i < changedKeys.length; i += 100) {
				const entries: Record<string, StoredHistoricalCandle> = {};
				for (const key of changedKeys.slice(i, i + 100)) {
					entries[key] = changedEntries[key];
				}
				if (Object.keys(entries).length > 0) {
					await this.ctx.storage.put(entries);
				}
			}

			const existingMeta =
				(await this.ctx.storage.get<HistoricalMeta>(
					historicalMetaKey(interval),
				)) ?? null;

			const datetimes = validRows.map((c) => c.datetime).sort();
			const requestOldest = datetimes.length > 0 ? datetimes[0] : null;
			const requestNewest =
				datetimes.length > 0 ? datetimes[datetimes.length - 1] : null;

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
			const latestDatetime = newestCandidates.sort().reverse()[0] ?? null;

			const meta: HistoricalMeta = {
				timeframe: interval,
				last_sync_ms: now,
				last_sync_time: cairoTime(now),
				latest_datetime: latestDatetime,
				oldest_datetime: oldestDatetime,
				last_request_count: validRows.length,
				last_requested_outputsize: outputsize,
				next_before: oldestDatetime,
				source: "historical_rest",
			};

			// One metadata write per actual REST call is intentional and bounded.
			await this.ctx.storage.put(historicalMetaKey(interval), meta);

			return {
				status: "ok",
				symbol: SYMBOL,
				interval,
				requested_outputsize: outputsize,
				rows_received: payload.candles.length,
				rows_considered_after_requested_limit: rowsToStore.length,
				rows_validated: validRows.length,
				rows_written: changedKeys.length,
				rows_unchanged_skipped: unchangedRows,
				request_newest: requestNewest,
				request_oldest: requestOldest,
				stored_latest_datetime: latestDatetime,
				stored_oldest_datetime: oldestDatetime,
				next_before: meta.next_before,
				dedupe_key: "interval + datetime",
				storage_layer: "raw_rest_persistent",
				write_policy: "diff_upsert_only",
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


	private async upsertNormalizedCandidates(
		candidates: NormalizedCandle[],
	) {
		let written = 0;
		let unchanged = 0;
		const changed: Record<string, NormalizedCandle> = {};

		for (const candle of candidates) {
			const key = normalizedCandleKey(candle.timeframe, candle.datetime);
			const existing =
				(await this.ctx.storage.get<NormalizedCandle>(key)) ?? null;
			if (sameNormalizedCandle(existing, candle)) {
				unchanged++;
				continue;
			}
			changed[key] = candle;
		}

		const keys = Object.keys(changed);
		for (let i = 0; i < keys.length; i += 100) {
			const entries: Record<string, NormalizedCandle> = {};
			for (const key of keys.slice(i, i + 100)) {
				entries[key] = changed[key];
			}
			if (Object.keys(entries).length > 0) {
				await this.ctx.storage.put(entries);
				written += Object.keys(entries).length;
			}
		}

		return { written, unchanged };
	}

	private async reconcileNormalizedRange(
		interval: AnalysisNormalizedInterval,
		candidates: NormalizedCandle[],
		fromDatetime: string | null,
		toDatetime: string | null,
	) {
		const stats = await this.upsertNormalizedCandidates(candidates);
		if (!fromDatetime || !toDatetime) {
			return { ...stats, deleted: 0 };
		}

		const keep = new Set(candidates.map((c) => c.datetime));
		const deleteKeys: string[] = [];
		let startAfter: string | undefined;

		while (true) {
			const page = await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix(interval),
				limit: 1000,
				...(startAfter ? { startAfter } : {}),
			});
			if (page.size === 0) break;

			for (const [key, candle] of page.entries()) {
				if (
					candle.datetime >= fromDatetime &&
					candle.datetime <= toDatetime &&
					!keep.has(candle.datetime)
				) {
					deleteKeys.push(key);
				}
			}

			if (page.size < 1000) break;
			const keys = Array.from(page.keys()) as string[];
			startAfter = keys[keys.length - 1];
		}

		for (let i = 0; i < deleteKeys.length; i += 100) {
			await this.ctx.storage.delete(deleteKeys.slice(i, i + 100));
		}

		return { ...stats, deleted: deleteKeys.length };
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
			let gapAdjustedRows = 0;
			let pendingClosedPeriod = false;

			let lastValid: StoredHistoricalCandle | null = null;
			let pendingGap: {
				previous_close: number;
			} | null = null;

			for (const candle of raw) {
				const closedMarket =
					isClosedMarketCairoDatetime(candle.datetime);

				if (closedMarket) {
					filteredClosedRows++;

					// Remember only the price before the closure. We never
					// create a candle for the closed period itself.
					if (lastValid && !pendingGap) {
						pendingGap = {
							previous_close: lastValid.close,
						};
					}

					continue;
				}

				const implicitClosureGap =
					lastValid !== null &&
					hasDeclaredClosureBetween(
						addMinutesToCairoDatetime(
							lastValid.datetime,
							durationMinutes,
						),
						candle.datetime,
					);
				const gapAdjusted =
					pendingGap !== null || implicitClosureGap;
				const normalizedOpen = gapAdjusted
					? pendingGap?.previous_close ?? lastValid!.close
					: candle.open;

				// The reopening candle contains the whole movement from the
				// previous valid close to the provider's reopening prices.
				// Therefore its O/H/L/C remains a real candle, with O anchored
				// to the previous close and the range expanded if necessary.
				const normalizedHigh = Math.max(
					candle.high,
					candle.open,
					normalizedOpen,
				);
				const normalizedLow = Math.min(
					candle.low,
					candle.open,
					normalizedOpen,
				);

				const expectedCloseTime =
					addMinutesToCairoDatetime(
						candle.datetime,
						durationMinutes,
					);

				// REST is authoritative only after the candle closes. The
				// still-open candle is supplied by the provisional live chain.
				if (statusFromExpectedClose(expectedCloseTime) === "OPEN") {
					continue;
				}

				normalized.push({
					timeframe: interval,
					datetime: candle.datetime,
					open_time: candle.datetime,
					expected_close_time: expectedCloseTime,
					open: normalizedOpen,
					high: normalizedHigh,
					low: normalizedLow,
					close: candle.close,
					status:
						statusFromExpectedClose(
							expectedCloseTime,
						),
					source: gapAdjusted
						? "historical_rest_gap_adjusted"
						: "historical_rest",
					provisional: false,
					confirmed: true,
					synthetic_gap: false,
					gap_adjusted: gapAdjusted,
					stored_at_ms: Date.now(),
				});

				if (gapAdjusted) {
					gapAdjustedRows++;
					pendingGap = null;
				}

				lastValid = candle;
			}

			if (pendingGap) {
				pendingClosedPeriod = true;
			}

			// v14: reconcile by content. Existing identical rows are left untouched;
			// obsolete rows inside the source range are removed selectively.
			const sourceFrom = raw.length > 0 ? raw[0].datetime : null;
			const sourceTo = raw.length > 0 ? raw[raw.length - 1].datetime : null;
			const writeStats = await this.reconcileNormalizedRange(
				interval,
				normalized,
				sourceFrom,
				sourceTo,
			);

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
				gap_adjusted_rows: gapAdjustedRows,
				synthetic_gap_rows: 0,
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
				gap_adjusted_rows: gapAdjustedRows,
				synthetic_gap_rows: 0,
				pending_closed_period: pendingClosedPeriod,
				normalized_rows_stored: normalized.length,
				rows_written: writeStats.written,
				rows_unchanged_skipped: writeStats.unchanged,
				rows_deleted: writeStats.deleted,
				oldest_datetime: meta.oldest_datetime,
				latest_datetime: meta.latest_datetime,
				analysis_performed: false,
				note:
					"Closed-market rows are removed. No standalone gap candle is created. The first real candle after a closure opens at the previous valid close and contains the reopening price gap in its own range. No FVG, liquidity, sweep, mitigation, or structure analysis is performed.",
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

	private async deriveThreeMinuteFromOneMinute() {
		try {
			const source: NormalizedCandle[] = [];
			let startAfter: string | undefined;

			while (true) {
				const page =
					await this.ctx.storage.list<NormalizedCandle>({
						prefix: normalizedPrefix("1min"),
						limit: 1000,
						...(startAfter ? { startAfter } : {}),
					});

				if (page.size === 0) {
					break;
				}

				source.push(...page.values());

				if (page.size < 1000) {
					break;
				}

				const keys = Array.from(page.keys()) as string[];
				startAfter = keys[keys.length - 1];
			}

			source.sort((a, b) =>
				a.datetime.localeCompare(b.datetime),
			);

			if (source.length === 0) {
				return {
					status: "error",
					interval: "3min",
					error:
						"No normalized 1min candles found. Run /normalize?interval=1min first.",
				};
			}

			const legacySyntheticRows = source.filter(
				(c) => c.synthetic_gap === true,
			);

			if (legacySyntheticRows.length > 0) {
				return {
					status: "error",
					interval: "3min",
					error:
						"Legacy standalone synthetic-gap rows detected in normalized 1min. Run /normalize?interval=1min first, or use /normalize?interval=all.",
				};
			}

			const sourceMeta =
				(await this.ctx.storage.get<NormalizedMeta>(
					normalizedMetaKey("1min"),
				)) ?? null;

			const regularRows = source;
			const sourceGapAdjustedRows = source.filter(
				(c) => c.gap_adjusted === true,
			).length;

			const buckets = new Map<string, NormalizedCandle[]>();

			for (const candle of regularRows) {
				const bucketStart =
					threeMinuteBucketStart(candle.datetime);

				if (!bucketStart) {
					continue;
				}

				const bucket = buckets.get(bucketStart) ?? [];
				bucket.push(candle);
				buckets.set(bucketStart, bucket);
			}

			const derived: NormalizedCandle[] = [];
			let completeBuckets = 0;
			let incompleteBuckets = 0;
			let gapContainingBuckets = 0;

			for (const [bucketStart, rows] of buckets.entries()) {
				rows.sort((a, b) =>
					a.datetime.localeCompare(b.datetime),
				);

				const expectedDatetimes = [
					bucketStart,
					addMinutesToCairoDatetime(bucketStart, 1),
					addMinutesToCairoDatetime(bucketStart, 2),
				];

				const actualDatetimes = rows.map((c) => c.datetime);

				const complete =
					rows.length === 3 &&
					expectedDatetimes.every(
						(value, index) =>
							actualDatetimes[index] === value,
					);

				if (!complete) {
					incompleteBuckets++;
					continue;
				}

				const expectedCloseTime =
					addMinutesToCairoDatetime(bucketStart, 3);
				const gapAdjusted = rows.some(
					(c) => c.gap_adjusted === true,
				);

				if (gapAdjusted) {
					gapContainingBuckets++;
				}

				derived.push({
					timeframe: "3min",
					datetime: bucketStart,
					open_time: bucketStart,
					expected_close_time: expectedCloseTime,
					open: rows[0].open,
					high: Math.max(...rows.map((c) => c.high)),
					low: Math.min(...rows.map((c) => c.low)),
					close: rows[2].close,
					status:
						statusFromExpectedClose(
							expectedCloseTime,
						),
					source: "derived_1min",
					provisional: false,
					confirmed: true,
					synthetic_gap: false,
					gap_adjusted: gapAdjusted,
					stored_at_ms: Date.now(),
				});

				completeBuckets++;
			}

			derived.sort((a, b) =>
				a.datetime.localeCompare(b.datetime),
			);

			// v14: diff/reconcile the derived layer instead of delete-all/rewrite-all.
			const sourceFrom = source.length > 0
				? threeMinuteBucketStart(source[0].datetime)
				: null;
			const sourceTo = source.length > 0
				? threeMinuteBucketStart(source[source.length - 1].datetime)
				: null;
			const writeStats = await this.reconcileNormalizedRange(
				"3min",
				derived,
				sourceFrom,
				sourceTo,
			);

			const datetimes = derived
				.map((c) => c.datetime)
				.sort();

			const now = Date.now();

			const meta: Derived3MinMeta = {
				timeframe: "3min",
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
				source_interval: "1min",
				source_rows_seen: source.length,
				source_regular_rows: regularRows.length,
				source_gap_adjusted_rows: sourceGapAdjustedRows,
				source_synthetic_gap_rows: 0,
				complete_3min_buckets: completeBuckets,
				incomplete_3min_buckets: incompleteBuckets,
				gap_containing_3min_buckets: gapContainingBuckets,
				derived_rows_stored: derived.length,
				pending_closed_period:
					sourceMeta?.pending_closed_period ?? false,
				layer: "analysis_normalized",
			};

			await this.ctx.storage.put(
				normalizedMetaKey("3min"),
				meta,
			);

			return {
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval: "3min",
				interval_minutes: 3,
				layer: "analysis_normalized",
				derived_from: "normalized_1min",
				source_rows_seen: source.length,
				source_regular_rows: regularRows.length,
				source_gap_adjusted_rows: sourceGapAdjustedRows,
				source_synthetic_gap_rows: 0,
				complete_3min_buckets: completeBuckets,
				incomplete_3min_buckets: incompleteBuckets,
				gap_containing_3min_buckets: gapContainingBuckets,
				synthetic_gap_rows: 0,
				pending_closed_period:
					meta.pending_closed_period,
				normalized_rows_stored: derived.length,
				rows_written: writeStats.written,
				rows_unchanged_skipped: writeStats.unchanged,
				rows_deleted: writeStats.deleted,
				oldest_datetime: meta.oldest_datetime,
				latest_datetime: meta.latest_datetime,
				analysis_performed: false,
				note:
					"3min is deterministic OHLC aggregation from normalized 1min. Reopening gaps are already contained inside the first real 1min candle, so no standalone gap row is created or carried into 3min. No market-analysis logic is performed.",
			};
		} catch (error) {
			return {
				status: "error",
				interval: "3min",
				error:
					error instanceof Error
						? error.message
						: String(error),
			};
		}
	}

	private async deriveFourHourFromOneHour() {
		try {
			const source: NormalizedCandle[] = [];
			let startAfter: string | undefined;

			while (true) {
				const page =
					await this.ctx.storage.list<NormalizedCandle>({
						prefix: normalizedPrefix("1h"),
						limit: 1000,
						...(startAfter ? { startAfter } : {}),
					});

				if (page.size === 0) {
					break;
				}

				source.push(...page.values());

				if (page.size < 1000) {
					break;
				}

				const keys = Array.from(page.keys()) as string[];
				startAfter = keys[keys.length - 1];
			}

			source.sort((a, b) =>
				a.datetime.localeCompare(b.datetime),
			);

			if (source.length === 0) {
				return {
					status: "error",
					interval: "4h",
					error:
						"No normalized 1h candles found. Run /normalize?interval=1h first.",
				};
			}

			const legacySyntheticRows = source.filter(
				(c) => c.synthetic_gap === true,
			);

			if (legacySyntheticRows.length > 0) {
				return {
					status: "error",
					interval: "4h",
					error:
						"Legacy standalone synthetic-gap rows detected in normalized 1h. Run /normalize?interval=1h first, or use /normalize?interval=all.",
				};
			}

			const sourceMeta =
				(await this.ctx.storage.get<NormalizedMeta>(
					normalizedMetaKey("1h"),
				)) ?? null;

			const regularRows = source;
			const sourceGapAdjustedRows = source.filter(
				(c) => c.gap_adjusted === true,
			).length;

			const buckets = new Map<string, NormalizedCandle[]>();

			for (const candle of regularRows) {
				const bucketStart =
					fourHourBucketStart(candle.datetime);

				if (!bucketStart) {
					continue;
				}

				const bucket = buckets.get(bucketStart) ?? [];
				bucket.push(candle);
				buckets.set(bucketStart, bucket);
			}

			const derived: NormalizedCandle[] = [];
			let completeBuckets = 0;
			let incompleteBuckets = 0;
			let gapAbsorbedBuckets = 0;

			for (const [bucketStart, rowsInput] of Array.from(
				buckets.entries(),
			).sort(([a], [b]) => a.localeCompare(b))) {
				const parts =
					parseCairoDatetimeParts(bucketStart);

				if (!parts) {
					incompleteBuckets++;
					continue;
				}

				const rows = rowsInput
					.slice()
					.sort((a, b) =>
						a.datetime.localeCompare(b.datetime),
					);

				let expectedDatetimes: string[];

				if (parts.hour === 0) {
					// Project rule: the 00:00-04:00 4H bucket has no
					// standalone closure candle. Its first real 1H row is
					// 01:00 and that row already opens at the previous valid
					// close, so the daily/weekend gap lives inside this 4H candle.
					expectedDatetimes = [
						addMinutesToCairoDatetime(bucketStart, 60),
						addMinutesToCairoDatetime(bucketStart, 120),
						addMinutesToCairoDatetime(bucketStart, 180),
					];
				} else {
					expectedDatetimes = [
						bucketStart,
						addMinutesToCairoDatetime(bucketStart, 60),
						addMinutesToCairoDatetime(bucketStart, 120),
						addMinutesToCairoDatetime(bucketStart, 180),
					];
				}

				const actualDatetimes = rows.map((c) => c.datetime);
				const complete =
					rows.length === expectedDatetimes.length &&
					expectedDatetimes.every(
						(value, index) =>
							actualDatetimes[index] === value,
					);

				if (!complete) {
					incompleteBuckets++;
					continue;
				}

				// The midnight 4H candle is only considered fully gap-aware
				// when its 01:00 source row was anchored to the previous close.
				if (parts.hour === 0 && rows[0].gap_adjusted !== true) {
					incompleteBuckets++;
					continue;
				}

				const expectedCloseTime =
					addMinutesToCairoDatetime(bucketStart, 240);
				const gapAdjusted = rows.some(
					(c) => c.gap_adjusted === true,
				);

				if (gapAdjusted) {
					gapAbsorbedBuckets++;
				}

				derived.push({
					timeframe: "4h",
					datetime: bucketStart,
					open_time: bucketStart,
					expected_close_time: expectedCloseTime,
					open: rows[0].open,
					high: Math.max(...rows.map((c) => c.high)),
					low: Math.min(...rows.map((c) => c.low)),
					close: rows[rows.length - 1].close,
					status:
						statusFromExpectedClose(
							expectedCloseTime,
						),
					source: "derived_1h_gap_aware",
					provisional: false,
					confirmed: true,
					synthetic_gap: false,
					gap_adjusted: gapAdjusted,
					stored_at_ms: Date.now(),
				});

				completeBuckets++;
			}

			// v14: diff/reconcile only changed 4H buckets; never wipe the full layer.
			const sourceFrom = source.length > 0
				? fourHourBucketStart(source[0].datetime)
				: null;
			const sourceTo = source.length > 0
				? fourHourBucketStart(source[source.length - 1].datetime)
				: null;
			const writeStats = await this.reconcileNormalizedRange(
				"4h",
				derived,
				sourceFrom,
				sourceTo,
			);

			const datetimes = derived
				.map((c) => c.datetime)
				.sort();

			const now = Date.now();

			const meta: Derived4HMeta = {
				timeframe: "4h",
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
				source_interval: "1h",
				source_rows_seen: source.length,
				source_regular_rows: regularRows.length,
				source_gap_adjusted_rows: sourceGapAdjustedRows,
				source_synthetic_gap_rows: 0,
				complete_4h_buckets: completeBuckets,
				incomplete_4h_buckets: incompleteBuckets,
				gap_absorbed_buckets: gapAbsorbedBuckets,
				absorbed_synthetic_gap_rows: 0,
				derived_rows_stored: derived.length,
				pending_closed_period:
					sourceMeta?.pending_closed_period ?? false,
				layer: "analysis_normalized",
			};

			await this.ctx.storage.put(
				normalizedMetaKey("4h"),
				meta,
			);

			return {
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval: "4h",
				interval_minutes: 240,
				layer: "analysis_normalized",
				derived_from: "normalized_1h",
				gap_policy:
					"No standalone gap candle. The first real candle after a closure opens at the previous valid close; the 00:00-04:00 4H bucket then contains that adjusted 01:00 candle plus 02:00 and 03:00.",
				source_rows_seen: source.length,
				source_regular_rows: regularRows.length,
				source_gap_adjusted_rows: sourceGapAdjustedRows,
				source_synthetic_gap_rows: 0,
				complete_4h_buckets: completeBuckets,
				incomplete_4h_buckets: incompleteBuckets,
				gap_absorbed_buckets: gapAbsorbedBuckets,
				absorbed_synthetic_gap_rows: 0,
				pending_closed_period:
					meta.pending_closed_period,
				normalized_rows_stored: derived.length,
				rows_written: writeStats.written,
				rows_unchanged_skipped: writeStats.unchanged,
				rows_deleted: writeStats.deleted,
				oldest_datetime: meta.oldest_datetime,
				latest_datetime: meta.latest_datetime,
				analysis_performed: false,
				note:
					"4h is deterministic OHLC aggregation from normalized 1h. The closure/reopen gap is part of the first real reopening candle, never a separate row. The Monday 00:00-04:00 candle uses the same rule with the weekend gap. No market-analysis logic is performed.",
			};
		} catch (error) {
			return {
				status: "error",
				interval: "4h",
				error:
					error instanceof Error
						? error.message
						: String(error),
			};
		}
	}


	private objectName() {
		return ((this.ctx.id as unknown as { name?: string | null }).name ?? null);
	}

	private isProductionInstance() {
		return this.objectName() === PROD_OBJECT_NAME;
	}

	private isRetiredInstance() {
		const name = this.objectName();
		return name !== null &&
			(RETIRED_OBJECT_NAMES as readonly string[]).includes(name);
	}

	private utcBudgetDay(ms = Date.now()) {
		return new Date(ms).toISOString().slice(0, 10);
	}

	private newRecoveryBudget(now = Date.now()): RecoveryBudgetState {
		return {
			utc_day: this.utcBudgetDay(now),
			estimated_used_rows: 0,
			soft_limit_rows: RECOVERY_SOFT_LIMIT_ROWS,
			manual_extra_rows: 0,
			hard_ceiling_rows: RECOVERY_HARD_CEILING_ROWS,
			last_update_ms: now,
			last_reason: null,
		};
	}

	private normalizedRecoveryBudget(
		stored: RecoveryBudgetState | null,
		now = Date.now(),
	) {
		const day = this.utcBudgetDay(now);
		if (!stored || stored.utc_day !== day) {
			return this.newRecoveryBudget(now);
		}
		return {
			...stored,
			soft_limit_rows: RECOVERY_SOFT_LIMIT_ROWS,
			hard_ceiling_rows: RECOVERY_HARD_CEILING_ROWS,
			manual_extra_rows: Math.max(
				0,
				Math.min(
					stored.manual_extra_rows ?? 0,
					RECOVERY_HARD_CEILING_ROWS - RECOVERY_SOFT_LIMIT_ROWS,
				),
			),
		};
	}

	private async recoveryBudgetStatus() {
		const stored =
			(await this.ctx.storage.get<RecoveryBudgetState>(
				RECOVERY_BUDGET_KEY,
			)) ?? null;
		const budget = this.normalizedRecoveryBudget(stored);
		const effectiveLimit = Math.min(
			budget.hard_ceiling_rows,
			budget.soft_limit_rows + budget.manual_extra_rows,
		);
		return {
			...budget,
			effective_limit_rows: effectiveLimit,
			remaining_estimated_rows: Math.max(
				0,
				effectiveLimit - budget.estimated_used_rows,
			),
			paused: budget.estimated_used_rows >= effectiveLimit,
			note:
				"Internal conservative recovery budget only. Live/current writes continue when recovery pauses.",
		};
	}

	private isRecoveryWork(item: AutoQueueItem) {
		return (
			item.reason.includes("bootstrap") ||
			item.reason.includes("backfill") ||
			item.reason.includes("recovery")
		);
	}

	private estimateRecoveryWriteCost(item: AutoQueueItem) {
		return Math.max(
			100,
			Math.ceil(item.outputsize * RECOVERY_COST_MULTIPLIER + 100),
		);
	}

	private async canRunRecoveryItem(item: AutoQueueItem) {
		const budget = await this.recoveryBudgetStatus();
		const estimatedCost = this.estimateRecoveryWriteCost(item);
		return {
			allowed:
				budget.estimated_used_rows + estimatedCost <=
				budget.effective_limit_rows,
			estimated_cost_rows: estimatedCost,
			budget,
		};
	}

	private async consumeRecoveryBudget(
		estimatedRows: number,
		reason: string,
	) {
		const stored =
			(await this.ctx.storage.get<RecoveryBudgetState>(
				RECOVERY_BUDGET_KEY,
			)) ?? null;
		const budget = this.normalizedRecoveryBudget(stored);
		budget.estimated_used_rows = Math.min(
			budget.hard_ceiling_rows,
			budget.estimated_used_rows + Math.max(0, estimatedRows),
		);
		budget.last_update_ms = Date.now();
		budget.last_reason = reason;
		await this.ctx.storage.put(RECOVERY_BUDGET_KEY, budget);
		return this.recoveryBudgetStatus();
	}

	private async addRecoveryBudget(addRows: number) {
		const stored =
			(await this.ctx.storage.get<RecoveryBudgetState>(
				RECOVERY_BUDGET_KEY,
			)) ?? null;
		const budget = this.normalizedRecoveryBudget(stored);
		const requested = Math.max(
			0,
			Math.min(
				Math.floor(addRows),
				RECOVERY_MAX_MANUAL_ADD_ROWS,
			),
		);
		const maximumExtra =
			RECOVERY_HARD_CEILING_ROWS - RECOVERY_SOFT_LIMIT_ROWS;
		budget.manual_extra_rows = Math.min(
			maximumExtra,
			budget.manual_extra_rows + requested,
		);
		budget.last_update_ms = Date.now();
		budget.last_reason = `manual_override_plus_${requested}`;
		await this.ctx.storage.put(RECOVERY_BUDGET_KEY, budget);
		return this.recoveryBudgetStatus();
	}

	private enqueueBootstrapPlan() {
		this.ensureAutoState();
		for (const interval of REST_INTERVALS) {
			this.enqueueAutoIntervals(
				[interval],
				BOOTSTRAP_OUTPUTSIZE_BY_INTERVAL[interval],
				"bootstrap",
			);
		}
		this.autoState!.bootstrap_pending = true;
	}

	private async retireThisInstance() {
		this.enabled = false;
		this.stopReconnectTimer();
		this.closeSocket("retired durable object");
		if (this.autoState) {
			this.autoState.enabled = false;
			this.autoState.queue = [];
			this.autoState.bootstrap_pending = false;
		}
		try {
			await this.ctx.storage.deleteAlarm();
		} catch {}
		return {
			status: "retired",
			build_version: BUILD_VERSION,
			object_name: this.objectName(),
			alarm_deleted: (await this.ctx.storage.getAlarm()) === null,
		};
	}

	private createInitialAutoState(
		nowCairo: string,
		nowMs: number,
	): AutoRefreshState {
		return {
			enabled: false,
			queue: [],
			last_5m_key: scheduleBucketKey(nowCairo, 5),
			last_15m_key: scheduleBucketKey(nowCairo, 15),
			last_hour_key: scheduleBucketKey(nowCairo, 60),
			last_4h_key: fourHourScheduleKey(nowCairo),
			last_day_key: dateKey(nowCairo),
			last_week_close_key: null,
			last_month_key: monthKey(nowCairo),
			rate_window_start_ms: nowMs,
			rate_requests: 0,
			last_alarm_ms: null,
			last_success_ms: null,
			last_error: null,
			bootstrap_pending: false,
		};
	}

	private ensureAutoState() {
		if (!this.autoState) {
			const now = Date.now();
			this.autoState = this.createInitialAutoState(
				cairoTime(now),
				now,
			);
		}
	}

	private enqueueAutoIntervals(
		intervals: readonly RestInterval[],
		outputsize: number,
		reason: string,
	) {
		this.ensureAutoState();
		const state = this.autoState!;
		const now = Date.now();

		for (const interval of intervals) {
			const existing = state.queue.find(
				(item) => item.interval === interval,
			);

			if (existing) {
				existing.outputsize = Math.max(
					existing.outputsize,
					outputsize,
				);
				if (!existing.reason.includes(reason)) {
					existing.reason += `|${reason}`;
				}
				continue;
			}

			state.queue.push({
				interval,
				outputsize,
				reason,
				enqueued_ms: now,
				attempts: 0,
			});
		}

		const priority: RestInterval[] = [
			"1month",
			"1week",
			"1day",
			"4h",
			"1h",
			"30min",
			"15min",
			"5min",
			"1min",
		];

		state.queue.sort(
			(a, b) =>
				priority.indexOf(a.interval) -
				priority.indexOf(b.interval),
		);
	}

	private async ensureStorageMigration() {
		const current =
			(await this.ctx.storage.get<string>(STORAGE_MIGRATION_KEY)) ?? null;

		if (current === STORAGE_MIGRATION_VERSION) {
			return {
				status: "ok",
				version: current,
				migrated: false,
			};
		}

		const results: unknown[] = [];

		for (const interval of HIGHER_NATIVE_INTERVALS) {
			// A previous build may have fetched the higher timeframe but failed
			// to normalize it because Twelve Data used date-only timestamps.
			// Prefer the already-stored raw layer; only fetch when that layer
			// is genuinely missing. Missing required history is an allowed REST
			// refresh under the project rules.
			let rawProbe = await this.ctx.storage.list<StoredHistoricalCandle>({
				prefix: historicalPrefix(interval),
				reverse: true,
				limit: 1,
			});

			let repairedFromRest = false;
			if (rawProbe.size === 0) {
				const sync = await this.syncHistoricalInterval(
					interval,
					AUTO_BOOTSTRAP_OUTPUTSIZE,
					null,
				);
				results.push({ interval, stage: "raw_repair", result: sync });

				if (sync.status !== "ok") {
					return {
						status: "error",
						version: current,
						migrated: false,
						failed_interval: interval,
						failed_stage: "raw_repair",
						results,
					};
				}
				repairedFromRest = true;
				rawProbe = await this.ctx.storage.list<StoredHistoricalCandle>({
					prefix: historicalPrefix(interval),
					reverse: true,
					limit: 1,
				});
			}

			const normalized = await this.normalizeHigherNativeInterval(interval);
			results.push({
				interval,
				stage: "normalize",
				repaired_from_rest: repairedFromRest,
				raw_present: rawProbe.size > 0,
				result: normalized,
			});

			if (normalized.status !== "ok") {
				return {
					status: "error",
					version: current,
					migrated: false,
					failed_interval: interval,
					failed_stage: "normalize",
					results,
				};
			}
		}

		await this.ctx.storage.put(
			STORAGE_MIGRATION_KEY,
			STORAGE_MIGRATION_VERSION,
		);

		return {
			status: "ok",
			version: STORAGE_MIGRATION_VERSION,
			migrated: true,
			results,
		};
	}

	private publicAutoState() {
		this.ensureAutoState();
		const state = this.autoState!;
		return {
			enabled: state.enabled,
			queue_length: state.queue.length,
			queue: state.queue.map((item) => ({
				interval: item.interval,
				outputsize: item.outputsize,
				reason: item.reason,
				before: item.before ?? null,
				attempts: item.attempts,
			})),
			bootstrap_pending: state.bootstrap_pending,
			recovery_pending: state.queue.some((item) =>
				item.reason.includes("recovery") || item.reason.includes("backfill"),
			),
			rate_requests_this_window: state.rate_requests,
			last_alarm_time:
				state.last_alarm_ms !== null
					? cairoTime(state.last_alarm_ms)
					: null,
			last_success_time:
				state.last_success_ms !== null
					? cairoTime(state.last_success_ms)
					: null,
			last_error: state.last_error,
			last_recovery_audit_time:
				state.last_recovery_audit_ms != null
					? cairoTime(state.last_recovery_audit_ms)
					: null,
			cadence: {
				"1min_rest": "every 5 minutes while market is open",
				"5min_rest": "every 15 minutes while market is open",
				"15min_rest": "every hour while market is open",
				"30min_rest": "every hour while market is open",
				"1h_rest": "every 4H close",
				"4h_rest": "every 4H close",
				"1day_rest": "after each trading-day close",
				"1week_rest": "after Friday trading closes / Saturday 00:00 Cairo",
				"1month_rest": "at the month boundary",
				"3min_rest": "never; derived from effective 1min",
			},
		};
	}

	private async persistAutoState() {
		this.ensureAutoState();
		try {
			await this.ctx.storage.put(AUTO_STATE_KEY, this.autoState!);
			return true;
		} catch (error) {
			this.lastError =
				error instanceof Error ? error.message : String(error);
			return false;
		}
	}

	private async scheduleAutoAlarm(delayMs = AUTO_ALARM_MS) {
		this.ensureAutoState();
		if (!this.autoState!.enabled) {
			return false;
		}
		try {
			await this.ctx.storage.setAlarm(Date.now() + delayMs);
			return true;
		} catch (error) {
			this.lastError =
				error instanceof Error ? error.message : String(error);
			return false;
		}
	}


	private async ensureAutoWatchdog() {
		this.ensureAutoState();
		const state = this.autoState!;
		if (!state.enabled) {
			return {
				enabled: false,
				rearmed: false,
				scheduled_alarm_ms: null as number | null,
			};
		}

		const now = Date.now();
		const scheduled = await this.ctx.storage.getAlarm();
		const stateStale =
			state.last_alarm_ms === null ||
			now - state.last_alarm_ms > AUTO_WATCHDOG_STALE_MS;
		const alarmMissingOrPast = scheduled === null || scheduled <= now;
		const alarmSuspiciouslyFar =
			stateStale && scheduled !== null && scheduled > now + 2 * AUTO_ALARM_MS;

		if (alarmMissingOrPast || alarmSuspiciouslyFar) {
			const next = now + 1_000;
			try {
				await this.ctx.storage.setAlarm(next);
				return {
					enabled: true,
					rearmed: true,
					scheduled_alarm_ms: next,
					write_error: null as string | null,
				};
			} catch (error) {
				const message =
					error instanceof Error ? error.message : String(error);
				this.lastError = message;
				return {
					enabled: true,
					rearmed: false,
					scheduled_alarm_ms: scheduled,
					write_error: message,
				};
			}
		}

		return {
			enabled: true,
			rearmed: false,
			scheduled_alarm_ms: scheduled,
		};
	}

	private continuityIntervalMinutes(interval: AnalysisNormalizedInterval) {
		if (interval === "3min") return 3;
		if (interval === "4h") return 240;
		if (interval === "1day") return 1440;
		if (interval === "1week") return 10080;
		if (interval === "1month") return 43200;
		return normalizedIntervalMinutes(interval as NormalizedRestInterval);
	}

	private countMissingMarketBuckets(
		fromDatetime: string,
		toDatetime: string,
		interval: AnalysisNormalizedInterval,
	) {
		const minutes = this.continuityIntervalMinutes(interval);
		let cursor = addMinutesToCairoDatetime(fromDatetime, minutes);
		let first: string | null = null;
		let last: string | null = null;
		let count = 0;
		let guard = 0;

		while (cursor < toDatetime && guard < 20_000) {
			guard++;
			if (!isClosedMarketCairoDatetime(cursor)) {
				if (first === null) first = cursor;
				last = cursor;
				count++;
			}
			cursor = addMinutesToCairoDatetime(cursor, minutes);
		}

		return { first, last, count };
	}

	private async effectiveRowsForContinuity(
		interval: AnalysisNormalizedInterval,
	) {
		const confirmedPage =
			await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix(interval),
				reverse: true,
				limit: CONTINUITY_SCAN_CONFIRMED_LIMIT,
			});
		const provisional = await this.buildProvisionalTail(interval);
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of provisional) merged.set(candle.datetime, candle);
		// Confirmed REST/derived-confirmed rows remain authoritative on overlap.
		for (const candle of confirmedPage.values()) merged.set(candle.datetime, candle);
		return Array.from(merged.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private isNonBlockingBoundaryOmission(gap: ContinuityGap) {
		// Twelve Data can occasionally omit the final 1M candle immediately
		// before the declared 00:00 Cairo daily closure. The derived 3M bucket
		// ending at that same boundary can then be incomplete as a consequence.
		// Record that omission, but do not invent/synthesize it and do not let it
		// block an otherwise complete current-session analysis.
		if (gap.interval !== "1min" && gap.interval !== "3min") return false;
		if (gap.missing_buckets !== 1) return false;

		const duration = this.continuityIntervalMinutes(gap.interval);
		const missingExpectedClose = addMinutesToCairoDatetime(
			gap.missing_to,
			duration,
		);

		return (
			isClosedMarketCairoDatetime(missingExpectedClose) &&
			hasDeclaredClosureBetween(missingExpectedClose, gap.after_datetime)
		);
	}

	private async auditContinuity(
		interval: AnalysisNormalizedInterval,
	): Promise<ContinuityAudit> {
		const rows = await this.effectiveRowsForContinuity(interval);
		const confirmed = rows.filter((row) => row.confirmed === true);
		const provisional = rows.filter((row) => row.provisional === true);
		let gap: ContinuityGap | null = null;
		let boundaryOmission: ContinuityGap | null = null;

		// Find the newest blocking market-hours gap. Normal daily/weekend closure
		// buckets are ignored by countMissingMarketBuckets(). A single known
		// 1M/derived-3M boundary omission is recorded separately and scanning
		// continues so it can never hide an older real gap.
		for (let i = rows.length - 2; i >= 0; i--) {
			const before = rows[i];
			const after = rows[i + 1];
			const missing = this.countMissingMarketBuckets(
				before.datetime,
				after.datetime,
				interval,
			);
			if (missing.count > 0 && missing.first && missing.last) {
				const candidate: ContinuityGap = {
					interval,
					before_datetime: before.datetime,
					missing_from: missing.first,
					missing_to: missing.last,
					after_datetime: after.datetime,
					missing_buckets: missing.count,
					missing_market_minutes:
						missing.count * this.continuityIntervalMinutes(interval),
				};

				if (this.isNonBlockingBoundaryOmission(candidate)) {
					if (boundaryOmission === null) boundaryOmission = candidate;
					continue;
				}

				gap = candidate;
				break;
			}
		}

		const latest = rows.length > 0 ? rows[rows.length - 1] : null;
		const latestMs = latest ? cairoDatetimeToMs(latest.datetime) : null;
		const allowedLagMs = Math.max(
			5 * 60_000,
			this.continuityIntervalMinutes(interval) * 2 * 60_000,
		);
		const effectiveFresh =
			isClosedMarketCairoDatetime(cairoTime(Date.now())) ||
			(latestMs !== null && Date.now() - latestMs <= allowedLagMs);

		return {
			interval,
			latest_effective_datetime: latest?.datetime ?? null,
			latest_confirmed_datetime:
				confirmed.length > 0 ? confirmed[confirmed.length - 1].datetime : null,
			current_provisional_datetime:
				provisional.length > 0
					? provisional[provisional.length - 1].datetime
					: null,
			gap,
			boundary_omission: boundaryOmission,
			effective_fresh: effectiveFresh,
		};
	}

	private recoveryOutputsizeForGap(
		interval: RecoveryInterval,
		gap: ContinuityGap,
	) {
		return Math.min(
			RECOVERY_MAX_OUTPUTSIZE,
			Math.max(
				AUTO_INCREMENTAL_OUTPUTSIZE,
				gap.missing_buckets + RECOVERY_BUFFER_ROWS,
			),
		);
	}

	private recoveryOutputsizeForStaleness(
		interval: RecoveryInterval,
		latestConfirmed: string | null,
	) {
		if (!latestConfirmed) return AUTO_BOOTSTRAP_OUTPUTSIZE;
		const startMs = cairoDatetimeToMs(latestConfirmed);
		if (startMs === null) return AUTO_BOOTSTRAP_OUTPUTSIZE;
		const elapsedMinutes = Math.max(0, Math.ceil((Date.now() - startMs) / 60_000));
		const duration = this.continuityIntervalMinutes(interval);
		return Math.min(
			RECOVERY_MAX_OUTPUTSIZE,
			Math.max(
				AUTO_INCREMENTAL_OUTPUTSIZE,
				Math.ceil(elapsedMinutes / duration) + RECOVERY_BUFFER_ROWS,
			),
		);
	}

	private enqueueAutoRepair(
		interval: RecoveryInterval,
		outputsize: number,
		before: string | null,
		reason: string,
	) {
		this.ensureAutoState();
		const state = this.autoState!;
		const existing = state.queue.find(
			(item) => item.interval === interval && (item.before ?? null) === before,
		);
		if (existing) {
			existing.outputsize = Math.max(existing.outputsize, outputsize);
			if (!existing.reason.includes(reason)) existing.reason += `|${reason}`;
			return;
		}
		state.queue.push({
			interval,
			outputsize: Math.min(RECOVERY_MAX_OUTPUTSIZE, Math.max(1, outputsize)),
			reason,
			enqueued_ms: Date.now(),
			attempts: 0,
			before,
		});
	}

	private async enqueueStaleRecovery(nowMs: number) {
		this.ensureAutoState();
		this.autoState!.last_recovery_audit_ms = nowMs;
		if (isClosedMarketCairoDatetime(cairoTime(nowMs))) return;
		for (const interval of RECOVERY_INTERVALS) {
			const audit = await this.auditContinuity(interval);
			if (audit.gap) {
				this.enqueueAutoRepair(
					interval,
					this.recoveryOutputsizeForGap(interval, audit.gap),
					audit.gap.after_datetime,
					"auto_recovery_backfill",
				);
				continue;
			}
			if (!audit.effective_fresh) {
				this.enqueueAutoRepair(
					interval,
					this.recoveryOutputsizeForStaleness(
						interval,
						audit.latest_confirmed_datetime,
					),
					null,
					"auto_recovery_backfill",
				);
			}
		}

		// 4H is derived from confirmed 1H. A missing closed 4H bucket can exist
		// even when 1H Effective looks fresh because the newest hours are still
		// provisional. In that case force a targeted 1H REST refresh ending at
		// the next existing 4H bucket, then the normal post-refresh derivation
		// rebuilds 4H from authoritative 1H history.
		const fourHourAudit = await this.auditContinuity("4h");
		if (fourHourAudit.gap) {
			const oneHourAudit = await this.auditContinuity("1h");
			this.enqueueAutoRepair(
				"1h",
				this.recoveryOutputsizeForStaleness(
					"1h",
					oneHourAudit.latest_confirmed_datetime,
				),
				fourHourAudit.gap.after_datetime,
				"auto_recovery_4h_source_backfill",
			);
		}
	}


	private async ensureGoldDataReady() {
		try {
			const watchdog = await this.ensureAutoWatchdog();
			await this.ensureConnection();
			this.ensureAutoState();

			const beforeAudits: ContinuityAudit[] = [];
			for (const interval of RECOVERY_INTERVALS) {
				const audit = await this.auditContinuity(interval);
				beforeAudits.push(audit);
				if (audit.gap) {
					this.enqueueAutoRepair(
						interval,
						this.recoveryOutputsizeForGap(interval, audit.gap),
						audit.gap.after_datetime,
						"gpt_recovery_backfill",
					);
				} else if (!audit.effective_fresh) {
					this.enqueueAutoRepair(
						interval,
						this.recoveryOutputsizeForStaleness(
							interval,
							audit.latest_confirmed_datetime,
						),
						null,
						"gpt_recovery_backfill",
					);
				}
			}

			// Derived 3M is audited explicitly but repaired locally from 1M.
			const threeMinuteBefore = await this.auditContinuity("3min");
			beforeAudits.push(threeMinuteBefore);
			if (
				threeMinuteBefore.gap !== null ||
				!threeMinuteBefore.effective_fresh
			) {
				const oneMinuteBefore =
					beforeAudits.find((audit) => audit.interval === "1min") ??
					await this.auditContinuity("1min");
				this.enqueueAutoRepair(
					"1min",
					this.recoveryOutputsizeForStaleness(
						"1min",
						oneMinuteBefore.latest_confirmed_datetime,
					),
					threeMinuteBefore.gap?.after_datetime ?? null,
					"gpt_recovery_3m_source_backfill",
				);
			}

			// 4H is derived from confirmed 1H. If a closed 4H bucket is missing,
			// force a targeted authoritative 1H refresh around that boundary.
			const fourHourBefore = await this.auditContinuity("4h");
			beforeAudits.push(fourHourBefore);
			if (fourHourBefore.gap) {
				const oneHourBefore =
					beforeAudits.find((audit) => audit.interval === "1h") ??
					await this.auditContinuity("1h");
				this.enqueueAutoRepair(
					"1h",
					this.recoveryOutputsizeForStaleness(
						"1h",
						oneHourBefore.latest_confirmed_datetime,
					),
					fourHourBefore.gap.after_datetime,
					"gpt_recovery_4h_source_backfill",
				);
			}

			const repairRequested = beforeAudits.some(
				(audit) => audit.gap !== null || !audit.effective_fresh,
			);

			const queueHasWork = this.autoState!.queue.length > 0;
			if (repairRequested || queueHasWork) {
				await this.persistAutoState();
				await this.processAutoQueue();
				await this.scheduleAutoAlarm();
			}

			const afterAudits: ContinuityAudit[] = [];
			for (const interval of [
				"1min",
				"3min",
				"5min",
				"15min",
				"30min",
				"1h",
				"4h",
			] as AnalysisNormalizedInterval[]) {
				afterAudits.push(await this.auditContinuity(interval));
			}

			const remainingProblems = afterAudits.filter(
				(audit) => audit.gap !== null || !audit.effective_fresh,
			);
			const analysisReady =
				isClosedMarketCairoDatetime(cairoTime(Date.now()))
					? remainingProblems.every((audit) => audit.gap === null)
					: remainingProblems.length === 0;

			return {
				status: "ok",
				build_version: BUILD_VERSION,
				symbol: SYMBOL,
				timezone: TIMEZONE,
				watchdog,
				repair_requested: repairRequested,
				repair_status: analysisReady
					? "ready"
					: this.autoState!.queue.length > 0
						? "queued_or_rate_limited"
						: "partial",
				analysis_ready: analysisReady,
				before: beforeAudits,
				after: afterAudits,
				auto: this.publicAutoState(),
				write_policy: "read_first_diff_upsert",
			};
		} catch (error) {
			return {
				status: "error",
				build_version: BUILD_VERSION,
				error: error instanceof Error ? error.message : String(error),
				analysis_ready: false,
			};
		}
	}

	private updateScheduleAndEnqueue(nowMs: number) {
		this.ensureAutoState();
		const state = this.autoState!;
		const nowCairo = cairoTime(nowMs);
		const parts = parseCairoDatetimeParts(nowCairo);
		if (!parts) return;

		const marketClosed = isClosedMarketCairoDatetime(nowCairo);
		const fiveKey = scheduleBucketKey(nowCairo, 5);
		const fifteenKey = scheduleBucketKey(nowCairo, 15);
		const hourKey = scheduleBucketKey(nowCairo, 60);
		const fourKey = fourHourScheduleKey(nowCairo);
		const currentDateKey = dateKey(nowCairo);
		const currentMonthKey = monthKey(nowCairo);

		if (fiveKey !== state.last_5m_key) {
			state.last_5m_key = fiveKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"5m_cadence",
				);
			}
		}

		if (fifteenKey !== state.last_15m_key) {
			state.last_15m_key = fifteenKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["5min", "1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"15m_cadence",
				);
			}
		}

		if (hourKey !== state.last_hour_key) {
			state.last_hour_key = hourKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["30min", "15min", "5min", "1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"hour_cadence",
				);
			}
		}

		if (fourKey !== state.last_4h_key) {
			state.last_4h_key = fourKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["4h", "1h", "30min", "15min", "5min", "1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"4h_close",
				);
			}
		}

		if (currentDateKey !== state.last_day_key) {
			const currentDayStart = dayStartCairo(nowCairo);
			const currentDayStartMs = cairoDatetimeToMs(currentDayStart);
			const previousDay =
				currentDayStartMs !== null
					? cairoTime(currentDayStartMs - 24 * 60 * 60 * 1000)
					: null;

			state.last_day_key = currentDateKey;

			if (previousDay && isTradingDayCairo(previousDay)) {
				this.enqueueAutoIntervals(
					["1day", "4h", "1h", "30min", "15min", "5min", "1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"day_close",
				);
			}
		}

		const dayOfWeek = cairoDayOfWeek(nowCairo);
		if (dayOfWeek === 6 && parts.hour === 0) {
			const weeklyKey = currentDateKey;
			if (state.last_week_close_key !== weeklyKey) {
				state.last_week_close_key = weeklyKey;
				this.enqueueAutoIntervals(
					["1week", "1day", "4h", "1h", "30min", "15min", "5min", "1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"week_close",
				);
			}
		}

		if (currentMonthKey !== state.last_month_key) {
			state.last_month_key = currentMonthKey;
			this.enqueueAutoIntervals(
				["1month", "1week", "1day", "4h", "1h", "30min", "15min", "5min", "1min"],
				AUTO_INCREMENTAL_OUTPUTSIZE,
				"month_close",
			);
		}
	}

	private async postAutoRefresh(item: AutoQueueItem) {
		const bootstrap = item.reason.includes("bootstrap") || item.reason.includes("backfill");

		if (isNormalizedRestInterval(item.interval)) {
			if (bootstrap) {
				await this.normalizeStoredInterval(item.interval);
			} else {
				await this.normalizeRecentStoredInterval(item.interval);
			}

			if (item.interval === "1min") {
				if (bootstrap) {
					await this.deriveThreeMinuteFromOneMinute();
				} else {
					await this.deriveRecentThreeMinuteFromOneMinute();
				}
			}

			if (item.interval === "1h") {
				if (bootstrap) {
					await this.deriveFourHourFromOneHour();
				} else {
					await this.deriveRecentFourHourFromOneHour();
				}
			}
			return;
		}

		if (isHigherNativeInterval(item.interval)) {
			await this.normalizeHigherNativeInterval(item.interval);
		}
	}

	private async processAutoQueue() {
		this.ensureAutoState();
		const state = this.autoState!;
		if (!state.enabled) return;

		const now = Date.now();
		if (now - state.rate_window_start_ms >= AUTO_RATE_WINDOW_MS) {
			state.rate_window_start_ms = now;
			state.rate_requests = 0;
		}

		while (
			state.queue.length > 0 &&
			state.rate_requests < AUTO_MAX_REQUESTS_PER_WINDOW
		) {
			let itemIndex = 0;
			let item = state.queue[itemIndex];
			let reservedRecoveryRows = 0;

			if (this.isRecoveryWork(item)) {
				const check = await this.canRunRecoveryItem(item);
				if (!check.allowed) {
					// Recovery pauses, but normal current-cadence refreshes are still
					// allowed to run so live/current operation is never frozen by the
					// 25K soft recovery budget.
					const liveIndex = state.queue.findIndex(
						(candidate) => !this.isRecoveryWork(candidate),
					);
					if (liveIndex < 0) {
						state.last_error =
							`RECOVERY_BUDGET_PAUSED: estimated ${check.estimated_cost_rows} rows would exceed internal limit ${check.budget.effective_limit_rows}.`;
						break;
					}
					itemIndex = liveIndex;
					item = state.queue[itemIndex];
				} else {
					reservedRecoveryRows = check.estimated_cost_rows;
				}
			}

			state.queue.splice(itemIndex, 1);
			state.rate_requests++;
			await this.persistAutoState();

			// Reserve the conservative estimate BEFORE any recovery writes happen.
			// If the operation later fails, the reservation is intentionally not
			// refunded; this biases the guard toward safety.
			if (reservedRecoveryRows > 0) {
				await this.consumeRecoveryBudget(
					reservedRecoveryRows,
					`${item.interval}:${item.reason}:reserved`,
				);
			}

			const result = await this.syncHistoricalInterval(
				item.interval,
				item.outputsize,
				item.before ?? null,
			);

			if (result.status === "ok") {
				try {
					await this.postAutoRefresh(item);
					state.last_success_ms = Date.now();
					state.last_error = null;
				} catch (error) {
					state.last_error =
						error instanceof Error
							? error.message
							: String(error);
				}
			} else {
				item.attempts++;
				state.last_error = String(
					(result as { error?: unknown }).error ??
						`Auto refresh failed for ${item.interval}`,
				);

				if (item.attempts < 3) {
					state.queue.push(item);
				}
			}
		}

		state.bootstrap_pending = state.queue.some((item) =>
			item.reason.includes("bootstrap"),
		);
		await this.persistAutoState();
	}

	async alarm() {
		try {
			if (this.isRetiredInstance()) {
				await this.retireThisInstance();
				return;
			}

			this.ensureAutoState();
			const state = this.autoState!;
			if (!state.enabled) {
				try {
					await this.ctx.storage.deleteAlarm();
				} catch {}
				return;
			}

			state.last_alarm_ms = Date.now();

			const recoveryAuditDue =
				state.last_recovery_audit_ms == null ||
				state.last_alarm_ms - state.last_recovery_audit_ms >=
					AUTO_RECOVERY_AUDIT_INTERVAL_MS;
			const restSuccessStale =
				state.last_success_ms == null ||
				state.last_alarm_ms - state.last_success_ms >= 10 * 60_000;

			if (recoveryAuditDue || restSuccessStale) {
				try {
					await this.enqueueStaleRecovery(state.last_alarm_ms);
				} catch (error) {
					state.last_error =
						error instanceof Error ? error.message : String(error);
				}
			}

			this.updateScheduleAndEnqueue(state.last_alarm_ms);
			await this.persistAutoState();

			try {
				await this.ensureConnection();
				await this.processAutoQueue();
			} catch (error) {
				state.last_error =
					error instanceof Error ? error.message : String(error);
				await this.persistAutoState();
			}

			const delay =
				state.queue.length > 0 &&
				state.rate_requests >= AUTO_MAX_REQUESTS_PER_WINDOW
					? Math.max(
						1_000,
						state.rate_window_start_ms +
							AUTO_RATE_WINDOW_MS +
							1_000 -
							Date.now(),
					)
					: AUTO_ALARM_MS;

			await this.scheduleAutoAlarm(delay);
		} catch (error) {
			this.lastError =
				error instanceof Error ? error.message : String(error);
			return;
		}
	}

	private async normalizeRecentStoredInterval(
		interval: NormalizedRestInterval,
	) {
		const durationMinutes = normalizedIntervalMinutes(interval);
		const page = await this.ctx.storage.list<StoredHistoricalCandle>({
			prefix: historicalPrefix(interval),
			reverse: true,
			limit: 160,
		});
		const raw = Array.from(page.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		if (raw.length === 0) return;

		const earliest = raw[0].datetime;
		const priorPage = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(interval),
			reverse: true,
			limit: 400,
		});
		const priorRows = Array.from(priorPage.values())
			.filter((c) => c.datetime < earliest)
			.sort((a, b) => b.datetime.localeCompare(a.datetime));
		let previous = priorRows[0] ?? null;

		let filtered = 0;
		let gapAdjustedRows = 0;
		let valid = 0;
		const candidates: NormalizedCandle[] = [];

		for (const candle of raw) {
			if (isClosedMarketCairoDatetime(candle.datetime)) {
				filtered++;
				continue;
			}

			const expectedCloseTime = addMinutesToCairoDatetime(
				candle.datetime,
				durationMinutes,
			);
			if (statusFromExpectedClose(expectedCloseTime) === "OPEN") continue;

			const gapAdjusted =
				previous !== null &&
				hasDeclaredClosureBetween(
					previous.expected_close_time,
					candle.datetime,
				);
			const open = gapAdjusted ? previous!.close : candle.open;
			const normalized: NormalizedCandle = {
				timeframe: interval,
				datetime: candle.datetime,
				open_time: candle.datetime,
				expected_close_time: expectedCloseTime,
				open,
				high: Math.max(candle.high, candle.open, open),
				low: Math.min(candle.low, candle.open, open),
				close: candle.close,
				status: "CLOSED",
				source: gapAdjusted
					? "historical_rest_gap_adjusted"
					: "historical_rest",
				provisional: false,
				confirmed: true,
				synthetic_gap: false,
				gap_adjusted: gapAdjusted,
				stored_at_ms: Date.now(),
			};
			candidates.push(normalized);
			previous = normalized;
			valid++;
			if (gapAdjusted) gapAdjustedRows++;
		}

		const writeStats = await this.upsertNormalizedCandidates(candidates);

		const newest = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(interval),
			reverse: true,
			limit: 1,
		});
		const oldest = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(interval),
			limit: 1,
		});
		const newestCandle = Array.from(newest.values())[0] ?? null;
		const oldestCandle = Array.from(oldest.values())[0] ?? null;
		const now = Date.now();
		const meta: NormalizedMeta = {
			timeframe: interval,
			last_normalized_ms: now,
			last_normalized_time: cairoTime(now),
			latest_datetime: newestCandle?.datetime ?? null,
			oldest_datetime: oldestCandle?.datetime ?? null,
			raw_rows_seen: raw.length,
			valid_raw_rows: valid,
			filtered_closed_rows: filtered,
			gap_adjusted_rows: gapAdjustedRows,
			synthetic_gap_rows: 0,
			pending_closed_period:
				isClosedMarketCairoDatetime(cairoTime(now)),
			layer: "analysis_normalized",
		};
		await this.ctx.storage.put(normalizedMetaKey(interval), meta);

		return {
			status: "ok",
			interval,
			rows_written: writeStats.written,
			rows_unchanged_skipped: writeStats.unchanged,
			write_policy: "diff_upsert_only",
		};
	}


	private async deriveRecentThreeMinuteFromOneMinute() {
		// Only a small tail is needed during routine 1M refreshes. Recovery uses
		// the full diff-based derivation instead.
		const page = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix("1min"),
			reverse: true,
			limit: 30,
		});
		const source = Array.from(page.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		if (source.length === 0) return;

		const buckets = new Map<string, NormalizedCandle[]>();
		for (const candle of source) {
			const bucketStart = threeMinuteBucketStart(candle.datetime);
			if (!bucketStart) continue;
			const rows = buckets.get(bucketStart) ?? [];
			rows.push(candle);
			buckets.set(bucketStart, rows);
		}

		const candidates: NormalizedCandle[] = [];
		for (const [bucketStart, rowsInput] of buckets) {
			const rows = rowsInput.slice().sort((a, b) =>
				a.datetime.localeCompare(b.datetime),
			);
			const expected = [
				bucketStart,
				addMinutesToCairoDatetime(bucketStart, 1),
				addMinutesToCairoDatetime(bucketStart, 2),
			];
			if (
				rows.length !== 3 ||
				!expected.every((value, index) => rows[index]?.datetime === value)
			) {
				continue;
			}

			const expectedClose = addMinutesToCairoDatetime(bucketStart, 3);
			if (statusFromExpectedClose(expectedClose) === "OPEN") continue;

			candidates.push({
				timeframe: "3min",
				datetime: bucketStart,
				open_time: bucketStart,
				expected_close_time: expectedClose,
				open: rows[0].open,
				high: Math.max(...rows.map((c) => c.high)),
				low: Math.min(...rows.map((c) => c.low)),
				close: rows[2].close,
				status: "CLOSED",
				source: "derived_1min",
				provisional: false,
				confirmed: true,
				synthetic_gap: false,
				gap_adjusted: rows.some((c) => c.gap_adjusted),
				stored_at_ms: Date.now(),
			});
		}

		return await this.upsertNormalizedCandidates(candidates);
	}

	private async deriveRecentFourHourFromOneHour() {
		// Routine 1H refresh only needs the most recent few 4H buckets.
		const page = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix("1h"),
			reverse: true,
			limit: 16,
		});
		const source = Array.from(page.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		if (source.length === 0) return;

		const buckets = new Map<string, NormalizedCandle[]>();
		for (const candle of source) {
			const bucketStart = fourHourBucketStart(candle.datetime);
			if (!bucketStart) continue;
			const rows = buckets.get(bucketStart) ?? [];
			rows.push(candle);
			buckets.set(bucketStart, rows);
		}

		const candidates: NormalizedCandle[] = [];
		for (const [bucketStart, rowsInput] of buckets) {
			const parts = parseCairoDatetimeParts(bucketStart);
			if (!parts) continue;
			const rows = rowsInput.slice().sort((a, b) =>
				a.datetime.localeCompare(b.datetime),
			);
			const expected = parts.hour === 0
				? [
					addMinutesToCairoDatetime(bucketStart, 60),
					addMinutesToCairoDatetime(bucketStart, 120),
					addMinutesToCairoDatetime(bucketStart, 180),
				]
				: [
					bucketStart,
					addMinutesToCairoDatetime(bucketStart, 60),
					addMinutesToCairoDatetime(bucketStart, 120),
					addMinutesToCairoDatetime(bucketStart, 180),
				];
			if (
				rows.length !== expected.length ||
				!expected.every((value, index) => rows[index]?.datetime === value)
			) {
				continue;
			}
			if (parts.hour === 0 && rows[0].gap_adjusted !== true) continue;

			const expectedClose = addMinutesToCairoDatetime(bucketStart, 240);
			if (statusFromExpectedClose(expectedClose) === "OPEN") continue;
			candidates.push({
				timeframe: "4h",
				datetime: bucketStart,
				open_time: bucketStart,
				expected_close_time: expectedClose,
				open: rows[0].open,
				high: Math.max(...rows.map((c) => c.high)),
				low: Math.min(...rows.map((c) => c.low)),
				close: rows[rows.length - 1].close,
				status: "CLOSED",
				source: "derived_1h_gap_aware",
				provisional: false,
				confirmed: true,
				synthetic_gap: false,
				gap_adjusted: rows.some((c) => c.gap_adjusted),
				stored_at_ms: Date.now(),
			});
		}

		return await this.upsertNormalizedCandidates(candidates);
	}

	private async normalizeHigherNativeInterval(
		interval: HigherNativeInterval,
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
				if (page.size === 0) break;
				raw.push(...page.values());
				if (page.size < 1000) break;
				const keys = Array.from(page.keys()) as string[];
				startAfter = keys[keys.length - 1];
			}

			raw.sort((a, b) => a.datetime.localeCompare(b.datetime));
			if (raw.length === 0) {
				return {
					status: "error",
					interval,
					error: `No stored ${interval} raw REST candles found`,
				};
			}

			const normalized: NormalizedCandle[] = [];
			let filteredClosedRows = 0;
			let gapAdjustedRows = 0;
			let previous: NormalizedCandle | null = null;

			for (const candle of raw) {
				const canonicalOpenTime = canonicalCairoDatetime(candle.datetime);
				if (!canonicalOpenTime) {
					filteredClosedRows++;
					continue;
				}

				if (
					interval === "1day" &&
					!isTradingDayCairo(canonicalOpenTime)
				) {
					filteredClosedRows++;
					continue;
				}

				const expectedCloseTime = expectedCloseForHigherInterval(
					interval,
					canonicalOpenTime,
				);
				if (statusFromExpectedClose(expectedCloseTime) === "OPEN") {
					continue;
				}

				// Project rule for higher native candles:
				// the new candle owns any price discontinuity from the previous
				// confirmed close. No standalone gap candle is ever created.
				const gapAdjusted = previous !== null;
				const open = gapAdjusted ? previous!.close : candle.open;
				const row: NormalizedCandle = {
					timeframe: interval,
					datetime: canonicalOpenTime,
					open_time: canonicalOpenTime,
					expected_close_time: expectedCloseTime,
					open,
					high: Math.max(candle.high, candle.open, open),
					low: Math.min(candle.low, candle.open, open),
					close: candle.close,
					status: "CLOSED",
					source: gapAdjusted
						? "historical_rest_gap_adjusted"
						: "historical_rest",
					provisional: false,
					confirmed: true,
					synthetic_gap: false,
					gap_adjusted: gapAdjusted,
					stored_at_ms: Date.now(),
				};
				normalized.push(row);
				previous = row;
				if (gapAdjusted) gapAdjustedRows++;
			}

			const sourceFrom = normalized.length > 0
				? normalized[0].datetime
				: null;
			const sourceTo = normalized.length > 0
				? normalized[normalized.length - 1].datetime
				: null;
			const writeStats = await this.reconcileNormalizedRange(
				interval,
				normalized,
				sourceFrom,
				sourceTo,
			);

			const now = Date.now();
			const meta: NormalizedMeta = {
				timeframe: interval,
				last_normalized_ms: now,
				last_normalized_time: cairoTime(now),
				latest_datetime:
					normalized.length > 0
						? normalized[normalized.length - 1].datetime
						: null,
				oldest_datetime:
					normalized.length > 0 ? normalized[0].datetime : null,
				raw_rows_seen: raw.length,
				valid_raw_rows: normalized.length,
				filtered_closed_rows: filteredClosedRows,
				gap_adjusted_rows: gapAdjustedRows,
				synthetic_gap_rows: 0,
				pending_closed_period:
					isClosedMarketCairoDatetime(cairoTime(now)),
				layer: "analysis_normalized",
			};
			await this.ctx.storage.put(normalizedMetaKey(interval), meta);

			return {
				status: "ok",
				symbol: SYMBOL,
				timezone: TIMEZONE,
				interval,
				layer: "analysis_normalized",
				historical_source: "native_twelve_data_rest",
				raw_rows_seen: raw.length,
				filtered_closed_rows: filteredClosedRows,
				gap_adjusted_rows: gapAdjustedRows,
				synthetic_gap_rows: 0,
				normalized_rows_stored: normalized.length,
				rows_written: writeStats.written,
				rows_unchanged_skipped: writeStats.unchanged,
				rows_deleted: writeStats.deleted,
				oldest_datetime: meta.oldest_datetime,
				latest_datetime: meta.latest_datetime,
				analysis_performed: false,
				note:
					interval === "1week"
						? "Weekly confirmed history is native REST. Every new weekly candle absorbs the preceding weekend gap by opening at the previous confirmed weekly close."
						: interval === "1month"
							? "Monthly confirmed history is native REST. A month-opening price discontinuity is absorbed into the new monthly candle; gaps inside the month remain inside that same monthly candle."
							: "Daily confirmed history is native REST. Weekend provider rows are excluded and the next real daily candle absorbs the preceding closure gap. No standalone gap candle is created.",
			};
		} catch (error) {
			return {
				status: "error",
				interval,
				error:
					error instanceof Error ? error.message : String(error),
			};
		}
	}

	private liveCandleToNormalized(
		candle: ProvisionalCandle,
	): NormalizedCandle {
		const expectedClose = addMinutesToCairoDatetime(candle.datetime, 1);
		return {
			timeframe: "1min",
			datetime: candle.datetime,
			open_time: candle.datetime,
			expected_close_time: expectedClose,
			open: candle.open,
			high: candle.high,
			low: candle.low,
			close: candle.close,
			status: statusFromExpectedClose(expectedClose),
			source: "websocket_ticks",
			provisional: true,
			confirmed: false,
			synthetic_gap: false,
			gap_adjusted: candle.gap_adjusted,
			stored_at_ms: Date.now(),
		};
	}

	private async getEffectiveOneMinuteRows(
		startDatetime: string,
		endDatetime: string,
	) {
		const confirmedPage =
			await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix("1min"),
				reverse: true,
				limit: 600,
			});

		const merged = new Map<string, NormalizedCandle>();
		for (const candle of confirmedPage.values()) {
			if (
				candle.datetime >= startDatetime &&
				candle.datetime <= endDatetime
			) {
				merged.set(candle.datetime, candle);
			}
		}

		for (const live of this.candles) {
			if (
				live.datetime >= startDatetime &&
				live.datetime <= endDatetime &&
				!merged.has(live.datetime)
			) {
				merged.set(live.datetime, this.liveCandleToNormalized(live));
			}
		}

		if (
			this.currentCandle &&
			this.currentCandle.datetime >= startDatetime &&
			this.currentCandle.datetime <= endDatetime &&
			!merged.has(this.currentCandle.datetime)
		) {
			merged.set(
				this.currentCandle.datetime,
				this.liveCandleToNormalized(this.currentCandle),
			);
		}

		return Array.from(merged.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private aggregateRows(
		interval: AnalysisNormalizedInterval,
		bucketStart: string,
		expectedClose: string,
		rows: NormalizedCandle[],
		source: string,
	): NormalizedCandle | null {
		if (rows.length === 0) return null;
		const sorted = rows.slice().sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		return {
			timeframe: interval,
			datetime: bucketStart,
			open_time: bucketStart,
			expected_close_time: expectedClose,
			open: sorted[0].open,
			high: Math.max(...sorted.map((c) => c.high)),
			low: Math.min(...sorted.map((c) => c.low)),
			close: sorted[sorted.length - 1].close,
			status: statusFromExpectedClose(expectedClose),
			source,
			provisional: true,
			confirmed: false,
			synthetic_gap: false,
			gap_adjusted: sorted.some((c) => c.gap_adjusted),
			stored_at_ms: Date.now(),
		};
	}

	private async deriveEffectiveIntradayFromOneMinute(
		interval: "3min" | "5min" | "15min" | "30min" | "1h",
		startDatetime: string,
		endDatetime: string,
	) {
		const minutes =
			interval === "3min"
				? 3
				: interval === "5min"
					? 5
					: interval === "15min"
						? 15
						: interval === "30min"
							? 30
							: 60;
		const oneMinute = await this.getEffectiveOneMinuteRows(
			startDatetime,
			endDatetime,
		);
		const buckets = new Map<string, NormalizedCandle[]>();

		for (const candle of oneMinute) {
			const bucketStart = intradayBucketStart(candle.datetime, minutes);
			if (!bucketStart) continue;
			const rows = buckets.get(bucketStart) ?? [];
			rows.push(candle);
			buckets.set(bucketStart, rows);
		}

		const derived: NormalizedCandle[] = [];
		for (const [bucketStart, rows] of buckets) {
			const expectedClose = addMinutesToCairoDatetime(
				bucketStart,
				minutes,
			);
			const candle = this.aggregateRows(
				interval,
				bucketStart,
				expectedClose,
				rows,
				"provisional_1min",
			);
			if (candle) derived.push(candle);
		}

		return derived.sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private async getEffectiveIntradaySeries(
		interval: "5min" | "15min" | "30min" | "1h",
		startDatetime: string,
		endDatetime: string,
	) {
		const confirmedPage =
			await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix(interval),
				reverse: true,
				limit: 500,
			});
		const merged = new Map<string, NormalizedCandle>();

		const derived = await this.deriveEffectiveIntradayFromOneMinute(
			interval,
			startDatetime,
			endDatetime,
		);
		for (const candle of derived) {
			merged.set(candle.datetime, candle);
		}

		// Confirmed REST always wins over provisional reconstruction.
		for (const candle of confirmedPage.values()) {
			if (
				candle.datetime >= startDatetime &&
				candle.datetime <= endDatetime
			) {
				merged.set(candle.datetime, candle);
			}
		}

		return Array.from(merged.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private async getEffectiveDailySeries(
		startDatetime: string,
		endDatetime: string,
	) {
		const confirmedPage =
			await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix("1day"),
				reverse: true,
				limit: 40,
			});
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of confirmedPage.values()) {
			if (
				candle.datetime >= startDatetime &&
				candle.datetime <= endDatetime
			) {
				merged.set(candle.datetime, candle);
			}
		}

		const current = await this.buildCurrentProvisional("1day");
		if (
			current &&
			current.datetime >= startDatetime &&
			current.datetime <= endDatetime
		) {
			merged.set(current.datetime, current);
		}

		return Array.from(merged.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private async buildCurrentProvisional(
		interval: AnalysisNormalizedInterval,
	): Promise<NormalizedCandle | null> {
		const nowMs = Date.now();
		const nowCairo = cairoTime(nowMs);
		const marketClosed = isClosedMarketCairoDatetime(nowCairo);

		// Intraday and Daily must not fabricate a new candle while the market
		// is closed. Weekly/Monthly, however, may already be active candles
		// spanning a daily closure; allow them to keep an effective provisional
		// view from the latest confirmed Daily data until their own close.
		if (
			marketClosed &&
			interval !== "1week" &&
			interval !== "1month"
		) {
			return null;
		}

		if (interval === "1min") {
			if (!this.currentCandle) return null;
			return this.liveCandleToNormalized(this.currentCandle);
		}

		if (
			interval === "3min" ||
			interval === "5min" ||
			interval === "15min" ||
			interval === "30min" ||
			interval === "1h"
		) {
			const minutes =
				interval === "3min"
					? 3
					: interval === "5min"
						? 5
						: interval === "15min"
							? 15
							: interval === "30min"
								? 30
								: 60;
			const bucketStart = intradayBucketStart(nowCairo, minutes);
			if (!bucketStart) return null;
			const rows = await this.getEffectiveOneMinuteRows(
				bucketStart,
				nowCairo,
			);
			return this.aggregateRows(
				interval,
				bucketStart,
				addMinutesToCairoDatetime(bucketStart, minutes),
				rows,
				"provisional_1min",
			);
		}

		if (interval === "4h") {
			const bucketStart = fourHourBucketStart(nowCairo);
			if (!bucketStart) return null;
			const rows = await this.getEffectiveIntradaySeries(
				"15min",
				bucketStart,
				nowCairo,
			);
			return this.aggregateRows(
				"4h",
				bucketStart,
				addMinutesToCairoDatetime(bucketStart, 240),
				rows,
				"provisional_15min",
			);
		}

		if (interval === "1day") {
			const bucketStart = dayStartCairo(nowCairo);
			const rows = await this.getEffectiveIntradaySeries(
				"1h",
				bucketStart,
				nowCairo,
			);
			return this.aggregateRows(
				"1day",
				bucketStart,
				addMinutesToCairoDatetime(bucketStart, 24 * 60),
				rows,
				"provisional_1h",
			);
		}

		if (interval === "1week") {
			const bucketStart = weekStartMondayCairo(nowCairo);
			const expectedClose =
				addMinutesToCairoDatetime(bucketStart, 5 * 24 * 60);

			// After Saturday 00:00 the trading week is closed; the Worker should
			// expose the confirmed weekly candle, not a provisional duplicate.
			const expectedCloseMs = cairoDatetimeToMs(expectedClose);
			if (
				expectedCloseMs !== null &&
				nowMs >= expectedCloseMs
			) {
				return null;
			}

			const rows = await this.getEffectiveDailySeries(
				bucketStart,
				nowCairo,
			);
			const weekly = this.aggregateRows(
				"1week",
				bucketStart,
				expectedClose,
				rows,
				"provisional_1day",
			);

			if (!weekly) {
				return null;
			}

			// Project rule: every new weekly candle begins at the previous
			// confirmed weekly close, so the weekend discontinuity is absorbed
			// inside the new weekly candle rather than inheriting the first
			// Daily candle's gap-adjusted open.
			const previousWeeklyPage =
				await this.ctx.storage.list<NormalizedCandle>({
					prefix: normalizedPrefix("1week"),
					reverse: true,
					limit: 20,
				});

			const previousConfirmedWeekly =
				Array.from(previousWeeklyPage.values())
					.filter(
						(candle) =>
							candle.confirmed === true &&
							candle.datetime < bucketStart,
					)
					.sort((a, b) =>
						b.datetime.localeCompare(a.datetime),
					)[0] ?? null;

			if (previousConfirmedWeekly) {
				const anchoredOpen = previousConfirmedWeekly.close;
				weekly.open = anchoredOpen;
				weekly.high = Math.max(weekly.high, anchoredOpen);
				weekly.low = Math.min(weekly.low, anchoredOpen);
				weekly.gap_adjusted = true;
			}

			return weekly;
		}

		const bucketStart = monthStartCairo(nowCairo);
		const rows = await this.getEffectiveDailySeries(
			bucketStart,
			nowCairo,
		);
		return this.aggregateRows(
			"1month",
			bucketStart,
			nextMonthStartCairo(bucketStart),
			rows,
			"provisional_1day",
		);
	}

	private async buildProvisionalTail(
		interval: AnalysisNormalizedInterval,
	) {
		if (
			interval === "4h" ||
			interval === "1day" ||
			interval === "1week" ||
			interval === "1month"
		) {
			const current = await this.buildCurrentProvisional(interval);
			return current ? [current] : [];
		}

		const latestPage =
			await this.ctx.storage.list<NormalizedCandle>({
				prefix: normalizedPrefix(interval),
				reverse: true,
				limit: 1,
			});
		const latest = Array.from(latestPage.values())[0] ?? null;
		const nowCairo = cairoTime(Date.now());
		if (isClosedMarketCairoDatetime(nowCairo)) return [];

		if (interval === "1min") {
			const start = latest?.expected_close_time ??
				cairoTime(Date.now() - 8 * 60 * 60 * 1000);
			const rows = await this.getEffectiveOneMinuteRows(start, nowCairo);
			return rows.filter((c) => !latest || c.datetime > latest.datetime);
		}

		const start = latest?.expected_close_time ??
			cairoTime(Date.now() - 8 * 60 * 60 * 1000);
		const rows = await this.deriveEffectiveIntradayFromOneMinute(
			interval,
			start,
			nowCairo,
		);
		return rows.filter((c) => !latest || c.datetime > latest.datetime);
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

			auto_refresh: this.publicAutoState(),

			data_policy: {
				websocket:
					"PROVISIONAL",

				historical_rest:
					"AUTHORITATIVE",

				rule:
					"If REST historical data differs from provisional data, REST replaces the overlap after candle close. No standalone gap candle is ever created.",
				provisional_chain:
					"The open part of every timeframe is rebuilt from the freshest effective lower timeframe so its latest close follows the live price.",
			},
		};
	}

	private async persist() {
		try {
			await this.ctx.storage.put("live_state", {
				enabled: this.enabled,
				lastPrice: this.lastPrice,
				lastTickMs: this.lastTickMs,
				tickCount: this.tickCount,
				reconnectCount: this.reconnectCount,
				currentCandle: this.currentCandle,
				candles: this.candles,
				lastError: this.lastError,
				subscribeStatus: this.subscribeStatus,
			});
			return true;
		} catch (error) {
			this.lastError =
				error instanceof Error ? error.message : String(error);
			return false;
		}
	}
}


// Public Worker
export default {
	async fetch(
		request: Request,
		env: LiveEnv,
	) {
		const url = new URL(request.url);

		if (url.pathname === "/ping" || url.pathname === "/version") {
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				service: "Gold Data Engine outer worker",
				production_object_name: PROD_OBJECT_NAME,
				legacy_object_name: LEGACY_OBJECT_NAME,
				retired_object_names: RETIRED_OBJECT_NAMES,
				recovery_soft_limit_rows: RECOVERY_SOFT_LIMIT_ROWS,
				recovery_hard_ceiling_rows: RECOVERY_HARD_CEILING_ROWS,
				durable_object_touched: false,
			});
		}

		if (url.pathname === "/probe-fresh-do") {
			try {
				// Dedicated zero-write probe object.
				const probeId = env.Chat.idFromName(
					"XAUUSD_V14_4_FRESH_PROBE_20260909",
				);
				const probeStub = env.Chat.get(probeId);
				const probeUrl = new URL(request.url);
				probeUrl.pathname = "/instance-probe";
				probeUrl.search = "";
				return await probeStub.fetch(
					new Request(probeUrl.toString(), {
						method: "GET",
						headers: request.headers,
					}),
				);
			} catch (error) {
				const anyError = error as {
					message?: unknown;
					name?: unknown;
					remote?: unknown;
					retryable?: unknown;
					overloaded?: unknown;
				};
				return json(
					{
						status: "error",
						build_version: BUILD_VERSION,
						probe: "fresh_durable_object",
						error:
							anyError?.message != null
								? String(anyError.message)
								: String(error),
						name:
							anyError?.name != null ? String(anyError.name) : null,
						remote: Boolean(anyError?.remote),
						retryable: Boolean(anyError?.retryable),
						overloaded: Boolean(anyError?.overloaded),
					},
					503,
				);
			}
		}

		if (url.pathname === "/retire-legacy") {
			const results = [];
			for (const objectName of RETIRED_OBJECT_NAMES) {
				try {
					const id = env.Chat.idFromName(objectName);
					const stub = env.Chat.get(id);
					const retireUrl = new URL(request.url);
					retireUrl.pathname = "/retire-instance";
					retireUrl.search = "";
					const response = await stub.fetch(
						new Request(retireUrl.toString(), {
							method: "GET",
							headers: request.headers,
						}),
					);
					let body: unknown = null;
					try {
						body = await response.json();
					} catch {
						body = await response.text();
					}
					results.push({
						object_name: objectName,
						http_status: response.status,
						result: body,
					});
				} catch (error) {
					results.push({
						object_name: objectName,
						http_status: 503,
						result: {
							status: "error",
							error:
								error instanceof Error
									? error.message
									: String(error),
						},
					});
				}
			}
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				action: "retire_legacy",
				production_object_name: PROD_OBJECT_NAME,
				results,
			});
		}

		if (url.pathname === "/probe-prod-do" || url.pathname === "/probe-legacy-do") {
			try {
				const objectName =
					url.pathname === "/probe-prod-do"
						? PROD_OBJECT_NAME
						: LEGACY_OBJECT_NAME;
				const probeId = env.Chat.idFromName(objectName);
				const probeStub = env.Chat.get(probeId);
				const probeUrl = new URL(request.url);
				probeUrl.pathname = "/instance-probe";
				probeUrl.search = "";
				return await probeStub.fetch(
					new Request(probeUrl.toString(), {
						method: "GET",
						headers: request.headers,
					}),
				);
			} catch (error) {
				const anyError = error as {
					message?: unknown;
					name?: unknown;
					remote?: unknown;
					retryable?: unknown;
					overloaded?: unknown;
				};
				return json(
					{
						status: "error",
						build_version: BUILD_VERSION,
						probe:
							url.pathname === "/probe-prod-do"
								? "production_durable_object"
								: "legacy_durable_object",
						error:
							anyError?.message != null
								? String(anyError.message)
								: String(error),
						name:
							anyError?.name != null ? String(anyError.name) : null,
						remote: Boolean(anyError?.remote),
						retryable: Boolean(anyError?.retryable),
						overloaded: Boolean(anyError?.overloaded),
					},
					503,
				);
			}
		}

		try {
			// Production traffic is intentionally moved to a fresh logical Durable
			// Object instance. The legacy XAUUSD instance remains untouched so its
			// stored data can be preserved for later inspection/recovery.
			const id = env.Chat.idFromName(PROD_OBJECT_NAME);
			const stub = env.Chat.get(id);
			return await stub.fetch(request);
		} catch (error) {
			const anyError = error as {
				message?: unknown;
				name?: unknown;
				remote?: unknown;
				retryable?: unknown;
				overloaded?: unknown;
			};
			return json(
				{
					status: "error",
					build_version: BUILD_VERSION,
					component: "durable_object",
					error:
						anyError?.message != null
							? String(anyError.message)
							: String(error),
					name:
						anyError?.name != null ? String(anyError.name) : null,
					remote: Boolean(anyError?.remote),
					retryable: Boolean(anyError?.retryable),
					overloaded: Boolean(anyError?.overloaded),
					note:
						"The outer Worker is healthy; the Durable Object request failed and was caught for diagnostics.",
				},
				503,
			);
		}
	},
};
