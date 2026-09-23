import { DurableObject } from "cloudflare:workers";

const SYMBOL = "XAU/USD";
const TIMEZONE = "Africa/Cairo";
const TWELVE_DATA_REST_URL = "https://api.twelvedata.com/time_series";
const BUILD_VERSION = "v15.9-authoritative-catchup-gap-queue-2026-09-23";
const PROD_OBJECT_NAME = "XAUUSD_V14_6_PROD_20260909";
const LEGACY_OBJECT_NAME = "XAUUSD";
const RETIRED_OBJECT_NAMES = ["XAUUSD", "XAUUSD_V14_5_PROD_20260909"] as const;

// Full bootstrap work uses an internal conservative reservation budget. It is NOT an
// actual Cloudflare SQL-rows-written counter. Targeted continuity/staleness repairs
// bypass this legacy budget because v15.7+ repairs are bounded to the affected range;
// the provider request-rate guard remains the controlling safety limit.
const RECOVERY_BUDGET_KEY = "recovery_write_budget_v1";
const RECOVERY_SOFT_LIMIT_ROWS = 40_000;
const RECOVERY_HARD_CEILING_ROWS = 60_000;
const RECOVERY_MAX_MANUAL_ADD_ROWS = 25_000;
const RECOVERY_COST_MULTIPLIER = 3;

const HEARTBEAT_MS = 10_000;
const RECONNECT_MS = 5_000;

// Keep enough provisional 1-minute candles to bridge every 4H refresh window.
const MAX_STORED_CANDLES = 480;

const AUTO_STATE_KEY = "auto_refresh_state";
// v14.7: while the queue is busy we may need a one-minute follow-up because
// of the Twelve Data rate window. When idle, the smallest REST cadence is
// five minutes, so waking the Durable Object every minute only creates
// unnecessary alarm/metadata writes.
const AUTO_BUSY_ALARM_MS = 60_000;
const AUTO_IDLE_ALARM_MS = 5 * 60_000;
const AUTO_RATE_WINDOW_MS = 60_000;
// Twelve Data Basic allows 8 API credits/minute. Keep one credit spare
// for a manual request and process the rest automatically on the next alarm.
const AUTO_MAX_REQUESTS_PER_WINDOW = 7;
const AUTO_INCREMENTAL_OUTPUTSIZE = 12;
const AUTO_BOOTSTRAP_OUTPUTSIZE = 1150;

// Self-healing scheduler / continuity recovery.
const AUTO_WATCHDOG_STALE_MS = 12 * 60_000;
const RECOVERY_BUFFER_ROWS = 40;
const RECOVERY_MAX_OUTPUTSIZE = 1150;
const RECOVERY_NOOP_COOLDOWN_MS = 30 * 60_000;
const RECOVERY_TARGET_OVERLAP_BUCKETS = 3;
const SYNTHETIC_MICRO_GAP_MAX_1M = 5;

// Quota-resilient lifecycle. The Durable Object constructor performs zero
// storage I/O; persistent state is loaded lazily on the first storage-dependent
// request/alarm. This keeps the SAME Durable Object addressable even while a
// daily Durable Objects storage quota is refusing reads.
const STORAGE_QUOTA_RESET_BUFFER_MS = 2 * 60_000;
const STORAGE_QUOTA_POST_RESET_GRACE_MS = 15 * 60_000;
const STORAGE_QUOTA_GRACE_RETRY_MS = 2 * 60_000;
const CONTINUITY_INITIAL_SCAN_LIMIT = 1000;
// Routine continuity checks verify only new data plus a safety overlap.
// Historical coverage is preserved by the rotating deep scan below.
const CONTINUITY_FAST_OVERLAP_BUCKETS = 32;
const CONTINUITY_DEEP_PAGE_LIMIT = 250;
const SYNTHETIC_RECENT_SCAN_LIMIT = 120;
const TARGETED_REPAIR_OVERLAP_BUCKETS = 2;
const AUTO_RECOVERY_AUDIT_INTERVAL_MS = 5 * 60_000;
const CONTINUITY_TRACKED_INTERVALS = [
	"1min",
	"3min",
	"5min",
	"15min",
	"30min",
	"1h",
	"4h",
] as const;
type ContinuityTrackedInterval = (typeof CONTINUITY_TRACKED_INTERVALS)[number];
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
	// Exact continuity gap that caused this recovery item. Persisting the target
	// lets us verify old gaps without re-scanning a large recent window.
	target_gap?: ContinuityGap | null;
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
	// Native REST timeframes must remain fresh independently of provisional data.
	// This prevents a fresh 1M-derived tail from hiding a stale authoritative layer.
	authoritative_fresh: boolean;
	authoritative_lag_minutes: number | null;
};

type AutoRefreshState = {
	enabled: boolean;
	queue: AutoQueueItem[];
	last_5m_key: string;
	last_15m_key: string;
	// Optional for backwards compatibility with v15.8 persisted auto-state.
	last_30m_key?: string;
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
	// v14.9: no-op recovery cooldowns are piggybacked on existing auto-state writes.
	// This prevents an unrecoverable provider omission from draining the internal
	// recovery reservation every five minutes. Key = interval|targeted end_date.
	recovery_noop_until_ms?: Record<string, number>;
	// v15.7 continuity state. Checkpoints make routine audits incremental; the
	// deep cursor walks older history in small rotating pages so old gaps remain
	// discoverable without re-reading the same 1000 rows every five minutes.
	continuity_fast_checkpoint?: Partial<Record<ContinuityTrackedInterval, string>>;
	continuity_deep_before?: Partial<Record<ContinuityTrackedInterval, string | null>>;
	continuity_deep_rotation_index?: number;
	continuity_active_gaps?: Record<string, ContinuityGap>;
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

// Durable Object storage `end` is exclusive. Appending a NUL byte creates
// the smallest lexicographic key strictly after the exact key, allowing an
// inclusive datetime range without widening the scan to the rest of a prefix.
function storageExclusiveEndAfterExactKey(key: string) {
	return `${key}\u0000`;
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
	// Storage-write health is tracked separately from WebSocket/provider errors.
	// A historical quota error must never masquerade as a current runtime error.
	private lastStorageWriteError: string | null = null;
	private lastStorageWriteErrorMs: number | null = null;
	private lastStorageWriteSuccessMs: number | null = null;
	private historicalStorageWriteError: string | null = null;
	private subscribeStatus: unknown = null;
	private autoState: AutoRefreshState | null = null;

	// v15.8 lazy-storage lifecycle state. None of these fields are persisted;
	// they exist only to stop repeated quota reads inside one runtime instance.
	private storageLoaded = false;
	private storageLoadPromise: Promise<boolean> | null = null;
	private lastStorageReadError: string | null = null;
	private lastStorageReadErrorMs: number | null = null;
	private storageReadBlockedUntilMs: number | null = null;

	constructor(ctx: DurableObjectState, env: LiveEnv) {
		super(ctx, env);

		// IMPORTANT: zero storage I/O in the constructor.
		//
		// A read-quota failure used to happen inside blockConcurrencyWhile(),
		// preventing this Durable Object from booting at all. Persistent state is
		// now loaded lazily by ensureStorageLoaded() after the instance is alive.
	}

	private isStorageReadQuotaError(value: unknown) {
		if (value == null) return false;
		const message = value instanceof Error ? value.message : String(value);
		return /(?:Exceeded allowed rows read|rows read in Durable Objects free tier|Durable Objects.*rows read|quota.*rows? read|quota.*read)/i.test(
			message,
		);
	}

	private nextUtcQuotaResetProbeMs(now = Date.now()) {
		const date = new Date(now);
		const nextMidnightUtc = Date.UTC(
			date.getUTCFullYear(),
			date.getUTCMonth(),
			date.getUTCDate() + 1,
			0,
			0,
			0,
			0,
		);
		return nextMidnightUtc + STORAGE_QUOTA_RESET_BUFFER_MS;
	}

	private quotaRetryAtMs(now = Date.now()) {
		const date = new Date(now);
		const msSinceUtcMidnight =
			date.getUTCHours() * 60 * 60_000 +
			date.getUTCMinutes() * 60_000 +
			date.getUTCSeconds() * 1_000 +
			date.getUTCMilliseconds();

		// A small post-reset grace window handles the exact failure mode observed
		// in production: the documented reset time has passed, yet one old object
		// can briefly continue returning the previous quota error. Retry sparsely
		// inside that window instead of sleeping for another full day.
		if (msSinceUtcMidnight <= STORAGE_QUOTA_POST_RESET_GRACE_MS) {
			return now + STORAGE_QUOTA_GRACE_RETRY_MS;
		}
		return this.nextUtcQuotaResetProbeMs(now);
	}

	private noteStorageReadQuotaFailure(error: unknown) {
		const now = Date.now();
		const message = error instanceof Error ? error.message : String(error);
		this.lastStorageReadError = message;
		this.lastStorageReadErrorMs = now;
		this.storageReadBlockedUntilMs = this.quotaRetryAtMs(now);
		this.lastError = message;
		return this.storageReadBlockedUntilMs;
	}

	private clearStorageReadQuotaFailure() {
		this.lastStorageReadError = null;
		this.lastStorageReadErrorMs = null;
		this.storageReadBlockedUntilMs = null;
		if (
			this.lastError !== null &&
			this.isStorageReadQuotaError(this.lastError)
		) {
			this.lastError = null;
		}
	}

	private storageQuotaRuntimeStatus() {
		const now = Date.now();
		return {
			storage_loaded: this.storageLoaded,
			read_status:
				this.lastStorageReadError !== null ? "quota_blocked" : "ready_or_unprobed",
			last_read_quota_error: this.lastStorageReadError,
			last_read_quota_error_time:
				this.lastStorageReadErrorMs !== null
					? new Date(this.lastStorageReadErrorMs).toISOString()
					: null,
			blocked_until:
				this.storageReadBlockedUntilMs !== null
					? new Date(this.storageReadBlockedUntilMs).toISOString()
					: null,
			probe_allowed_now:
				this.storageReadBlockedUntilMs === null ||
				now >= this.storageReadBlockedUntilMs,
		};
	}

	private async loadPersistentState() {
		const saved = await this.ctx.storage.get<{
			enabled?: boolean;
			lastPrice?: number | null;
			lastTickMs?: number | null;
			tickCount?: number;
			reconnectCount?: number;
			currentCandle?: ProvisionalCandle | null;
			candles?: ProvisionalCandle[];
			lastError?: string | null;
			lastStorageWriteError?: string | null;
			lastStorageWriteErrorMs?: number | null;
			lastStorageWriteSuccessMs?: number | null;
			historicalStorageWriteError?: string | null;
			subscribeStatus?: unknown;
		}>("live_state");

		if (saved) {
			this.enabled = saved.enabled ?? true;
			this.tickCount = saved.tickCount ?? 0;
			this.reconnectCount = saved.reconnectCount ?? 0;

			const savedLastError = saved.lastError ?? null;
			this.lastStorageWriteError = saved.lastStorageWriteError ?? null;
			this.lastStorageWriteErrorMs = saved.lastStorageWriteErrorMs ?? null;
			this.lastStorageWriteSuccessMs = saved.lastStorageWriteSuccessMs ?? null;
			this.historicalStorageWriteError =
				saved.historicalStorageWriteError ?? null;

			if (
				savedLastError !== null &&
				this.isStorageWriteErrorMessage(savedLastError)
			) {
				this.historicalStorageWriteError = savedLastError;
				this.lastError = null;
			} else if (!this.isStorageReadQuotaError(savedLastError)) {
				this.lastError = savedLastError;
			}
			this.subscribeStatus = saved.subscribeStatus ?? null;

			this.candles = (saved.candles ?? [])
				.filter(
					(candle) =>
						!isClosedMarketCairoDatetime(candle.datetime),
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
						gap_adjusted: restoredCurrent.gap_adjusted ?? false,
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
			(await this.ctx.storage.get<AutoRefreshState>(AUTO_STATE_KEY)) ?? null;

		if (savedAuto) {
			this.autoState = savedAuto;
			if (
				this.autoState.last_error !== null &&
				this.isStorageWriteErrorMessage(this.autoState.last_error)
			) {
				this.historicalStorageWriteError =
					this.historicalStorageWriteError ?? this.autoState.last_error;
				this.autoState.last_error = null;
			}
		} else {
			// Preserve controlled activation: deploy alone never starts bootstrap.
			this.autoState = this.createInitialAutoState(nowCairo, now);
			this.autoState.enabled = false;
			this.autoState.queue = [];
			this.autoState.bootstrap_pending = false;
		}
	}

	private async ensureStorageLoaded(forceProbe = false) {
		if (this.storageLoaded) {
			return { ok: true as const, loaded_now: false };
		}

		const now = Date.now();
		if (
			!forceProbe &&
			this.storageReadBlockedUntilMs !== null &&
			now < this.storageReadBlockedUntilMs
		) {
			return {
				ok: false as const,
				quota_blocked: true,
				error: this.lastStorageReadError,
				retry_at_ms: this.storageReadBlockedUntilMs,
			};
		}

		if (this.storageLoadPromise === null) {
			this.storageLoadPromise = (async () => {
				try {
					await this.loadPersistentState();
					this.storageLoaded = true;
					this.clearStorageReadQuotaFailure();
					return true;
				} catch (error) {
					if (this.isStorageReadQuotaError(error)) {
						this.noteStorageReadQuotaFailure(error);
					} else {
						this.lastError =
							error instanceof Error ? error.message : String(error);
					}
					return false;
				} finally {
					this.storageLoadPromise = null;
				}
			})();
		}

		const loaded = await this.storageLoadPromise;
		if (loaded) {
			return { ok: true as const, loaded_now: true };
		}
		return {
			ok: false as const,
			quota_blocked: this.lastStorageReadError !== null,
			error: this.lastStorageReadError ?? this.lastError,
			retry_at_ms: this.storageReadBlockedUntilMs,
		};
	}

	private async scheduleQuotaRecoveryAlarm(retryAtMs: number | null) {
		if (retryAtMs === null) return false;
		try {
			await this.ctx.storage.setAlarm(retryAtMs);
			this.markStorageWriteSuccess();
			return true;
		} catch (error) {
			this.markStorageWriteFailure(error);
			return false;
		}
	}

	async fetch(request: Request) {
		const url = new URL(request.url);

		// These routes never require persistent storage. They remain available even
		// while the storage read quota is refusing the production object.
		if (url.pathname === "/instance-probe") {
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				instance_probe: true,
				storage_io_attempted: false,
				object_name: this.objectName(),
				quota_runtime: this.storageQuotaRuntimeStatus(),
			});
		}

		if (url.pathname === "/quota-status") {
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				object_name: this.objectName(),
				quota_runtime: this.storageQuotaRuntimeStatus(),
			});
		}

		if (url.pathname === "/runtime-reset-instance") {
			this.ctx.abort("Manual quota recovery runtime reset", {
				retryAlarm: false,
			});
		}

		if (url.pathname === "/storage-probe") {
			const loaded = await this.ensureStorageLoaded(true);
			if (!loaded.ok) {
				await this.scheduleQuotaRecoveryAlarm(loaded.retry_at_ms);
				return json(
					{
						status: "degraded",
						build_version: BUILD_VERSION,
						storage_probe: false,
						object_name: this.objectName(),
						error: loaded.error,
						quota_runtime: this.storageQuotaRuntimeStatus(),
					},
					503,
				);
			}
			return json({
				status: "ok",
				build_version: BUILD_VERSION,
				storage_probe: true,
				object_name: this.objectName(),
				quota_runtime: this.storageQuotaRuntimeStatus(),
			});
		}

		const loaded = await this.ensureStorageLoaded();
		if (!loaded.ok) {
			await this.scheduleQuotaRecoveryAlarm(loaded.retry_at_ms);
			return json(
				{
					status: "degraded",
					build_version: BUILD_VERSION,
					component: "durable_object_storage",
					error: loaded.error,
					note:
						"Durable Object runtime is alive. Storage reads are circuit-broken until the next guarded probe; no object rotation or data deletion is required.",
					quota_runtime: this.storageQuotaRuntimeStatus(),
				},
				503,
			);
		}

		try {
			return await this.handleFetch(request);
		} catch (error) {
			if (this.isStorageReadQuotaError(error)) {
				const retryAtMs = this.noteStorageReadQuotaFailure(error);
				await this.scheduleQuotaRecoveryAlarm(retryAtMs);
				return json(
					{
						status: "degraded",
						build_version: BUILD_VERSION,
						component: "durable_object_storage",
						error:
							error instanceof Error ? error.message : String(error),
						quota_runtime: this.storageQuotaRuntimeStatus(),
					},
					503,
				);
			}
			throw error;
		}
	}

	private async handleFetch(request: Request) {
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
				storage_write_health: this.storageWriteHealth(),
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
						"Legacy Durable Object is retired. Production uses the current in-place object.",
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
						"Direct heavy maintenance is disabled. Use guarded recovery.",
				},
				403,
			);
		}

		if (url.pathname === "/activate") {
			if (!this.isProductionInstance()) {
				return json(
					{
						status: "error",
						error: "Activation is allowed only on the current production object.",
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
				await this.postAutoRefresh(
					{
						interval: requestedInterval,
						outputsize,
						reason: before ? "manual_backfill" : "manual_sync",
						enqueued_ms: Date.now(),
						attempts: 0,
						before,
					},
					result,
				);
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

			// normalized-data is strictly read-only. Higher native
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
				runtime_health: {
					current_last_error: this.lastError,
					storage_write: this.storageWriteHealth(),
				},
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

	private async listNormalizedRange(
		interval: AnalysisNormalizedInterval,
		fromDatetime: string,
		toDatetime: string,
	) {
		if (fromDatetime > toDatetime) return [] as NormalizedCandle[];

		const startKey = normalizedCandleKey(interval, fromDatetime);
		const endKey = storageExclusiveEndAfterExactKey(
			normalizedCandleKey(interval, toDatetime),
		);
		const rows: NormalizedCandle[] = [];
		let startAfter: string | undefined;

		while (true) {
			const page = await this.ctx.storage.list<NormalizedCandle>({
				...(startAfter ? { startAfter } : { start: startKey }),
				end: endKey,
				limit: 1000,
			});
			if (page.size === 0) break;
			rows.push(...page.values());
			if (page.size < 1000) break;
			const keys = Array.from(page.keys()) as string[];
			startAfter = keys[keys.length - 1];
		}

		return rows;
	}

	private async listHistoricalRange(
		interval: RestInterval,
		fromDatetime: string,
		toDatetime: string,
	) {
		if (fromDatetime > toDatetime) return [] as StoredHistoricalCandle[];

		const startKey = historicalCandleKey(interval, fromDatetime);
		const endKey = storageExclusiveEndAfterExactKey(
			historicalCandleKey(interval, toDatetime),
		);
		const rows: StoredHistoricalCandle[] = [];
		let startAfter: string | undefined;

		while (true) {
			const page = await this.ctx.storage.list<StoredHistoricalCandle>({
				...(startAfter ? { startAfter } : { start: startKey }),
				end: endKey,
				limit: 1000,
			});
			if (page.size === 0) break;
			rows.push(...page.values());
			if (page.size < 1000) break;
			const keys = Array.from(page.keys()) as string[];
			startAfter = keys[keys.length - 1];
		}

		return rows;
	}

	private async listRecentConfirmedWindow(
		interval: AnalysisNormalizedInterval,
		fromDatetime: string | null,
		toDatetime: string | null,
		limit = CONTINUITY_INITIAL_SCAN_LIMIT,
	) {
		if (!toDatetime) return [] as NormalizedCandle[];
		const options: Record<string, unknown> = {
			reverse: true,
			limit,
		};
		if (fromDatetime) {
			options.start = normalizedCandleKey(interval, fromDatetime);
			options.end = storageExclusiveEndAfterExactKey(
				normalizedCandleKey(interval, toDatetime),
			);
		} else {
			options.prefix = normalizedPrefix(interval);
		}
		const page = await this.ctx.storage.list<NormalizedCandle>(options);
		return Array.from(page.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private async previousNormalizedCandle(
		interval: AnalysisNormalizedInterval,
		beforeDatetime: string,
	) {
		const page = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(interval),
			end: normalizedCandleKey(interval, beforeDatetime),
			reverse: true,
			limit: 1,
		});
		return Array.from(page.values())[0] ?? null;
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

		if (fromDatetime > toDatetime) {
			return { ...stats, deleted: 0 };
		}

		const keep = new Set(candidates.map((c) => c.datetime));
		const deleteKeys: string[] = [];
		const existingRange = await this.listNormalizedRange(
			interval,
			fromDatetime,
			toDatetime,
		);

		for (const candle of existingRange) {
			if (!keep.has(candle.datetime)) {
				deleteKeys.push(normalizedCandleKey(interval, candle.datetime));
			}
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


			const sourceMeta =
				(await this.ctx.storage.get<NormalizedMeta>(
					normalizedMetaKey("1min"),
				)) ?? null;

			const regularRows = source;
			const sourceGapAdjustedRows = source.filter(
				(c) => c.gap_adjusted === true,
			).length;
			const sourceSyntheticGapRows = source.filter(
				(c) => c.synthetic_gap === true,
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
				const containsSynthetic = rows.some(
					(c) => c.synthetic_gap === true,
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
					source: containsSynthetic
						? "derived_1min_with_synthetic_bridge"
						: "derived_1min",
					provisional: false,
					confirmed: true,
					synthetic_gap: containsSynthetic,
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
				source_synthetic_gap_rows: sourceSyntheticGapRows,
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
				source_synthetic_gap_rows: sourceSyntheticGapRows,
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
					"3min is deterministic OHLC aggregation from normalized 1min. Isolated 1M omissions of up to five candles may be bridged by explicitly tagged synthetic approximations and are then aggregated normally. No market-analysis logic is performed here.",
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


	private isStorageWriteErrorMessage(value: unknown) {
		if (value == null) return false;
		const message = String(value);
		return /(?:Exceeded allowed rows written|rows written in Durable Objects|storage\s*write|SQLITE.*write|quota.*rows? written|quota.*write)/i.test(message);
	}

	private markStorageWriteFailure(error: unknown) {
		const message = error instanceof Error ? error.message : String(error);
		const now = Date.now();
		this.lastStorageWriteError = message;
		this.lastStorageWriteErrorMs = now;
		this.historicalStorageWriteError = message;
		this.lastError = message;
		if (this.autoState) {
			this.autoState.last_error = message;
		}
		return message;
	}

	private markStorageWriteSuccess(successMs = Date.now()) {
		this.lastStorageWriteSuccessMs = successMs;
		// Any successful Durable Object write proves a previously persisted
		// write-quota error is no longer current. Keep it only as history.
		if (this.lastStorageWriteError !== null) {
			this.historicalStorageWriteError =
				this.historicalStorageWriteError ?? this.lastStorageWriteError;
		}
		this.lastStorageWriteError = null;
		this.lastStorageWriteErrorMs = null;
		if (this.lastError !== null && this.isStorageWriteErrorMessage(this.lastError)) {
			this.lastError = null;
		}
		if (
			this.autoState?.last_error !== null &&
			this.autoState?.last_error !== undefined &&
			this.isStorageWriteErrorMessage(this.autoState.last_error)
		) {
			this.autoState.last_error = null;
		}
	}

	private storageWriteHealth() {
		const currentError = this.lastStorageWriteError;
		return {
			status: currentError !== null ? "degraded" : "healthy",
			current_error: currentError,
			current_error_time:
				this.lastStorageWriteErrorMs !== null
					? cairoTime(this.lastStorageWriteErrorMs)
					: null,
			last_success_time:
				this.lastStorageWriteSuccessMs !== null
					? cairoTime(this.lastStorageWriteSuccessMs)
					: null,
			historical_error: this.historicalStorageWriteError,
			degradation_rule:
				"Historical errors are informational only. Degrade only on a current failed Action/readiness failure or current storage_write_health=degraded.",
		};
	}

	private autoStateFingerprint() {
		this.ensureAutoState();
		const { last_alarm_ms: _ignoredLastAlarm, ...stable } = this.autoState!;
		return JSON.stringify(stable);
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
			reserved_estimated_rows: budget.estimated_used_rows,
			accounting_scope: "recovery_reservation_only_not_cloudflare_actual_usage",
			effective_limit_rows: effectiveLimit,
			remaining_estimated_rows: Math.max(
				0,
				effectiveLimit - budget.estimated_used_rows,
			),
			paused: budget.estimated_used_rows >= effectiveLimit,
			note:
				"Internal conservative FULL-BOOTSTRAP reservation only; it is not Cloudflare SQL rows written. Targeted gap/staleness recovery bypasses this legacy reservation and remains bounded by per-request size plus the 7 REST requests/minute guard.",
		};
	}

	private isRecoveryWork(item: AutoQueueItem) {
		return (
			item.reason.includes("bootstrap") ||
			item.reason.includes("backfill") ||
			item.reason.includes("recovery")
		);
	}

	private isRecoveryBudgetGuardedWork(item: AutoQueueItem) {
		// The legacy 40K reservation remains only for full bootstrap work.
		// Exact gap repairs and stale authoritative-tail catch-up are targeted and
		// remain bounded by outputsize + the global 7 REST requests/minute guard.
		return item.reason.includes("bootstrap");
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
			last_30m_key: scheduleBucketKey(nowCairo, 30),
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
			continuity_fast_checkpoint: {},
			continuity_deep_before: {},
			continuity_deep_rotation_index: 0,
			continuity_active_gaps: {},
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
		// v15.9 migration-in-place: old persisted v15.8 state has no 30M key.
		// Initialize it without forcing a duplicate fetch; stale-tail recovery below
		// independently catches any authoritative history missed before this deploy.
		if (this.autoState.last_30m_key == null) {
			this.autoState.last_30m_key = scheduleBucketKey(cairoTime(Date.now()), 30);
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
				(item) =>
					item.interval === interval &&
					(item.before ?? null) === null &&
					item.target_gap == null &&
					!item.reason.includes("bootstrap"),
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
				target_gap: item.target_gap ?? null,
			})),
			bootstrap_pending: state.bootstrap_pending,
			recovery_pending: state.queue.some((item) =>
				item.reason.includes("recovery") || item.reason.includes("backfill"),
			),
			recovery_noop_cooldowns_active: Object.values(
				state.recovery_noop_until_ms ?? {},
			).filter((until) => until > Date.now()).length,
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
			continuity: {
				fast_overlap_buckets: CONTINUITY_FAST_OVERLAP_BUCKETS,
				initial_scan_limit: CONTINUITY_INITIAL_SCAN_LIMIT,
				deep_page_limit: CONTINUITY_DEEP_PAGE_LIMIT,
				deep_rotation_index: state.continuity_deep_rotation_index ?? 0,
				fast_checkpoint: state.continuity_fast_checkpoint ?? {},
				deep_before: state.continuity_deep_before ?? {},
				active_gaps: Object.values(state.continuity_active_gaps ?? {}),
			},
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
				"scheduler_idle_wake": "every 5 minutes; busy/rate-window follow-up may use 1 minute",
			},
		};
	}

	private async persistAutoState() {
		this.ensureAutoState();
		const state = this.autoState!;
		if (state.last_error !== null && this.isStorageWriteErrorMessage(state.last_error)) {
			// A successful write below proves the old quota error is stale.
			state.last_error = null;
		}
		try {
			await this.ctx.storage.put(AUTO_STATE_KEY, state);
			this.markStorageWriteSuccess();
			return true;
		} catch (error) {
			this.markStorageWriteFailure(error);
			return false;
		}
	}

	private async persistAutoStateIfChanged(beforeFingerprint: string) {
		if (this.autoStateFingerprint() === beforeFingerprint) {
			return true;
		}
		return this.persistAutoState();
	}

	private async scheduleAutoAlarm(delayMs = AUTO_IDLE_ALARM_MS) {
		this.ensureAutoState();
		if (!this.autoState!.enabled) {
			return false;
		}
		try {
			await this.ctx.storage.setAlarm(Date.now() + delayMs);
			this.markStorageWriteSuccess();
			return true;
		} catch (error) {
			this.markStorageWriteFailure(error);
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
			stateStale && scheduled !== null && scheduled > now + 2 * AUTO_IDLE_ALARM_MS;

		if (alarmMissingOrPast || alarmSuspiciouslyFar) {
			const next = now + 1_000;
			try {
				await this.ctx.storage.setAlarm(next);
				this.markStorageWriteSuccess();
				return {
					enabled: true,
					rearmed: true,
					scheduled_alarm_ms: next,
					write_error: null as string | null,
				};
			} catch (error) {
				const message = this.markStorageWriteFailure(error);
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

	private liveRowsForContinuityWindow(
		interval: ContinuityTrackedInterval,
		startDatetime: string,
		endDatetime: string,
	) {
		const oneMinute: NormalizedCandle[] = [];
		for (const live of this.candles) {
			if (live.datetime >= startDatetime && live.datetime <= endDatetime) {
				oneMinute.push(this.liveCandleToNormalized(live));
			}
		}
		if (
			this.currentCandle &&
			this.currentCandle.datetime >= startDatetime &&
			this.currentCandle.datetime <= endDatetime
		) {
			oneMinute.push(this.liveCandleToNormalized(this.currentCandle));
		}
		oneMinute.sort((a, b) => a.datetime.localeCompare(b.datetime));
		if (interval === "1min") return oneMinute;

		const buckets = new Map<string, NormalizedCandle[]>();
		for (const candle of oneMinute) {
			const bucketStart = interval === "4h"
				? fourHourBucketStart(candle.datetime)
				: intradayBucketStart(
					candle.datetime,
					this.continuityIntervalMinutes(interval),
				);
			if (!bucketStart || bucketStart < startDatetime || bucketStart > endDatetime) continue;
			const rows = buckets.get(bucketStart) ?? [];
			rows.push(candle);
			buckets.set(bucketStart, rows);
		}

		const derived: NormalizedCandle[] = [];
		for (const [bucketStart, rows] of buckets) {
			const duration = this.continuityIntervalMinutes(interval);
			const candle = this.aggregateRows(
				interval,
				bucketStart,
				addMinutesToCairoDatetime(bucketStart, duration),
				rows,
				"provisional_live_memory",
			);
			if (candle) derived.push(candle);
		}
		return derived.sort((a, b) => a.datetime.localeCompare(b.datetime));
	}

	private async effectiveRowsForContinuity(
		interval: ContinuityTrackedInterval,
		limit = CONTINUITY_INITIAL_SCAN_LIMIT,
	) {
		const confirmedPage = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(interval),
			reverse: true,
			limit,
		});
		const confirmed = Array.from(confirmedPage.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		const nowCairo = cairoTime(Date.now());
		const start = confirmed[0]?.datetime ?? addMinutesToCairoDatetime(
			nowCairo,
			-this.continuityIntervalMinutes(interval) * Math.max(2, limit),
		);
		const provisional = this.liveRowsForContinuityWindow(
			interval,
			start,
			nowCairo,
		);
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of provisional) merged.set(candle.datetime, candle);
		for (const candle of confirmed) merged.set(candle.datetime, candle);
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

	private scanContinuityRows(
		interval: ContinuityTrackedInterval,
		rows: NormalizedCandle[],
	) {
		const gaps: ContinuityGap[] = [];
		let boundaryOmission: ContinuityGap | null = null;
		const sorted = rows.slice().sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		for (let i = 0; i < sorted.length - 1; i++) {
			const before = sorted[i];
			const after = sorted[i + 1];
			const missing = this.countMissingMarketBuckets(
				before.datetime,
				after.datetime,
				interval,
			);
			if (missing.count < 1 || !missing.first || !missing.last) continue;
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
				boundaryOmission ??= candidate;
				continue;
			}
			gaps.push(candidate);
		}
		return { gaps, boundary_omission: boundaryOmission };
	}

	private continuityGapKey(gap: ContinuityGap) {
		return `${gap.interval}|${gap.missing_from}|${gap.missing_to}`;
	}

	private recordContinuityGap(gap: ContinuityGap) {
		this.ensureAutoState();
		this.autoState!.continuity_active_gaps ??= {};
		this.autoState!.continuity_active_gaps![this.continuityGapKey(gap)] = gap;
	}

	private removeContinuityGap(gap: ContinuityGap) {
		this.ensureAutoState();
		if (!this.autoState!.continuity_active_gaps) return;
		delete this.autoState!.continuity_active_gaps![this.continuityGapKey(gap)];
	}

	private activeContinuityGaps(interval: ContinuityTrackedInterval) {
		this.ensureAutoState();
		return Object.values(this.autoState!.continuity_active_gaps ?? {})
			.filter((gap) => gap.interval === interval)
			.sort((a, b) => b.missing_from.localeCompare(a.missing_from));
	}

	private async verifyContinuityGap(gap: ContinuityGap) {
		const interval = gap.interval as ContinuityTrackedInterval;
		const confirmed = await this.listNormalizedRange(
			interval,
			gap.before_datetime,
			gap.after_datetime,
		);
		const live = this.liveRowsForContinuityWindow(
			interval,
			gap.before_datetime,
			gap.after_datetime,
		);
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of live) merged.set(candle.datetime, candle);
		for (const candle of confirmed) merged.set(candle.datetime, candle);
		// Losing either anchor is not evidence that the gap was repaired. Keep the
		// original blocker active so a destructive or incomplete range mutation can
		// never be mistaken for successful recovery.
		if (
			!merged.has(gap.before_datetime) ||
			!merged.has(gap.after_datetime)
		) {
			return gap;
		}
		const scan = this.scanContinuityRows(
			interval,
			Array.from(merged.values()),
		);
		return scan.gaps[scan.gaps.length - 1] ?? null;
	}

	private recoverySourceIntervalForGap(gap: ContinuityGap): RecoveryInterval | null {
		if (gap.interval === "3min") return "1min";
		if (gap.interval === "4h") return "1h";
		return (RECOVERY_INTERVALS as readonly string[]).includes(gap.interval)
			? (gap.interval as RecoveryInterval)
			: null;
	}

	private recoveryOutputsizeForSourceGap(
		sourceInterval: RecoveryInterval,
		gap: ContinuityGap,
	) {
		const sourceMinutes = this.continuityIntervalMinutes(sourceInterval);
		const sourceBuckets = Math.max(
			1,
			Math.ceil(gap.missing_market_minutes / sourceMinutes),
		);
		return Math.min(
			RECOVERY_MAX_OUTPUTSIZE,
			Math.max(AUTO_INCREMENTAL_OUTPUTSIZE, sourceBuckets + RECOVERY_BUFFER_ROWS),
		);
	}

	private enqueueRecoveryForContinuityGap(
		gap: ContinuityGap,
		reasonPrefix: string,
	) {
		const sourceInterval = this.recoverySourceIntervalForGap(gap);
		if (!sourceInterval) return false;
		const suffix = gap.interval === "3min"
			? "3m_source_backfill"
			: gap.interval === "4h"
				? "4h_source_backfill"
				: "backfill";
		return this.enqueueAutoRepair(
			sourceInterval,
			this.recoveryOutputsizeForSourceGap(sourceInterval, gap),
			this.recoveryTargetEndDate(sourceInterval, gap),
			`${reasonPrefix}_${suffix}`,
			gap,
		);
	}

	private async auditContinuity(
		interval: AnalysisNormalizedInterval,
		cache?: Map<AnalysisNormalizedInterval, ContinuityAudit>,
	): Promise<ContinuityAudit> {
		const cached = cache?.get(interval);
		if (cached) return cached;
		const tracked = interval as ContinuityTrackedInterval;
		this.ensureAutoState();
		const state = this.autoState!;
		state.continuity_fast_checkpoint ??= {};

		// A gap discovered by either the fast or deep scanner is kept explicitly.
		// Verify only its exact bounded span; never rescan a thousand recent rows
		// just to learn that the same old omission is still present.
		let activeGap: ContinuityGap | null = null;
		const known = this.activeContinuityGaps(tracked);
		if (known.length > 0) {
			const verified = await this.verifyContinuityGap(known[0]);
			if (verified) {
				if (this.continuityGapKey(verified) !== this.continuityGapKey(known[0])) {
					this.removeContinuityGap(known[0]);
					this.recordContinuityGap(verified);
				}
				activeGap = verified;
			} else {
				this.removeContinuityGap(known[0]);
			}
		}

		const latestPage = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(tracked),
			reverse: true,
			limit: 1,
		});
		const latestConfirmed = Array.from(latestPage.values())[0] ?? null;
		const checkpoint = state.continuity_fast_checkpoint[tracked] ?? null;
		const overlapStart = checkpoint
			? addMinutesToCairoDatetime(
				checkpoint,
				-this.continuityIntervalMinutes(tracked) * CONTINUITY_FAST_OVERLAP_BUCKETS,
			)
			: null;
		const confirmed = latestConfirmed
			? await this.listRecentConfirmedWindow(
				tracked,
				overlapStart,
				latestConfirmed.datetime,
				CONTINUITY_INITIAL_SCAN_LIMIT,
			)
			: [];
		const nowCairo = cairoTime(Date.now());
		const liveStart = confirmed[0]?.datetime ?? overlapStart ?? addMinutesToCairoDatetime(
			nowCairo,
			-this.continuityIntervalMinutes(tracked) * CONTINUITY_FAST_OVERLAP_BUCKETS,
		);
		const provisional = this.liveRowsForContinuityWindow(
			tracked,
			liveStart,
			nowCairo,
		);
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of provisional) merged.set(candle.datetime, candle);
		for (const candle of confirmed) merged.set(candle.datetime, candle);
		const rows = Array.from(merged.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		const fastScan = this.scanContinuityRows(tracked, rows);
		for (const discovered of fastScan.gaps) {
			this.recordContinuityGap(discovered);
		}
		const fastGap = fastScan.gaps[fastScan.gaps.length - 1] ?? null;
		if (latestConfirmed) {
			state.continuity_fast_checkpoint[tracked] = latestConfirmed.datetime;
		}

		const gapCandidates = [activeGap, fastGap]
			.filter((value): value is ContinuityGap => value !== null)
			.sort((a, b) => b.missing_from.localeCompare(a.missing_from));
		const gap = gapCandidates[0] ?? null;
		const latest = rows.length > 0 ? rows[rows.length - 1] : latestConfirmed;
		const currentProvisional = provisional.length > 0
			? provisional[provisional.length - 1]
			: null;
		const latestEffectiveDatetime = [
			latest?.datetime ?? null,
			currentProvisional?.datetime ?? null,
		].filter((value): value is string => value !== null).sort().reverse()[0] ?? null;
		const latestMs = latestEffectiveDatetime
			? cairoDatetimeToMs(latestEffectiveDatetime)
			: null;
		const allowedLagMs = Math.max(
			5 * 60_000,
			this.continuityIntervalMinutes(tracked) * 2 * 60_000,
		);
		const effectiveFresh =
			isClosedMarketCairoDatetime(nowCairo) ||
			(latestMs !== null && Date.now() - latestMs <= allowedLagMs);
		const latestConfirmedMs = latestConfirmed
			? cairoDatetimeToMs(latestConfirmed.datetime)
			: null;
		const authoritativeLagMinutes = latestConfirmedMs !== null
			? Math.max(0, Math.floor((Date.now() - latestConfirmedMs) / 60_000))
			: null;
		const authoritativeFresh =
			isClosedMarketCairoDatetime(nowCairo) ||
			(latestConfirmedMs !== null && Date.now() - latestConfirmedMs <= allowedLagMs);

		const result: ContinuityAudit = {
			interval: tracked,
			latest_effective_datetime: latestEffectiveDatetime,
			latest_confirmed_datetime: latestConfirmed?.datetime ?? null,
			current_provisional_datetime: currentProvisional?.datetime ?? null,
			gap,
			boundary_omission: fastScan.boundary_omission,
			effective_fresh: effectiveFresh,
			authoritative_fresh: authoritativeFresh,
			authoritative_lag_minutes: authoritativeLagMinutes,
		};
		cache?.set(interval, result);
		return result;
	}

	private enqueuePendingActiveGapRecoveries() {
		this.ensureAutoState();
		const gaps = Object.values(this.autoState!.continuity_active_gaps ?? {})
			.sort((a, b) => b.missing_from.localeCompare(a.missing_from));
		let enqueued = 0;
		for (const gap of gaps) {
			// Queue every known exact gap. Execution is still globally throttled by
			// AUTO_MAX_REQUESTS_PER_WINDOW=7, so removing the old 3-gap enqueue cap
			// does not create a provider burst. Existing items are deduplicated.
			if (this.enqueueRecoveryForContinuityGap(gap, "active_gap_retry")) {
				enqueued++;
			}
		}
		return enqueued;
	}

	private async runRotatingDeepContinuityScan() {
		this.ensureAutoState();
		const state = this.autoState!;
		state.continuity_deep_before ??= {};
		const index = Math.max(0, state.continuity_deep_rotation_index ?? 0);
		const interval = CONTINUITY_TRACKED_INTERVALS[
			index % CONTINUITY_TRACKED_INTERVALS.length
		];
		state.continuity_deep_rotation_index =
			(index + 1) % CONTINUITY_TRACKED_INTERVALS.length;

		const before = state.continuity_deep_before[interval] ?? null;
		let boundary: NormalizedCandle | null = null;
		if (before) {
			boundary =
				(await this.ctx.storage.get<NormalizedCandle>(
					normalizedCandleKey(interval, before),
				)) ?? null;
		}
		const page = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix(interval),
			...(before
				? { end: normalizedCandleKey(interval, before) }
				: {}),
			reverse: true,
			limit: CONTINUITY_DEEP_PAGE_LIMIT,
		});
		const rows = Array.from(page.values());
		if (boundary) rows.push(boundary);
		rows.sort((a, b) => a.datetime.localeCompare(b.datetime));

		if (rows.length >= 2) {
			const scan = this.scanContinuityRows(interval, rows);
			for (const gap of scan.gaps) {
				this.recordContinuityGap(gap);
			}
			// Keep recovery controlled: retain every discovered gap, but enqueue only
			// the newest one from this page. Older gaps remain active and will be
			// processed after newer blockers or on later rotations.
			const newestGap = scan.gaps[scan.gaps.length - 1] ?? null;
			if (newestGap) {
				this.enqueueRecoveryForContinuityGap(newestGap, "deep_continuity");
			}
		}

		const pageRows = Array.from(page.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		if (pageRows.length === 0 || page.size < CONTINUITY_DEEP_PAGE_LIMIT) {
			state.continuity_deep_before[interval] = null;
		} else {
			state.continuity_deep_before[interval] = pageRows[0].datetime;
		}
		return {
			interval,
			rows_scanned: rows.length,
			next_before: state.continuity_deep_before[interval] ?? null,
		};
	}


	private async synthesizeSmallOneMinuteGapsFromRows(rows: NormalizedCandle[]) {
		if (rows.length < 2) {
			return { created: 0, candles: [] as string[] };
		}
		const sorted = rows.slice().sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
		const candidates: NormalizedCandle[] = [];
		for (let i = 0; i < sorted.length - 1; i++) {
			const before = sorted[i];
			const after = sorted[i + 1];
			const missing = this.countMissingMarketBuckets(
				before.datetime,
				after.datetime,
				"1min",
			);
			if (
				missing.count < 1 ||
				missing.count > SYNTHETIC_MICRO_GAP_MAX_1M ||
				!missing.first ||
				!missing.last
			) continue;
			if (hasDeclaredClosureBetween(before.datetime, after.datetime)) continue;

			const start = Number(before.close);
			const end = Number(after.open);
			if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
			const step = (end - start) / missing.count;
			for (let n = 0; n < missing.count; n++) {
				const datetime = addMinutesToCairoDatetime(before.datetime, n + 1);
				const open = start + step * n;
				const close = start + step * (n + 1);
				const expectedClose = addMinutesToCairoDatetime(datetime, 1);
				candidates.push({
					timeframe: "1min",
					datetime,
					open_time: datetime,
					expected_close_time: expectedClose,
					open,
					high: Math.max(open, close),
					low: Math.min(open, close),
					close,
					status: "CLOSED",
					source: "synthetic_bridge_approximation",
					provisional: false,
					confirmed: true,
					synthetic_gap: true,
					gap_adjusted: false,
					stored_at_ms: Date.now(),
				});
			}
		}
		if (candidates.length === 0) {
			return { created: 0, candles: [] as string[] };
		}
		const stats = await this.upsertNormalizedCandidates(candidates);
		return {
			created: stats.written,
			unchanged: stats.unchanged,
			candles: candidates.map((c) => c.datetime),
			method:
				"open=previous close; close=next open (linear bridge for up to five candles); high/low=body extrema; no invented wicks",
		};
	}

	private async synthesizeSmallOneMinuteGaps(
		limit = SYNTHETIC_RECENT_SCAN_LIMIT,
	) {
		// Routine fallback scans only a bounded recent window. Bootstrap may pass
		// the original 1000-row horizon once, while historical gaps are otherwise
		// handled by the deep scanner and the targeted-range variant below.
		const rows = await this.effectiveRowsForContinuity(
			"1min",
			limit,
		);
		return this.synthesizeSmallOneMinuteGapsFromRows(rows);
	}

	private async synthesizeSmallOneMinuteGapsInRange(
		fromDatetime: string,
		toDatetime: string,
	) {
		const confirmed = await this.listNormalizedRange(
			"1min",
			fromDatetime,
			toDatetime,
		);
		const live = this.liveRowsForContinuityWindow(
			"1min",
			fromDatetime,
			toDatetime,
		);
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of live) merged.set(candle.datetime, candle);
		for (const candle of confirmed) merged.set(candle.datetime, candle);
		return this.synthesizeSmallOneMinuteGapsFromRows(
			Array.from(merged.values()),
		);
	}

	private async syntheticApproximationReport() {
		const page = await this.ctx.storage.list<NormalizedCandle>({
			prefix: normalizedPrefix("1min"),
			reverse: true,
			limit: CONTINUITY_INITIAL_SCAN_LIMIT,
		});
		const synthetic = Array.from(page.values())
			.filter(
				(candle) =>
					candle.synthetic_gap === true &&
					candle.source === "synthetic_bridge_approximation",
			)
			.sort((a, b) => a.datetime.localeCompare(b.datetime));
		return {
			count: synthetic.length,
			candles: synthetic.map((c) => c.datetime),
			note:
				"Informational only. These isolated 1M candles were approximated automatically and remain fully analysis-eligible. Do not repeatedly downgrade or restate this warning in market analysis.",
		};
	}

	private recoveryTargetEndDate(
		sourceInterval: RecoveryInterval,
		gap: ContinuityGap,
	) {
		// Twelve Data end_date behavior can be inclusive/exclusive around a
		// missing bucket. Move the cursor a few SOURCE buckets past the first
		// known candle after the gap so the returned page overlaps both sides.
		const overlapMinutes =
			this.continuityIntervalMinutes(sourceInterval) *
			RECOVERY_TARGET_OVERLAP_BUCKETS;
		return addMinutesToCairoDatetime(gap.after_datetime, overlapMinutes);
	}

	private recoveryNoopKey(
		interval: RecoveryInterval,
		before: string | null,
	) {
		return `${interval}|${before ?? "latest"}`;
	}

	private isRecoveryNoopCoolingDown(
		interval: RecoveryInterval,
		before: string | null,
	) {
		this.ensureAutoState();
		const until =
			this.autoState!.recovery_noop_until_ms?.[
				this.recoveryNoopKey(interval, before)
			] ?? 0;
		return until > Date.now();
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
		targetGap: ContinuityGap | null = null,
	) {
		this.ensureAutoState();
		const state = this.autoState!;
		if (
			targetGap !== null &&
			this.isRecoveryNoopCoolingDown(interval, before)
		) {
			// Cooldown applies only to a specific exact gap that the provider could
			// not fill. A stale authoritative tail must keep catching up normally.
			return false;
		}
		const existing = state.queue.find(
			(item) => item.interval === interval && (item.before ?? null) === before,
		);
		if (existing) {
			existing.outputsize = Math.max(existing.outputsize, outputsize);
			if (!existing.reason.includes(reason)) existing.reason += `|${reason}`;
			if (targetGap) existing.target_gap = targetGap;
			return true;
		}
		state.queue.push({
			interval,
			outputsize: Math.min(RECOVERY_MAX_OUTPUTSIZE, Math.max(1, outputsize)),
			reason,
			enqueued_ms: Date.now(),
			attempts: 0,
			before,
			target_gap: targetGap,
		});
		return true;
	}

	private async enqueueStaleRecovery(nowMs: number) {
		this.ensureAutoState();
		this.autoState!.last_recovery_audit_ms = nowMs;
		if (isClosedMarketCairoDatetime(cairoTime(nowMs))) return;
		const auditCache = new Map<AnalysisNormalizedInterval, ContinuityAudit>();
		for (const interval of RECOVERY_INTERVALS) {
			const audit = await this.auditContinuity(interval, auditCache);
			if (audit.gap) {
				this.enqueueRecoveryForContinuityGap(audit.gap, "auto_recovery");
				continue;
			}
			if (!audit.authoritative_fresh) {
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
		let fourHourAudit = await this.auditContinuity("4h", auditCache);
		if (fourHourAudit.gap) {
			const oneHourAudit = await this.auditContinuity("1h", auditCache);
			// First rebuild the derived 4H layer from already-authoritative 1H.
			// Only spend a REST recovery request if the source itself cannot fix it.
			if (oneHourAudit.gap === null && oneHourAudit.effective_fresh) {
				await this.deriveFourHourRangeFromOneHour(
					fourHourAudit.gap.missing_from,
					fourHourAudit.gap.missing_to,
				);
				auditCache.delete("4h");
				fourHourAudit = await this.auditContinuity("4h", auditCache);
			}
			if (fourHourAudit.gap) {
				this.enqueueRecoveryForContinuityGap(
					fourHourAudit.gap,
					"auto_recovery",
				);
			}
		}

		// One small historical page is scanned per recovery audit. The cursor is
		// persisted, so repeated alarms walk the whole retained history without
		// turning every five-minute check into a full historical re-read.
		await this.runRotatingDeepContinuityScan();
		// Queue every persisted exact gap on each audit. Exact-gap cooldowns are
		// enforced inside enqueueAutoRepair(); the global 7/minute executor limit
		// controls provider load while preventing an arbitrary 3-gap bottleneck.
		this.enqueuePendingActiveGapRecoveries();
	}


	private auditHasBlockingProblem(audit: ContinuityAudit) {
		if (audit.gap !== null || !audit.effective_fresh) return true;
		// Native REST layers must be authoritative-fresh as well. Derived 3M/4H
		// are validated through their effective/source continuity paths instead.
		return (RECOVERY_INTERVALS as readonly string[]).includes(audit.interval)
			? !audit.authoritative_fresh
			: false;
	}

	private async ensureGoldDataReady() {
		try {
			const watchdog = await this.ensureAutoWatchdog();
			await this.ensureConnection();
			this.ensureAutoState();

			// Cache continuity audits only for this readiness execution. The underlying
			// audit is already incremental; this cache additionally prevents duplicate
			// bounded reads of the same timeframe before anything has changed. It is
			// cleared after operations that may mutate normalized storage.
			const auditCache = new Map<AnalysisNormalizedInterval, ContinuityAudit>();

			const beforeAudits: ContinuityAudit[] = [];
			for (const interval of RECOVERY_INTERVALS) {
				const audit = await this.auditContinuity(interval, auditCache);
				beforeAudits.push(audit);
				if (audit.gap) {
					this.enqueueRecoveryForContinuityGap(audit.gap, "gpt_recovery");
				} else if (!audit.authoritative_fresh) {
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

			// 3M is derived from 1M. Never double-charge recovery when 1M already
			// has the blocking source gap. If 1M is healthy, rebuild 3M locally
			// first; only use REST if a source-side problem remains.
			let threeMinuteBefore = await this.auditContinuity("3min", auditCache);
			beforeAudits.push(threeMinuteBefore);
			if (
				threeMinuteBefore.gap !== null ||
				!threeMinuteBefore.effective_fresh
			) {
				const oneMinuteBefore =
					beforeAudits.find((audit) => audit.interval === "1min") ??
					await this.auditContinuity("1min", auditCache);

				if (oneMinuteBefore.gap === null && oneMinuteBefore.effective_fresh) {
					if (threeMinuteBefore.gap) {
						await this.deriveThreeMinuteRangeFromOneMinute(
							threeMinuteBefore.gap.missing_from,
							threeMinuteBefore.gap.missing_to,
						);
					} else {
						await this.deriveRecentThreeMinuteFromOneMinute();
					}
					auditCache.delete("3min");
					threeMinuteBefore = await this.auditContinuity("3min", auditCache);
				}

				if (
					(threeMinuteBefore.gap !== null ||
						!threeMinuteBefore.effective_fresh) &&
					oneMinuteBefore.gap === null
				) {
					if (threeMinuteBefore.gap) {
						this.enqueueRecoveryForContinuityGap(
							threeMinuteBefore.gap,
							"gpt_recovery",
						);
					} else {
						this.enqueueAutoRepair(
							"1min",
							this.recoveryOutputsizeForStaleness(
								"1min",
								oneMinuteBefore.latest_confirmed_datetime,
							),
							null,
							"gpt_recovery_3m_source_backfill",
						);
					}
				}
			}

			// 4H is derived from confirmed 1H. If a closed 4H bucket is missing,
			// force a targeted authoritative 1H refresh around that boundary.
			let fourHourBefore = await this.auditContinuity("4h", auditCache);
			beforeAudits.push(fourHourBefore);
			if (fourHourBefore.gap) {
				const oneHourBefore =
					beforeAudits.find((audit) => audit.interval === "1h") ??
					await this.auditContinuity("1h", auditCache);

				if (oneHourBefore.gap === null && oneHourBefore.effective_fresh) {
					await this.deriveFourHourRangeFromOneHour(
						fourHourBefore.gap.missing_from,
						fourHourBefore.gap.missing_to,
					);
					auditCache.delete("4h");
					fourHourBefore = await this.auditContinuity("4h", auditCache);
				}

				if (fourHourBefore.gap) {
					this.enqueueRecoveryForContinuityGap(
						fourHourBefore.gap,
						"gpt_recovery",
					);
				}
			}

			const repairRequested = beforeAudits.some((audit) =>
				this.auditHasBlockingProblem(audit),
			);

			const queueHasWork = this.autoState!.queue.length > 0;
			if (repairRequested || queueHasWork) {
				await this.persistAutoState();
				await this.processAutoQueue();
				auditCache.clear();
				await this.scheduleAutoAlarm();
			}

			// v15.2: real recovery gets first priority. If one or two isolated 1M
			// candles are still absent, create deterministic bridge candles and rebuild
			// 3M so analysis can proceed normally.
			const syntheticBridge = await this.synthesizeSmallOneMinuteGaps();
			if (syntheticBridge.created > 0) {
				auditCache.delete("1min");
				await this.deriveRecentThreeMinuteFromOneMinute();
				auditCache.delete("3min");
			}

			// v15.5: if the 3M gap is historical enough to fall outside the 1M
			// continuity scan, inspect that exact historical 1M source window. Normal
			// REST recovery has already run above; only now may the bounded synthetic
			// bridge policy fill the source hole. Then rebuild the missing 3M rows.
			const historicalSourceBridge =
				await this.synthesizeHistoricalOneMinuteGapForThreeMinuteAudit(auditCache);
			if (historicalSourceBridge.created > 0) {
				auditCache.delete("1min");
				auditCache.delete("3min");
				await this.forceRebuildHistoricalThreeMinuteGap(auditCache);
				auditCache.delete("3min");
			}

			// Always attempt a direct historical 3M rebuild from any exact 1M rows
			// that already exist, even when no synthetic source row was required.
			const historicalThreeMinuteRebuild =
				await this.forceRebuildHistoricalThreeMinuteGap(auditCache);
			if (
				Number((historicalThreeMinuteRebuild as { written?: number }).written ?? 0) > 0 ||
				Number((historicalThreeMinuteRebuild as { deleted?: number }).deleted ?? 0) > 0
			) {
				auditCache.delete("3min");
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
				afterAudits.push(await this.auditContinuity(interval, auditCache));
			}

			const remainingProblems = afterAudits.filter((audit) =>
				this.auditHasBlockingProblem(audit),
			);

			// Fallback only: v15.2 normally bridges one/two isolated 1M omissions. If a
			// bridge cannot be formed yet (for example no next real candle exists), keep
			// the older restricted-window safety mode rather than inventing an endpoint.
			const oneMinuteAfter = afterAudits.find((audit) => audit.interval === "1min") ?? null;
			const threeMinuteAfter = afterAudits.find((audit) => audit.interval === "3min") ?? null;
			const higherAfter = afterAudits.filter((audit) =>
				["5min", "15min", "30min", "1h", "4h"].includes(audit.interval),
			);
			const isolatedOneMinuteGap =
				oneMinuteAfter?.gap !== null &&
				oneMinuteAfter?.gap !== undefined &&
				oneMinuteAfter.gap.missing_buckets === 1 &&
				oneMinuteAfter.gap.missing_market_minutes === 1 &&
				oneMinuteAfter.effective_fresh === true;
			const threeMinuteCompatible =
				threeMinuteAfter !== null &&
				threeMinuteAfter.effective_fresh === true &&
				(threeMinuteAfter.gap === null ||
					(threeMinuteAfter.gap.missing_buckets === 1 &&
					 threeMinuteAfter.gap.missing_market_minutes === 3));
			const higherFramesHealthy = higherAfter.every((audit) =>
				!this.auditHasBlockingProblem(audit),
			);
			const onlyMicroProblems = remainingProblems.every((audit) =>
				audit.interval === "1min" || audit.interval === "3min",
			);
			const isolatedMicroGapAllowed = Boolean(
				isolatedOneMinuteGap &&
				threeMinuteCompatible &&
				higherFramesHealthy &&
				onlyMicroProblems,
			);

			const approximationReport = await this.syntheticApproximationReport();

			const marketClosed = isClosedMarketCairoDatetime(cairoTime(Date.now()));
			const analysisReady = marketClosed
				? remainingProblems.every((audit) => audit.gap === null)
				: remainingProblems.length === 0 || isolatedMicroGapAllowed;

			const restrictedGap = isolatedMicroGapAllowed ? oneMinuteAfter!.gap : null;
			const restrictedThreeMinuteBucket = restrictedGap
				? threeMinuteBucketStart(restrictedGap.missing_from)
				: null;

			return {
				status: "ok",
				build_version: BUILD_VERSION,
				symbol: SYMBOL,
				timezone: TIMEZONE,
				watchdog,
				repair_requested: repairRequested,
				repair_status: analysisReady
					? isolatedMicroGapAllowed
						? "ready_with_isolated_micro_gap"
						: "ready"
					: this.autoState!.queue.length > 0
						? "queued_or_rate_limited"
						: "partial",
				analysis_ready: analysisReady,
				synthetic_approximation_report: approximationReport,
				readiness_mode: isolatedMicroGapAllowed
					? "isolated_micro_gap_non_blocking"
					: analysisReady
						? "full"
						: "degraded",
				restricted_timeframes: isolatedMicroGapAllowed ? ["1min", "3min"] : [],
				restricted_window: isolatedMicroGapAllowed && restrictedGap
					? {
						one_minute_missing_from: restrictedGap.missing_from,
						one_minute_missing_to: restrictedGap.missing_to,
						three_minute_bucket: restrictedThreeMinuteBucket,
					}
					: null,
				execution_policy: isolatedMicroGapAllowed
					? "Do not confirm or invalidate 1M/3M MSS, BOS, FVG, UC/OB, sweep, PH/PL, or other candle-derived events whose evidence crosses the restricted window. Use 5M as the smallest fully trusted execution timeframe until the window ages out. No OHLC is synthesized."
					: "normal",
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
		const thirtyKey = scheduleBucketKey(nowCairo, 30);
		const hourKey = scheduleBucketKey(nowCairo, 60);
		const fourKey = fourHourScheduleKey(nowCairo);
		const currentDateKey = dateKey(nowCairo);
		const currentMonthKey = monthKey(nowCairo);

		if (fiveKey !== state.last_5m_key) {
			state.last_5m_key = fiveKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["5min", "1min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"5m_cadence",
				);
			}
		}

		if (fifteenKey !== state.last_15m_key) {
			state.last_15m_key = fifteenKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["15min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"15m_cadence",
				);
			}
		}

		if (thirtyKey !== state.last_30m_key) {
			state.last_30m_key = thirtyKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["30min"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"30m_cadence",
				);
			}
		}

		if (hourKey !== state.last_hour_key) {
			state.last_hour_key = hourKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["1h"],
					AUTO_INCREMENTAL_OUTPUTSIZE,
					"hour_cadence",
				);
			}
		}

		if (fourKey !== state.last_4h_key) {
			state.last_4h_key = fourKey;
			if (!marketClosed) {
				this.enqueueAutoIntervals(
					["4h"],
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

	private async postAutoRefresh(
		item: AutoQueueItem,
		result?: {
			request_oldest?: string | null;
			request_newest?: string | null;
		},
	) {
		const bootstrap = item.reason.includes("bootstrap");
		const backfill = item.reason.includes("backfill");

		if (isNormalizedRestInterval(item.interval)) {
			let normalizedRange: { from: string; to: string } | null = null;
			if (bootstrap) {
				await this.normalizeStoredInterval(item.interval);
			} else if (
				backfill &&
				result?.request_oldest &&
				result?.request_newest
			) {
				const duration = normalizedIntervalMinutes(item.interval);
				normalizedRange = {
					from: addMinutesToCairoDatetime(
						result.request_oldest,
						-duration * TARGETED_REPAIR_OVERLAP_BUCKETS,
					),
					to: addMinutesToCairoDatetime(
						result.request_newest,
						duration * TARGETED_REPAIR_OVERLAP_BUCKETS,
					),
				};
				await this.normalizeStoredIntervalRange(
					item.interval,
					normalizedRange.from,
					normalizedRange.to,
				);
			} else {
				await this.normalizeRecentStoredInterval(item.interval);
			}

			if (item.interval === "1min") {
				if (normalizedRange) {
					await this.synthesizeSmallOneMinuteGapsInRange(
						normalizedRange.from,
						normalizedRange.to,
					);
				} else {
					await this.synthesizeSmallOneMinuteGaps(
						bootstrap
							? CONTINUITY_INITIAL_SCAN_LIMIT
							: SYNTHETIC_RECENT_SCAN_LIMIT,
					);
				}
				if (bootstrap) {
					await this.deriveThreeMinuteFromOneMinute();
				} else if (normalizedRange) {
					await this.deriveThreeMinuteRangeFromOneMinute(
						normalizedRange.from,
						normalizedRange.to,
					);
				} else {
					await this.deriveRecentThreeMinuteFromOneMinute();
				}
			}

			if (item.interval === "1h") {
				if (bootstrap) {
					await this.deriveFourHourFromOneHour();
				} else if (normalizedRange) {
					await this.deriveFourHourRangeFromOneHour(
						normalizedRange.from,
						normalizedRange.to,
					);
				} else {
					await this.deriveRecentFourHourFromOneHour();
				}
			}
			return;
		}

		if (isHigherNativeInterval(item.interval)) {
			// These layers are small (daily/weekly/monthly) and remain native REST.
			// Keeping their existing full normalization avoids changing higher-timeframe
			// gap semantics while the expensive intraday backfills stay targeted.
			await this.normalizeHigherNativeInterval(item.interval);
		}
	}

	private async recoveryGapStillPresentForItem(item: AutoQueueItem) {
		if (!this.isRecoveryWork(item)) return false;

		if (item.target_gap) {
			const remaining = await this.verifyContinuityGap(item.target_gap);
			if (remaining) {
				if (
					this.continuityGapKey(remaining) !==
					this.continuityGapKey(item.target_gap)
				) {
					this.removeContinuityGap(item.target_gap);
				}
				this.recordContinuityGap(remaining);
				item.target_gap = remaining;
				return true;
			}
			this.removeContinuityGap(item.target_gap);
			return false;
		}

		// Staleness/bootstrap items have no exact missing span. Fall back to the
		// lightweight incremental audit rather than a full recent-history scan.
		let auditInterval: AnalysisNormalizedInterval = item.interval;
		if (item.reason.includes("3m_source_backfill")) auditInterval = "3min";
		if (item.reason.includes("4h_source_backfill")) auditInterval = "4h";
		const audit = await this.auditContinuity(auditInterval);
		if (!audit.gap) return false;
		const sourceInterval = this.recoverySourceIntervalForGap(audit.gap);
		if (!sourceInterval) return false;
		const expectedCursor = this.recoveryTargetEndDate(
			sourceInterval,
			audit.gap,
		);
		return (item.before ?? null) === expectedCursor;
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

			if (this.isRecoveryBudgetGuardedWork(item)) {
				const check = await this.canRunRecoveryItem(item);
				if (!check.allowed) {
					// Full bootstrap pauses at the legacy internal reservation, but current
					// cadence and targeted recovery remain eligible to run.
					const liveIndex = state.queue.findIndex(
						(candidate) => !this.isRecoveryBudgetGuardedWork(candidate),
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
			// For guarded recovery/bootstrap, checkpoint queue removal before heavy
			// work. Routine current-cadence items are diff-upserts and are persisted
			// once after processing, avoiding one metadata write per item.
			if (this.isRecoveryWork(item)) {
				await this.persistAutoState();
			}

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
					await this.postAutoRefresh(item, result);
					state.last_success_ms = Date.now();
					state.last_error = null;

					if (this.isRecoveryWork(item)) {
						state.recovery_noop_until_ms ??= {};
						const key = this.recoveryNoopKey(
							item.interval as RecoveryInterval,
							item.before ?? null,
						);
						if (item.target_gap) {
							const rowsWritten = Number(
								(result as { rows_written?: number }).rows_written ?? 0,
							);
							const sameGapStillPresent =
								await this.recoveryGapStillPresentForItem(item);
							if (rowsWritten === 0 || sameGapStillPresent) {
								state.recovery_noop_until_ms[key] =
									Date.now() + RECOVERY_NOOP_COOLDOWN_MS;
							} else {
								delete state.recovery_noop_until_ms[key];
							}
						} else {
							// Stale-tail catch-up is not an irrecoverable exact gap. Never leave
							// a legacy 30-minute no-op cooldown blocking normal authoritative refresh.
							delete state.recovery_noop_until_ms[key];
						}
					}
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
		if (this.isRetiredInstance()) {
			// Retired instances do not need persistent reads to identify themselves.
			try {
				await this.retireThisInstance();
			} catch {}
			return;
		}

		const loaded = await this.ensureStorageLoaded();
		if (!loaded.ok) {
			await this.scheduleQuotaRecoveryAlarm(loaded.retry_at_ms);
			return;
		}

		try {
			await this.handleAlarm();
		} catch (error) {
			if (this.isStorageReadQuotaError(error)) {
				const retryAtMs = this.noteStorageReadQuotaFailure(error);
				await this.scheduleQuotaRecoveryAlarm(retryAtMs);
				return;
			}
			this.lastError =
				error instanceof Error ? error.message : String(error);
		}
	}

	private async handleAlarm() {
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
					this.markStorageWriteSuccess();
				} catch (error) {
					this.markStorageWriteFailure(error);
				}
				return;
			}

			const beforeFingerprint = this.autoStateFingerprint();
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

			try {
				await this.ensureConnection();
				if (state.queue.length > 0) {
					await this.processAutoQueue();
				} else {
					// No queue work: persist only if something other than last_alarm_ms
					// actually changed. This removes idle metadata writes.
					await this.persistAutoStateIfChanged(beforeFingerprint);
				}
			} catch (error) {
				state.last_error =
					error instanceof Error ? error.message : String(error);
				await this.persistAutoState();
			}

			const recoveryPaused =
				state.last_error?.startsWith("RECOVERY_BUDGET_PAUSED") ?? false;
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
					: state.queue.length > 0 && !recoveryPaused
						? AUTO_BUSY_ALARM_MS
						: AUTO_IDLE_ALARM_MS;

			await this.scheduleAutoAlarm(delay);
		} catch (error) {
			if (this.isStorageReadQuotaError(error)) {
				const retryAtMs = this.noteStorageReadQuotaFailure(error);
				await this.scheduleQuotaRecoveryAlarm(retryAtMs);
				return;
			}
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
		// We only need the immediate confirmed predecessor to determine whether
		// the first recent row crosses a declared closure. Reading hundreds of
		// newer rows and filtering them in memory wastes Durable Object row reads.
		let previous = await this.previousNormalizedCandle(interval, earliest);

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



	private async normalizeStoredIntervalRange(
		interval: NormalizedRestInterval,
		fromDatetime: string,
		toDatetime: string,
	) {
		const durationMinutes = normalizedIntervalMinutes(interval);
		const raw = (await this.listHistoricalRange(
			interval,
			fromDatetime,
			toDatetime,
		)).sort((a, b) => a.datetime.localeCompare(b.datetime));
		if (raw.length === 0) {
			return {
				status: "ok",
				interval,
				rows_written: 0,
				rows_unchanged_skipped: 0,
				rows_deleted: 0,
				write_policy: "targeted_range_no_raw_rows",
			};
		}

		let previous = await this.previousNormalizedCandle(
			interval,
			raw[0].datetime,
		);
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

		const writeStats = await this.reconcileNormalizedRange(
			interval,
			candidates,
			raw[0].datetime,
			raw[raw.length - 1].datetime,
		);

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
			pending_closed_period: isClosedMarketCairoDatetime(cairoTime(now)),
			layer: "analysis_normalized",
		};
		await this.ctx.storage.put(normalizedMetaKey(interval), meta);

		return {
			status: "ok",
			interval,
			rows_written: writeStats.written,
			rows_unchanged_skipped: writeStats.unchanged,
			rows_deleted: writeStats.deleted,
			write_policy: "targeted_range_diff_reconcile",
		};
	}

	private async deriveThreeMinuteRangeFromOneMinute(
		fromDatetime: string,
		toDatetime: string,
	) {
		const bucketFrom = threeMinuteBucketStart(fromDatetime);
		const bucketTo = threeMinuteBucketStart(toDatetime);
		if (!bucketFrom || !bucketTo) {
			return { written: 0, unchanged: 0, deleted: 0 };
		}
		const source = (await this.listNormalizedRange(
			"1min",
			bucketFrom,
			addMinutesToCairoDatetime(bucketTo, 2),
		)).sort((a, b) => a.datetime.localeCompare(b.datetime));

		const buckets = new Map<string, NormalizedCandle[]>();
		for (const candle of source) {
			const bucketStart = threeMinuteBucketStart(candle.datetime);
			if (!bucketStart || bucketStart < bucketFrom || bucketStart > bucketTo) continue;
			const rows = buckets.get(bucketStart) ?? [];
			rows.push(candle);
			buckets.set(bucketStart, rows);
		}

		const candidates: NormalizedCandle[] = [];
		for (
			let bucketStart = bucketFrom;
			bucketStart <= bucketTo;
			bucketStart = addMinutesToCairoDatetime(bucketStart, 3)
		) {
			const rows = (buckets.get(bucketStart) ?? []).slice().sort((a, b) =>
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
			) continue;
			const expectedClose = addMinutesToCairoDatetime(bucketStart, 3);
			if (statusFromExpectedClose(expectedClose) === "OPEN") continue;
			const containsSynthetic = rows.some((c) => c.synthetic_gap === true);
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
				source: containsSynthetic
					? "derived_1min_with_synthetic_bridge"
					: "derived_1min",
				provisional: false,
				confirmed: true,
				synthetic_gap: containsSynthetic,
				gap_adjusted: rows.some((c) => c.gap_adjusted),
				stored_at_ms: Date.now(),
			});
		}
		return this.reconcileNormalizedRange(
			"3min",
			candidates,
			bucketFrom,
			bucketTo,
		);
	}

	private async deriveFourHourRangeFromOneHour(
		fromDatetime: string,
		toDatetime: string,
	) {
		const bucketFrom = fourHourBucketStart(fromDatetime);
		const bucketTo = fourHourBucketStart(toDatetime);
		if (!bucketFrom || !bucketTo) {
			return { written: 0, unchanged: 0, deleted: 0 };
		}
		const source = (await this.listNormalizedRange(
			"1h",
			bucketFrom,
			addMinutesToCairoDatetime(bucketTo, 180),
		)).sort((a, b) => a.datetime.localeCompare(b.datetime));
		const buckets = new Map<string, NormalizedCandle[]>();
		for (const candle of source) {
			const bucketStart = fourHourBucketStart(candle.datetime);
			if (!bucketStart || bucketStart < bucketFrom || bucketStart > bucketTo) continue;
			const rows = buckets.get(bucketStart) ?? [];
			rows.push(candle);
			buckets.set(bucketStart, rows);
		}

		const candidates: NormalizedCandle[] = [];
		for (
			let bucketStart = bucketFrom;
			bucketStart <= bucketTo;
			bucketStart = addMinutesToCairoDatetime(bucketStart, 240)
		) {
			const parts = parseCairoDatetimeParts(bucketStart);
			if (!parts) continue;
			const rows = (buckets.get(bucketStart) ?? []).slice().sort((a, b) =>
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
			) continue;
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
		return this.reconcileNormalizedRange(
			"4h",
			candidates,
			bucketFrom,
			bucketTo,
		);
	}


	private async synthesizeHistoricalOneMinuteGapForThreeMinuteAudit(
		auditCache?: Map<AnalysisNormalizedInterval, ContinuityAudit>,
	) {
		// v15.5: A historical 3M gap can be older than the 1M continuity scan
		// horizon. In that case current 1M readiness may look clean even though
		// the exact historical 1M source rows needed by the missing 3M bucket are
		// absent. After normal REST recovery has already been attempted, inspect
		// the 1M source window implied by the 3M gap directly. If that source hole
		// is bounded by real/effective 1M candles and is no larger than the normal
		// synthetic bridge policy, fill it deterministically, then let 3M rebuild.
		const threeAudit = await this.auditContinuity("3min", auditCache);
		if (!threeAudit.gap) {
			return { attempted: false, created: 0, candles: [] as string[] };
		}

		const gap = threeAudit.gap;
		const sourceStart = addMinutesToCairoDatetime(gap.missing_from, -1);
		const sourceEnd = addMinutesToCairoDatetime(gap.missing_to, 3);

		// Read only the persisted 1M source rows in/around the historical gap.
		// The old implementation listed the entire 1M prefix and filtered in JS,
		// which made a tiny repair consume the whole historical read set.
		const source = (await this.listNormalizedRange(
			"1min",
			sourceStart,
			sourceEnd,
		)).sort((a, b) => a.datetime.localeCompare(b.datetime));

		if (source.length < 2) {
			return { attempted: true, created: 0, candles: [] as string[] };
		}

		const candidates: NormalizedCandle[] = [];
		for (let i = 0; i < source.length - 1; i++) {
			const before = source[i];
			const after = source[i + 1];
			const missing = this.countMissingMarketBuckets(
				before.datetime,
				after.datetime,
				"1min",
			);
			if (
				missing.count < 1 ||
				missing.count > SYNTHETIC_MICRO_GAP_MAX_1M ||
				!missing.first ||
				!missing.last
			) {
				continue;
			}
			if (hasDeclaredClosureBetween(before.datetime, after.datetime)) continue;

			// Only bridge holes that overlap the 1M source span required by the
			// missing 3M interval; do not repair unrelated historical data here.
			const overlapsRequiredSource =
				missing.last >= gap.missing_from &&
				missing.first <= addMinutesToCairoDatetime(gap.missing_to, 2);
			if (!overlapsRequiredSource) continue;

			const start = Number(before.close);
			const end = Number(after.open);
			if (!Number.isFinite(start) || !Number.isFinite(end)) continue;
			const step = (end - start) / missing.count;

			for (let n = 0; n < missing.count; n++) {
				const datetime = addMinutesToCairoDatetime(before.datetime, n + 1);
				const open = start + step * n;
				const close = start + step * (n + 1);
				const expectedClose = addMinutesToCairoDatetime(datetime, 1);
				candidates.push({
					timeframe: "1min",
					datetime,
					open_time: datetime,
					expected_close_time: expectedClose,
					open,
					high: Math.max(open, close),
					low: Math.min(open, close),
					close,
					status: "CLOSED",
					source: "synthetic_bridge_approximation",
					provisional: false,
					confirmed: true,
					synthetic_gap: true,
					gap_adjusted: false,
					stored_at_ms: Date.now(),
				});
			}
		}

		if (candidates.length === 0) {
			return { attempted: true, created: 0, candles: [] as string[] };
		}

		const stats = await this.upsertNormalizedCandidates(candidates);
		return {
			attempted: true,
			created: stats.written,
			unchanged: stats.unchanged,
			candles: candidates.map((c) => c.datetime),
		};
	}


	private async forceRebuildHistoricalThreeMinuteGap(
		auditCache?: Map<AnalysisNormalizedInterval, ContinuityAudit>,
	) {
		// v15.7: rebuild the exact missing 3M span from a bounded 1M range. There
		// is no fixed 100-bucket cap and no per-candle get() loop; large historical
		// gaps are still repairable without turning the operation into a full scan.
		const audit = await this.auditContinuity("3min", auditCache);
		if (!audit.gap) {
			return { attempted: false, written: 0, unchanged: 0, deleted: 0 };
		}
		const stats = await this.deriveThreeMinuteRangeFromOneMinute(
			audit.gap.missing_from,
			audit.gap.missing_to,
		);
		return { attempted: true, ...stats };
	}

	private async deriveRecentThreeMinuteFromOneMinute() {
		// v15.1: recent derivation is a reconciliation, not append-only. If a
		// previously stored 3M candle loses one of its three authoritative 1M
		// source rows, that stale 3M row must be removed instead of surviving and
		// falsely making continuity look complete.
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
		const bucketStarts = Array.from(buckets.keys()).sort();
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

			const containsSynthetic = rows.some((c) => c.synthetic_gap === true);
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
				source: containsSynthetic
					? "derived_1min_with_synthetic_bridge"
					: "derived_1min",
				provisional: false,
				confirmed: true,
				synthetic_gap: containsSynthetic,
				gap_adjusted: rows.some((c) => c.gap_adjusted),
				stored_at_ms: Date.now(),
			});
		}

		if (bucketStarts.length === 0) {
			return { written: 0, unchanged: 0, deleted: 0 };
		}

		return await this.reconcileNormalizedRange(
			"3min",
			candidates,
			bucketStarts[0],
			bucketStarts[bucketStarts.length - 1],
		);
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
		const confirmedRows = await this.listNormalizedRange(
			"1min",
			startDatetime,
			endDatetime,
		);

		const merged = new Map<string, NormalizedCandle>();
		for (const candle of confirmedRows) {
			merged.set(candle.datetime, candle);
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
		const confirmedRows = await this.listNormalizedRange(
			interval,
			startDatetime,
			endDatetime,
		);
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
		for (const candle of confirmedRows) {
			merged.set(candle.datetime, candle);
		}

		return Array.from(merged.values()).sort((a, b) =>
			a.datetime.localeCompare(b.datetime),
		);
	}

	private async getEffectiveDailySeries(
		startDatetime: string,
		endDatetime: string,
	) {
		const confirmedRows = await this.listNormalizedRange(
			"1day",
			startDatetime,
			endDatetime,
		);
		const merged = new Map<string, NormalizedCandle>();
		for (const candle of confirmedRows) {
			merged.set(candle.datetime, candle);
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
			const previousConfirmedWeekly =
				await this.previousNormalizedCandle("1week", bucketStart);

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

		const nowCairo = cairoTime(Date.now());
		if (isClosedMarketCairoDatetime(nowCairo)) return [];

		// v15.0 IMPORTANT:
		// Do not start the Effective overlay only AFTER the latest REST-confirmed
		// candle. Twelve Data can omit an isolated closed 1M candle while later
		// REST candles continue normally. In that case the tick-built candle may
		// still exist in live_state, but the old logic hid it because it was older
		// than latest confirmed. Overlay the whole retained live window instead.
		// Confirmed REST is merged afterwards by callers and remains authoritative
		// on every overlapping timestamp. No candle is fabricated.
		const retainedLiveStart =
			this.candles.length > 0
				? this.candles[0].datetime
				: this.currentCandle?.datetime ??
					cairoTime(Date.now() - 8 * 60 * 60 * 1000);

		if (interval === "1min") {
			return this.getEffectiveOneMinuteRows(retainedLiveStart, nowCairo);
		}

		return this.deriveEffectiveIntradayFromOneMinute(
			interval,
			retainedLiveStart,
			nowCairo,
		);
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

			storage_write_health: this.storageWriteHealth(),

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
		const successMs = Date.now();
		const persistedLastError =
			this.lastError !== null && this.isStorageWriteErrorMessage(this.lastError)
				? null
				: this.lastError;
		try {
			await this.ctx.storage.put("live_state", {
				enabled: this.enabled,
				lastPrice: this.lastPrice,
				lastTickMs: this.lastTickMs,
				tickCount: this.tickCount,
				reconnectCount: this.reconnectCount,
				currentCandle: this.currentCandle,
				candles: this.candles,
				lastError: persistedLastError,
				lastStorageWriteError: null,
				lastStorageWriteErrorMs: null,
				lastStorageWriteSuccessMs: successMs,
				historicalStorageWriteError: this.historicalStorageWriteError,
				subscribeStatus: this.subscribeStatus,
			});
			this.lastError = persistedLastError;
			this.markStorageWriteSuccess(successMs);
			return true;
		} catch (error) {
			this.markStorageWriteFailure(error);
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
				scheduler_idle_alarm_ms: AUTO_IDLE_ALARM_MS,
				scheduler_busy_alarm_ms: AUTO_BUSY_ALARM_MS,
				production_object_reused_in_place: true,
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
				probeUrl.pathname = "/storage-probe";
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
				probeUrl.pathname = "/storage-probe";
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


		if (url.pathname === "/runtime-reset") {
			const id = env.Chat.idFromName(PROD_OBJECT_NAME);
			const stub = env.Chat.get(id);
			const resetUrl = new URL(request.url);
			resetUrl.pathname = "/runtime-reset-instance";
			resetUrl.search = "";
			try {
				await stub.fetch(
					new Request(resetUrl.toString(), {
						method: "POST",
						headers: request.headers,
					}),
				);
				return json({
					status: "reset_requested",
					build_version: BUILD_VERSION,
					production_object_name: PROD_OBJECT_NAME,
					note:
						"Runtime reset request completed. Persistent Durable Object storage was not deleted.",
				}, 202);
			} catch (error) {
				// ctx.abort() intentionally terminates the in-memory Durable Object,
				// so the stub request normally rejects. Treat that as a successful
				// reset request; the next call creates a fresh runtime for the SAME ID.
				return json({
					status: "reset_requested",
					build_version: BUILD_VERSION,
					production_object_name: PROD_OBJECT_NAME,
					note:
						"Durable Object runtime was aborted intentionally. Persistent storage remains attached to the same object ID.",
					runtime_message:
						error instanceof Error ? error.message : String(error),
				}, 202);
			}
		}

		try {
			// Production traffic continues to use the SAME logical Durable
			// Object instance. v15.8 does not rotate or replace the production ID;
			// quota recovery resets only the in-memory runtime, never its storage.
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
