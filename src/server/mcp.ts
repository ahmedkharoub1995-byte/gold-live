import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "agents/mcp/server";
import { z } from "zod";

type GoldMcpEnv = {
	Chat: DurableObjectNamespace;
};

type McpForwardResult = {
	ok: boolean;
	status: number;
	payload: unknown;
};

const ANALYSIS_INTERVALS = [
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
] as const;

async function forwardToProductionDurableObject(
	env: GoldMcpEnv,
	productionObjectName: string,
	pathname: string,
	searchParams?: URLSearchParams,
): Promise<McpForwardResult> {
	const id = env.Chat.idFromName(productionObjectName);
	const stub = env.Chat.get(id);
	const url = new URL(`https://gold-data-engine.internal${pathname}`);
	if (searchParams) {
		url.search = searchParams.toString();
	}

	const response = await stub.fetch(
		new Request(url.toString(), {
			method: "GET",
			headers: {
				Accept: "application/json",
			},
		}),
	);

	const raw = await response.text();
	let payload: unknown;
	try {
		payload = raw.length > 0 ? JSON.parse(raw) : null;
	} catch {
		payload = {
			status: "error",
			error: "Gold Data Engine returned a non-JSON response.",
			http_status: response.status,
			body: raw,
		};
	}

	return {
		ok: response.ok,
		status: response.status,
		payload,
	};
}

function asToolResult(result: McpForwardResult) {
	const text = JSON.stringify(result.payload);
	return {
		content: [{ type: "text" as const, text }],
		isError: !result.ok,
	};
}

function createGoldMcpServer(
	env: GoldMcpEnv,
	productionObjectName: string,
) {
	const server = new McpServer({
		name: "gold-data-engine",
		version: "1.0.0",
	});

	server.registerTool(
		"getGoldLivePrice",
		{
			description:
				"Get the latest live/provisional XAU/USD price and runtime connection status from the Gold Data Engine. This is the authoritative live-price source for Gold Swing analysis.",
			inputSchema: {},
		},
		async () =>
			asToolResult(
				await forwardToProductionDurableObject(
					env,
					productionObjectName,
					"/price",
				),
			),
	);

	server.registerTool(
		"getGoldEffectiveCandles",
		{
			description:
				"Read authoritative normalized/effective XAU/USD candles from the existing Gold Data Engine cache. Use the exact next_before cursor returned by a page when older history is required.",
			inputSchema: {
				interval: z.enum(ANALYSIS_INTERVALS),
				limit: z.number().int().min(1).max(200).optional(),
				before: z.string().min(1).optional(),
			},
		},
		async ({ interval, limit, before }) => {
			const params = new URLSearchParams({ interval });
			if (limit !== undefined) params.set("limit", String(limit));
			if (before !== undefined) params.set("before", before);
			return asToolResult(
				await forwardToProductionDurableObject(
					env,
					productionObjectName,
					"/normalized-data",
					params,
				),
			);
		},
	);

	server.registerTool(
		"getGoldLiveState",
		{
			description:
				"Inspect the Gold Data Engine WebSocket/runtime state. Use this when the live price is null, stale, or connection status is unclear.",
			inputSchema: {},
		},
		async () =>
			asToolResult(
				await forwardToProductionDurableObject(
					env,
					productionObjectName,
					"/state",
				),
			),
	);

	server.registerTool(
		"getGoldSystemCheck",
		{
			description:
				"Inspect Gold Data Engine health, scheduler/recovery status, build information, and per-timeframe diagnostics. This route is diagnostic and does not perform market analysis.",
			inputSchema: {},
		},
		async () =>
			asToolResult(
				await forwardToProductionDurableObject(
					env,
					productionObjectName,
					"/system-check",
				),
			),
	);

	server.registerTool(
		"ensureGoldDataReady",
		{
			description:
				"Run the Gold Data Engine bounded readiness/self-healing check before a full Gold Swing analysis or full refresh. Return the existing analysis_ready, gap, boundary, repair, queue, and recovery metadata without adding any separate MCP recovery state.",
			inputSchema: {},
		},
		async () =>
			asToolResult(
				await forwardToProductionDurableObject(
					env,
					productionObjectName,
					"/ensure-data-ready",
				),
			),
	);

	return server;
}

export async function handleGoldMcpRequest(
	request: Request,
	env: GoldMcpEnv,
	ctx: ExecutionContext,
	productionObjectName: string,
): Promise<Response> {
	const handler = createMcpHandler(
		() => createGoldMcpServer(env, productionObjectName),
		{
			route: "/mcp",
			responseMode: "auto",
		},
	);
	return handler(request, env, ctx);
}
