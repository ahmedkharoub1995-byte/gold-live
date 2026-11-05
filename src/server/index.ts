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

			// A version-override header is only for smoke-testing the outer
			// Worker version. Durable Objects are version-assigned separately
			// during gradual deployments, so do not propagate that override
			// into the production Durable Object subrequest. This also mirrors
			// the MCP lane, which creates a fresh internal request.
			const forwardedRequest = new Request(request);
			forwardedRequest.headers.delete(
				"Cloudflare-Workers-Version-Overrides",
			);
			return await stub.fetch(forwardedRequest);
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
