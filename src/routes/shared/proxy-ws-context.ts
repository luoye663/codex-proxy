import { getWsPool } from "../../proxy/ws-pool.js";
import type { WsConnectionPool } from "../../proxy/ws-pool.js";
import type {
  ResponseOwnerGoneReason,
  ResponseOwnerLookup,
} from "../../proxy/ws-pool.js";
import type { WsPoolContext } from "../../proxy/codex-api.js";

export interface BuildWsPoolContextOptions {
  useWebSocket?: boolean;
  conversationId: string | null | undefined;
  entryId: string;
  variantHash: string;
  requestId: string;
  tag: string;
  /** Distinguishes a continuity-recovery WS from a busy canonical chain. */
  poolKeySuffix?: string;
  /** Opaque account-token generation captured by newly-opened connections. */
  credentialGeneration?: string;
}

export interface BuildWsPoolContextDeps {
  getWsPool: () => WsConnectionPool;
  log: (line: string) => void;
}

const defaultDeps: BuildWsPoolContextDeps = {
  getWsPool,
  log: (line) => console.log(line),
};

/** Remove a response-to-physical-WS owner through the pool boundary. */
export function forgetWsResponseOwner(responseId: string): void {
  getWsPool().forgetResponseOwner(responseId);
}

/** Read response ownership without coupling the proxy orchestrator to the
 * singleton implementation. */
export function lookupWsResponseOwner(responseId: string): ResponseOwnerLookup {
  return getWsPool().lookupResponseOwner(responseId);
}

export function hasUsableWsResponseOwner(responseId: string): boolean {
  return getWsPool().hasUsableResponseOwner(responseId);
}

/** Evict only the lane owning one failed response chain. */
export function evictWsResponseOwnerLane(
  responseId: string,
  reason: ResponseOwnerGoneReason,
): void {
  getWsPool().evictByResponseId(responseId, reason);
}

/** Build a per-request WS pool context only when the WS path has a stable chain id. */
export function buildWsPoolContext(
  options: BuildWsPoolContextOptions,
  deps: Partial<BuildWsPoolContextDeps> = {},
): WsPoolContext | undefined {
  if (!options.useWebSocket) return undefined;
  if (!options.conversationId) return undefined;

  const log = deps.log ?? defaultDeps.log;
  const entryId = options.entryId;
  return {
    pool: (deps.getWsPool ?? defaultDeps.getWsPool)(),
    // Full-input chains are isolated by variant. Explicit continuations do
    // not use this key; response-owner lookup selects their physical WS.
    poolKey: [
      entryId,
      options.conversationId,
      options.variantHash,
      options.poolKeySuffix,
    ].filter((part): part is string => Boolean(part)).join(":"),
    entryId,
    credentialGeneration: options.credentialGeneration,
    onHttpFallback: (reason) => {
      log(`[${options.tag}] Account ${entryId} | rid=${options.requestId.slice(0, 8)} | transport=http-sse | ws-fallback=${reason}`);
    },
    onDecision: (decision) => {
      const ridShort = options.requestId.slice(0, 8);
      const wsTag = decision.kind === "bypass"
        ? `bypass(${decision.reason})`
        : decision.kind === "retry-after-stale-reuse"
          ? `retry-after-stale-reuse:${decision.wsId}`
          : `${decision.kind}:${decision.wsId}`;
      log(`[${options.tag}] Account ${entryId} | rid=${ridShort} | ws=${wsTag}`);
    },
  };
}
