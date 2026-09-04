/**
 * Shared proxy handler — orchestrates the account acquire → retry →
 * stream/collect → release lifecycle common to all API format routes.
 *
 * Delegates to:
 *   - account-acquisition.ts  — acquire / release with idempotent guard
 *   - proxy-egress-log.ts     — upstream request audit log entries
 *   - proxy-error-handler.ts  — CodexApiError classification + pool state mutations
 *   - proxy-error-retry-transition.ts — CodexApiError retry/release/fallback transition
 *   - proxy-fallback-account-retry.ts — fallback account acquire / API rebuild
 *   - proxy-implicit-resume-lifecycle.ts — implicit-resume state machine / rollback
 *   - proxy-implicit-resume-request.ts — implicit-resume request apply/restore state
 *   - proxy-request-preparation.ts — request input/default forwarding fields
 *   - proxy-session-context.ts — prompt cache / affinity / implicit-resume derived state
 *   - proxy-retry-recovery.ts — same-account retry recovery decision/application
 *   - proxy-upstream-attempt.ts — one upstream request attempt + egress/rate-limit capture
 *   - proxy-debug-dump.ts     — opt-in request payload diagnostics
 *   - proxy-request-diagnostics.ts — request summary / large payload logs
 *   - proxy-stagger.ts        — request interval staggering
 *   - proxy-ws-context.ts     — WebSocket pool context construction
 *   - streaming-handler.ts    — streaming (SSE) response lifecycle
 *   - non-streaming-handler.ts — collect / retry response lifecycle
 */

import { CodexApi, CodexApiError, PreviousResponseWebSocketError } from "../../proxy/codex-api.js";
import { toQuota } from "../../auth/quota-utils.js";
import { markFallbackUsed } from "../../auth/fallback-state.js";
import { acquireAccount, releaseAccount } from "./account-acquisition.js";
import { handleCodexApiError } from "./proxy-error-handler.js";
import { handleStreaming } from "./streaming-handler.js";
import { handleNonStreaming } from "./non-streaming-handler.js";
import { annotateImageGenOutcome, buildCodexApi } from "./proxy-handler-utils.js";
import { handleDirectRequest } from "./direct-request-handler.js";
import { ResponsesUpstream } from "../../proxy/responses-upstream.js";
import type {
  FormatAdapter,
  HandleProxyRequestOptions,
  ProxyErrorDetails,
  ProxyRequest,
} from "./proxy-handler-types.js";
import { getSessionAffinityMap } from "../../auth/session-affinity.js";
import { createHash, randomUUID } from "crypto";
import {
  respondWithNoAccount,
  respondWithProxyError,
} from "./proxy-error-response.js";
import { applyProxyErrorRetryTransition } from "./proxy-error-retry-transition.js";
import { createImplicitResumeLifecycle } from "./proxy-implicit-resume-lifecycle.js";
import { captureImplicitResumeRequestState } from "./proxy-implicit-resume-request.js";
import {
  applyProxyRequestForwardingDefaults,
  ensureProxyRequestInputArray,
} from "./proxy-request-preparation.js";
import { logRequestDiagnostics } from "./proxy-request-diagnostics.js";
import {
  applyProxyRetryRecoveryDecision,
  applyCascadingBanDefense,
  buildProxyRetryRecoveryDecision,
  invalidateRejectedPreviousResponse,
} from "./proxy-retry-recovery.js";
import { classifyRetryAction } from "./proxy-retry-classifier.js";
import { buildProxySessionContext } from "./proxy-session-context.js";
import { staggerIfNeeded } from "./proxy-stagger.js";
import { sendProxyUpstreamAttempt } from "./proxy-upstream-attempt.js";
import {
  buildWsPoolContext,
  evictWsResponseOwnerLane,
  forgetWsResponseOwner,
  lookupWsResponseOwner,
} from "./proxy-ws-context.js";
import {
  containsInvalidEncryptedContentSignal,
  getReasoningReplayCache,
} from "../../proxy/reasoning-replay-cache.js";
import type { PreviousResponseContinuityReason } from "../../proxy/codex-types.js";

function fingerprintCredential(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

/**
 * Respond when no OAuth account is available. When a fallback upstream apikey
 * is configured, route the request through it (Responses API wire) instead of
 * returning "no account". The fallback is a pure last-resort: it is only
 * reached after every account is exhausted.
 */
async function respondNoAccountOrFallback(
  options: HandleProxyRequestOptions,
  req: ProxyRequest,
  fmt: FormatAdapter,
): Promise<Response> {
  const fallback = options.fallbackUpstream?.get();
  if (fallback) {
    console.log(
      `[${fmt.tag}] No available OAuth accounts — routing through fallback upstream apikey (${fallback.baseUrl})`,
    );
    markFallbackUsed();
    return handleDirectRequest({
      c: options.c,
      upstream: new ResponsesUpstream("fallback", fallback.apiKey, fallback.baseUrl),
      req,
      fmt,
    });
  }
  return respondWithNoAccount({ c: options.c, req, fmt });
}

/**
 * Respond with a proxy error, but when a fallback upstream apikey is
 * configured and the retry path found every OAuth account unusable, route the
 * request through the fallback as a last resort instead of surfacing the
 * error. Callers only reach this when `attemptFallback` was set on a retry
 * decision that found no usable account left.
 */
async function respondProxyErrorOrFallback(
  options: HandleProxyRequestOptions,
  req: ProxyRequest,
  fmt: FormatAdapter,
  status: number,
  message: string,
  useFormat429?: boolean,
  details?: ProxyErrorDetails,
): Promise<Response> {
  const fallback = options.fallbackUpstream?.get();
  if (fallback) {
    console.log(
      `[${fmt.tag}] Retry exhausted — routing through fallback upstream apikey (${fallback.baseUrl})`,
    );
    markFallbackUsed();
    return handleDirectRequest({
      c: options.c,
      upstream: new ResponsesUpstream("fallback", fallback.apiKey, fallback.baseUrl),
      req,
      fmt,
    });
  }
  return respondWithProxyError({ c: options.c, req, fmt, status, message, useFormat429, details });
}

export async function handleProxyRequest(options: HandleProxyRequestOptions): Promise<Response> {
  const { c, accountPool, cookieJar, req, fmt, proxyPool } = options;
  c.set("logForwarded", true);

  const affinityMap = getSessionAffinityMap();
  const requestId = c.get("requestId") ?? randomUUID().slice(0, 8);
  ensureProxyRequestInputArray(req);
  const originalRequestState = captureImplicitResumeRequestState(req);
  const sessionContext = buildProxySessionContext({ request: req, affinityMap });
  let chainAdvanceTicket = sessionContext.chainAdvanceTicket;
  let recoveryWsKeySuffix: string | undefined;
  let continuityRecoveryCount = 0;

  // turnState is scoped to one Codex turn. Preserve only a value supplied by
  // the current client request; never restore it from cross-turn affinity.
  applyProxyRequestForwardingDefaults({
    request: req,
    promptCacheKey: sessionContext.promptCacheKey,
  });

  const released = new Set<string>();
  const verifiedExcludeIds: string[] = [];

  // An explicit WS continuation is physically bound to the account/socket
  // that observed the response ID. Resolve it before account rotation.
  const strictPreviousResponseId =
    req.codexRequest.useWebSocket ? sessionContext.explicitPrevRespId : undefined;
  const ownerLookup = strictPreviousResponseId
    ? lookupWsResponseOwner(strictPreviousResponseId)
    : null;
  if (ownerLookup?.kind === "gone") {
    const reason = ownerLookup.tombstone.reason;
    return respondWithProxyError({
      c, req, fmt, status: 410,
      message: `The WebSocket history for previous_response_id is no longer available (${reason}).`,
      details: {
        type: "invalid_request_error",
        code: "ws_response_history_gone",
        continuityReason: reason,
        retryable: false,
      },
    });
  }
  if (ownerLookup?.kind === "unknown" && !sessionContext.preferredEntryId) {
    return respondWithProxyError({
      c, req, fmt, status: 410,
      message: "The WebSocket owner for previous_response_id is unknown to this proxy process.",
      details: {
        type: "invalid_request_error",
        code: "ws_response_history_gone",
        continuityReason: "unknown_owner",
        retryable: false,
      },
    });
  }
  const requiredEntryId = ownerLookup?.kind === "live"
    ? ownerLookup.entryId
    : strictPreviousResponseId
      ? sessionContext.preferredEntryId ?? undefined
      : undefined;

  let acquired = acquireAccount(
    accountPool,
    req.codexRequest.model,
    undefined,
    fmt.tag,
    sessionContext.preferredEntryId ?? undefined,
    requiredEntryId,
  );
  if (!acquired) {
    if (requiredEntryId) {
      const entry = accountPool.getEntry(requiredEntryId);
      const concurrency = accountPool.getAccounts()
        .find((candidate) => candidate.id === requiredEntryId)?.concurrency;
      const permanentReasonByStatus: Partial<Record<string, PreviousResponseContinuityReason>> = {
        expired: "account_expired",
        banned: "account_banned",
        disabled: "account_disabled",
        quota_exhausted: "account_quota_exhausted",
      };
      const permanentReason = entry ? permanentReasonByStatus[entry.status] : "account_removed";
      if (permanentReason) {
        return respondWithProxyError({
          c, req, fmt, status: 410,
          message: `The account owning previous_response_id is permanently unavailable (${permanentReason}).`,
          details: {
            type: "invalid_request_error",
            code: "ws_response_history_gone",
            continuityReason: permanentReason,
            retryable: false,
          },
        });
      }
      const reason: PreviousResponseContinuityReason =
        concurrency && concurrency.used >= concurrency.limit
          ? "account_concurrency_limit"
          : "account_temporarily_unavailable";
      c.header("Retry-After", "1");
      return respondWithProxyError({
        c, req, fmt, status: 429,
        message: `The account owning previous_response_id is temporarily unavailable (${reason}).`,
        details: {
          type: "rate_limit_error",
          code: "ws_response_owner_unavailable",
          continuityReason: reason,
          retryable: true,
        },
      });
    }
    return respondNoAccountOrFallback(options, req, fmt);
  }

  // ── Drift-Defense & Verification Loop ──
  // Caps the number of upstream /usage checks per request to avoid amplification
  // when many accounts are simultaneously dirty.
  const MAX_VERIFY_ATTEMPTS = 5;
  let verifyAttempts = 0;
  for (;;) {
    if (!acquired) return respondNoAccountOrFallback(options, req, fmt);
    const entry = accountPool.getEntry(acquired.entryId);
    if (entry?.quotaVerifyRequired) {
      const verifyingEntryId = acquired.entryId;
      console.log(`[${fmt.tag}] 🔍 Account ${verifyingEntryId} (${entry.email ?? "?"}) requires quota verification due to local reset. Syncing with upstream...`);
      try {
        const usage = await new CodexApi(
          acquired.token,
          acquired.accountId,
          cookieJar,
          acquired.entryId,
          proxyPool?.resolveProxyUrl(acquired.entryId),
        ).getUsage();
        
        const quota = toQuota(usage);
        accountPool.updateCachedQuota(acquired.entryId, quota);

        if (quota.rate_limit.limit_reached) {
          console.warn(`[${fmt.tag}] 🚫 Upstream reports account ${acquired.entryId} is still limit_reached. Releasing and retrying another...`);
          releaseAccount(accountPool, acquired.entryId, undefined, released);
          verifiedExcludeIds.push(acquired.entryId);

          verifyAttempts++;
          if (verifyAttempts >= MAX_VERIFY_ATTEMPTS) {
            console.warn(`[${fmt.tag}] ⚠️ Drift-defense hit MAX_VERIFY_ATTEMPTS (${MAX_VERIFY_ATTEMPTS}). Giving up to avoid excess upstream calls.`);
            if (requiredEntryId) {
              c.header("Retry-After", "1");
              return respondWithProxyError({
                c, req, fmt, status: 429,
                message: "The account owning previous_response_id is temporarily rate-limited.",
                details: {
                  type: "rate_limit_error",
                  code: "ws_response_owner_unavailable",
                  continuityReason: "account_temporarily_unavailable",
                  retryable: true,
                },
              });
            }
            return respondNoAccountOrFallback(options, req, fmt);
          }

          if (requiredEntryId) {
            c.header("Retry-After", "1");
            return respondWithProxyError({
              c, req, fmt, status: 429,
              message: "The account owning previous_response_id is temporarily rate-limited.",
              details: {
                type: "rate_limit_error",
                code: "ws_response_owner_unavailable",
                continuityReason: "account_temporarily_unavailable",
                retryable: true,
              },
            });
          }
          acquired = acquireAccount(accountPool, req.codexRequest.model, verifiedExcludeIds, fmt.tag, sessionContext.preferredEntryId ?? undefined);
          if (!acquired) {
            return respondNoAccountOrFallback(options, req, fmt);
          }
          continue; // Loop back to check the newly acquired account
        }
      } catch (err) {
        console.warn(`[${fmt.tag}] ⚠️ Failed to verify dirty quota for ${verifyingEntryId}:`, err);
        // Keep quotaVerifyRequired=true so the flag isn't silently cleared on transient network errors.
        // The ActiveQuotaRefresher or the next request will retry. This avoids promoting a still-limited
        // account to "clean" just because the upstream check temporarily failed.
      }
    }
    break; // Verified or no verification required, proceed!
  }

  if (!acquired) return respondNoAccountOrFallback(options, req, fmt);
  let { entryId } = acquired;
  let credentialGeneration = fingerprintCredential(acquired.token);
  // First account this request acquired; later attempts that switch to another
  // entry (fallback account retry) are marked as fallback in the audit log.
  const initialEntryId = entryId;

  const accountDisplayName = (id: string): string | null => {
    const entry = accountPool.getEntry(id);
    if (entry?.label) return entry.label;
    if (entry?.email) return entry.email;
    return id.slice(0, 8);
  };

  // ── Session Affinity Fallback Defense (Cascading Ban Prevention) ──
  // Only strip session identifiers when the preferred account is banned/disabled.
  // Quota exhaustion is normal rotation — no ban propagation risk.
  if (sessionContext.preferredEntryId && sessionContext.preferredEntryId !== entryId) {
    const preferredEntry = accountPool.getEntry(sessionContext.preferredEntryId);
    applyCascadingBanDefense({
      request: req,
      affinityMap,
      preferredEntryId: sessionContext.preferredEntryId,
      acquiredEntryId: entryId,
      preferredStatus: preferredEntry?.status,
      explicitPrevRespId: sessionContext.explicitPrevRespId,
      tag: fmt.tag,
    });
  }
  let codexApi = buildCodexApi(
    acquired.token,
    acquired.accountId,
    cookieJar,
    entryId,
    proxyPool,
    acquired.codexFingerprintMode ?? "off",
  );
  const triedEntryIds: string[] = [entryId];
  let modelRetried = false;
  let earlyServerErrorRetried = false;
  let stripAndRetryDone = false;
  const reasoningReplayCache = getReasoningReplayCache();
  const reasoningReplayItems = sessionContext.implicitPrevRespId
    ? reasoningReplayCache.lookup({
        responseId: sessionContext.implicitPrevRespId,
        entryId,
        conversationId: sessionContext.chainConversationId,
        variantHash: sessionContext.variantHash,
      })
    : [];

  const implicitResume = createImplicitResumeLifecycle({
    request: req,
    snapshot: originalRequestState,
    affinityMap,
    tag: fmt.tag,
    implicitPrevRespId: sessionContext.implicitPrevRespId,
    continuationInputStart: sessionContext.continuationInputStart,
    reasoningReplayItems,
    resumeEvaluationInput: sessionContext.resumeEvaluationInput,
    acquiredEntryId: entryId,
  });
  implicitResume.logSkippedWarnings();
  implicitResume.activate();

  const diagnostics = logRequestDiagnostics({
    tag: fmt.tag,
    entryId,
    requestId,
    request: req,
    chainConversationId: sessionContext.chainConversationId,
    promptCacheKey: sessionContext.promptCacheKey,
    variantHash: sessionContext.variantHash,
    explicitPrevRespId: sessionContext.explicitPrevRespId,
    implicitPrevRespId: sessionContext.implicitPrevRespId,
    prevRespId: sessionContext.prevRespId,
    resumeActive: implicitResume.evaluation.active,
    resumeReason: implicitResume.evaluation.reason,
    preferredEntryId: sessionContext.preferredEntryId,
  });

  // Guard: when implicit resume fails due to missing tool calls, block runaway
  // full-history replays that would burn massive token budgets silently.
  // Relaxed thresholds: legitimate client-driven full replays (e.g. after
  // Codex CLI /compact) regularly hit 300-800KB / 100-800 items, and the
  // previous 250KB / 80-item gate was 413'ing them. Real runaway loops
  // typically blow past several MB before the issue becomes obvious.
  const PAYLOAD_GUARD_BYTES = 2_000_000;
  const PAYLOAD_GUARD_ITEMS = 1000;
  if (
    implicitResume.evaluation.reason === "missing_tool_calls" ||
    implicitResume.evaluation.reason === "unanswered_tool_calls"
  ) {
    const inputItemCount = req.codexRequest.input?.length ?? 0;
    if (diagnostics.payloadBytes > PAYLOAD_GUARD_BYTES || inputItemCount > PAYLOAD_GUARD_ITEMS) {
      console.warn(
        `[${fmt.tag}] ⛔ Payload guard: blocking ${(diagnostics.payloadBytes / 1024).toFixed(0)}KB / ${inputItemCount} items ` +
        `full-history replay (resume=${implicitResume.evaluation.reason}). ` +
        `Client should compact the conversation.`,
      );
      releaseAccount(accountPool, entryId, undefined, released);
      return respondWithProxyError({
        c, req, fmt,
        status: 413,
        message:
          `Context too large for full-history replay ` +
          `(${(diagnostics.payloadBytes / 1024).toFixed(0)}KB, ${inputItemCount} items). ` +
          `Implicit resume failed: ${implicitResume.evaluation.reason}. ` +
          `Please compact or restart the conversation.`,
      });
    }
  }

  const abortController = new AbortController();
  c.req.raw.signal.addEventListener("abort", () => abortController.abort(), { once: true });

  await staggerIfNeeded(acquired.prevSlotMs);

  const buildPoolCtx = (forEntryId: string = entryId) =>
    buildWsPoolContext({
      useWebSocket: req.codexRequest.useWebSocket,
      conversationId: sessionContext.chainConversationId,
      entryId: forEntryId,
      variantHash: sessionContext.variantHash,
      requestId,
      tag: fmt.tag,
      poolKeySuffix: recoveryWsKeySuffix,
      credentialGeneration,
    });

  for (;;) {
    try {
      const { rawResponse, upstreamTurnState } = await sendProxyUpstreamAttempt({
        accountPool,
        api: codexApi,
        request: req,
        entryId,
        account: accountDisplayName(entryId),
        fallback: entryId !== initialEntryId,
        abortSignal: abortController.signal,
        buildPoolCtx,
        requestId,
        tag: fmt.tag,
        conversationId: sessionContext.chainConversationId,
        implicitResumeActive: implicitResume.isActive(),
        resumeReason: implicitResume.resumeReasonForAttempt(),
      });

      // ── Streaming path ──
      if (req.isStreaming) {
        return handleStreaming({
          c,
          accountPool,
          req,
          fmt,
          api: codexApi,
          response: rawResponse,
          entryId,
          abortController,
          released,
          requestId,
          affinityMap,
          conversationId: sessionContext.chainConversationId,
          turnState: upstreamTurnState,
          usageHint: implicitResume.getUsageHint(),
          variantHash: sessionContext.variantHash,
          chainAdvanceTicket,
          implicitResumeActive: implicitResume.isActive(),
          fallback: entryId !== initialEntryId,
        });
      }

      // ── Non-streaming path (with empty-response retry) ──
      return await handleNonStreaming({
        c,
        accountPool,
        cookieJar,
        req,
        fmt,
        proxyPool,
        initialApi: codexApi,
        initialResponse: rawResponse,
        initialEntryId: entryId,
        abortController,
        released,
        requestId,
        affinityMap,
        conversationId: sessionContext.chainConversationId,
        turnState: upstreamTurnState,
        getUsageHint: () => implicitResume.getUsageHint(),
        restoreImplicitResumeRequest: implicitResume.restore,
        buildPoolCtx,
        setActiveAccount: (nextEntryId, nextApi) => {
          entryId = nextEntryId;
          codexApi = nextApi;
          if (!triedEntryIds.includes(nextEntryId)) triedEntryIds.push(nextEntryId);
        },
        variantHash: sessionContext.variantHash,
        chainAdvanceTicket,
      });
    } catch (err) {
      invalidateRejectedPreviousResponse({
        err,
        previousResponseId: req.codexRequest.previous_response_id,
        affinityMap,
        forgetResponseOwner: forgetWsResponseOwner,
      });
      if (containsInvalidEncryptedContentSignal(err)) {
        reasoningReplayCache.evictByIdentity({
          entryId,
          conversationId: sessionContext.chainConversationId,
          variantHash: sessionContext.variantHash,
        });
      }
      const retryAction = classifyRetryAction(
        err,
        {
          stripAndRetryDone,
          modelRetried,
          implicitResumeActive: implicitResume.isActive(),
          previousResponseId: req.codexRequest.previous_response_id,
          explicitPreviousResponseId: Boolean(sessionContext.explicitPrevRespId),
        },
        (e) => implicitResume.canReplayAfterError(e),
      );

      switch (retryAction.type) {
        case "not_codex_error":
          releaseAccount(accountPool, entryId, annotateImageGenOutcome(undefined, req.expectsImageGen), released);
          throw err;

        case "implicit_resume_replay": {
          if (!implicitResume.replayFullInputAfterError(err)) throw err;
          stripAndRetryDone = true;
          const staleId = sessionContext.implicitPrevRespId;
          const continuityReason = err instanceof PreviousResponseWebSocketError
            ? err.continuityReason
            : undefined;

          // A busy owner is a live sibling branch. Keep the parent head and
          // original ticket so only the first sibling completion advances it.
          // Missing/dead owners are stale: invalidate and rebuild from a root.
          if (staleId && continuityReason !== "busy") {
            affinityMap.forget(staleId);
            forgetWsResponseOwner(staleId);
            chainAdvanceTicket = affinityMap.captureChainAdvance(
              sessionContext.chainConversationId,
              sessionContext.variantHash,
              null,
            );
          }

          // A unique pooled key avoids both the busy canonical connection and
          // one-shot fallback while establishing a new response owner.
          recoveryWsKeySuffix =
            `recovery-${requestId.slice(0, 8)}-${++continuityRecoveryCount}`;
          continue;
        }

        case "strip_and_retry": {
          stripAndRetryDone = true;
          const decision = buildProxyRetryRecoveryDecision({
            err, tag: fmt.tag, entryId, stripAndRetryDone: false,
            previousResponseId: req.codexRequest.previous_response_id,
          });
          applyProxyRetryRecoveryDecision({
            decision,
            request: req,
            affinityMap,
            restoreImplicitResumeRequest: implicitResume.restore,
          });
          if (decision.action === "retry" && decision.staleId) {
            forgetWsResponseOwner(decision.staleId);
            chainAdvanceTicket = affinityMap.captureChainAdvance(
              sessionContext.chainConversationId,
              sessionContext.variantHash,
              null,
            );
            recoveryWsKeySuffix =
              `recovery-${requestId.slice(0, 8)}-${++continuityRecoveryCount}`;
          }
          continue;
        }

        case "error_handler_decides": {
          const liveOwner = req.codexRequest.previous_response_id
            ? lookupWsResponseOwner(req.codexRequest.previous_response_id)
            : null;
          const requestCredentialGeneration = liveOwner?.kind === "live"
            ? liveOwner.credentialGeneration ?? credentialGeneration
            : credentialGeneration;
          const currentToken = accountPool.getEntry(entryId)?.token;
          const currentCredentialGeneration = currentToken
            ? fingerprintCredential(currentToken)
            : undefined;
          const credentialWasRotated =
            currentCredentialGeneration !== undefined &&
            currentCredentialGeneration !== requestCredentialGeneration;
          const decision = credentialWasRotated && (err as CodexApiError).status === 401
            ? (() => {
                // A refresh may race an older authenticated lane. Its 401 must
                // not expire the newly-refreshed account generation.
                const previousResponseId = req.codexRequest.previous_response_id;
                if (previousResponseId) {
                  evictWsResponseOwnerLane(previousResponseId, "connection_replaced");
                  return {
                    action: "respond" as const,
                    status: 410,
                    message: "The WebSocket owner used an older credential generation after token refresh.",
                    details: {
                      type: "invalid_request_error",
                      code: "ws_response_history_gone",
                      continuityReason: "connection_replaced",
                      retryable: false,
                    },
                  };
                }
                return {
                  action: "respond" as const,
                  status: 503,
                  message: "The request used an older credential generation; retry with the refreshed account.",
                  details: {
                    type: "server_error",
                    code: "ws_pool_connection_unavailable",
                    retryable: true,
                  },
                };
              })()
            : handleCodexApiError(
                err as CodexApiError, accountPool, entryId, req.codexRequest.model, fmt.tag, modelRetried, cookieJar,
                earlyServerErrorRetried,
              );

          // Explicit continuations cannot rotate accounts: server-side state
          // is on the owning physical lane. Surface the owner's failure to the
          // downstream caller and preserve its opportunity to retry.
          let continuitySafeDecision = decision;
          if (requiredEntryId && decision.action === "retry") {
            const permanentStatusReason: Partial<Record<number, PreviousResponseContinuityReason>> = {
              401: accountPool.getEntry(entryId)?.status === "banned"
                ? "account_banned"
                : "account_expired",
              402: "account_quota_exhausted",
              403: "account_banned",
            };
            const permanentReason = permanentStatusReason[decision.status];
            continuitySafeDecision = permanentReason
              ? {
                  action: "respond" as const,
                  status: 410,
                  message: `The account owning previous_response_id is permanently unavailable (${permanentReason}).`,
                  details: {
                    type: "invalid_request_error",
                    code: "ws_response_history_gone",
                    continuityReason: permanentReason,
                    retryable: false,
                  },
                }
              : decision.status === 429
                ? {
                    action: "respond" as const,
                    status: 429,
                    message: decision.message,
                    details: {
                      type: "rate_limit_error",
                      code: "ws_response_owner_unavailable",
                      continuityReason: "account_temporarily_unavailable",
                      retryable: true,
                    },
                  }
                : {
                    action: "respond" as const,
                    status: decision.status,
                    message: decision.message,
                    ...(decision.details ? { details: decision.details } : {}),
                  };
          }
          const errorRetryTransition = applyProxyErrorRetryTransition({
            accountPool, entryId,
            model: req.codexRequest.model,
            triedEntryIds, tag: fmt.tag,
            decision: continuitySafeDecision, released,
            restoreImplicitResumeRequest: implicitResume.restore,
            modelRetried,
            expectsImageGen: req.expectsImageGen,
            cookieJar, proxyPool,
          });
          if (errorRetryTransition.action === "respond") {
            if (
              errorRetryTransition.status === 429 &&
              errorRetryTransition.details?.code === "ws_response_owner_unavailable"
            ) {
              c.header("Retry-After", "1");
            }
            if (errorRetryTransition.attemptFallback) {
              return respondProxyErrorOrFallback(
                options, req, fmt,
                errorRetryTransition.status,
                errorRetryTransition.message,
                errorRetryTransition.useFormat429,
                errorRetryTransition.details,
              );
            }
            return respondWithProxyError({
              c, req, fmt,
              status: errorRetryTransition.status,
              message: errorRetryTransition.message,
              ...(errorRetryTransition.details
                ? { details: errorRetryTransition.details }
                : {}),
              ...(errorRetryTransition.useFormat429 ? { useFormat429: true } : {}),
            });
          }

          modelRetried = errorRetryTransition.modelRetried;
          if (decision.action === "retry" && decision.markEarlyServerErrorRetried) {
            earlyServerErrorRetried = true;
          }
          entryId = errorRetryTransition.entryId;
          const nextToken = accountPool.getEntry(entryId)?.token;
          if (nextToken) credentialGeneration = fingerprintCredential(nextToken);
          triedEntryIds.push(errorRetryTransition.entryId);
          codexApi = errorRetryTransition.api;
          await staggerIfNeeded(errorRetryTransition.prevSlotMs);
          continue;
        }
      }
    }
  }
}
