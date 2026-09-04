import { EventEmitter } from "node:events";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@src/config.js", () => ({
  getConfig: () => ({ api: { base_url: "https://test.invalid/backend-api" }, client: { app_version: "test" } }),
}));
vi.mock("@src/fingerprint/manager.js", () => ({
  buildHeaders: () => ({}),
  buildHeadersWithContentType: () => ({}),
}));
const { post } = vi.hoisted(() => ({ post: vi.fn() }));
vi.mock("@src/tls/transport.js", () => ({
  getTransport: () => ({ post, isImpersonate: () => false }),
}));

import { CodexApi } from "@src/proxy/codex-api.js";
import { CodexApiError, type CodexResponsesRequest } from "@src/proxy/codex-types.js";
import { PersistentWs, WsConnectionPool, type WsLike } from "@src/proxy/ws-pool.js";
import { createImplicitResumeLifecycle } from "@src/routes/shared/proxy-implicit-resume-lifecycle.js";
import { captureImplicitResumeRequestState } from "@src/routes/shared/proxy-implicit-resume-request.js";

class Socket extends EventEmitter implements WsLike {
  readyState = 1;
  send(): void {}
  ping(): void {}
  close(): void { this.readyState = 3; }
  push(value: unknown): void { this.emit("message", JSON.stringify(value)); }
}

const pools: WsConnectionPool[] = [];
afterEach(async () => {
  for (const pool of pools.splice(0)) await pool.shutdown();
  vi.restoreAllMocks();
  post.mockReset();
});

async function fixture() {
  const pool = new WsConnectionPool({ maxPerAccount: 1 }, { startGc: false });
  pools.push(pool);
  const socket = new Socket();
  const owner = await pool.acquire("entry", "entry:old-conversation", async (deps) =>
    new PersistentWs({ ...deps, ws: socket, pingIntervalMs: 0 }),
  );
  if (!("ws" in owner)) throw new Error("expected owner");
  const sent = owner.ws.send({
    request: { type: "response.create", model: "gpt-test", instructions: "", input: [] },
    signal: undefined, onRateLimits: undefined, reused: false,
  });
  socket.push({ type: "response.completed", response: { id: "resp_old" } });
  await (await sent).text();
  post.mockImplementation(async () => ({
    status: 200,
    headers: new Headers({ "content-type": "text/event-stream" }),
    body: new Response('event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_http"}}\n\n').body,
    setCookieHeaders: [],
  }));
  const request: CodexResponsesRequest = {
    model: "gpt-5.6-luna", instructions: "keep instructions",
    input: [{ role: "user", content: "hello" }],
    useWebSocket: true, stream: true, store: false,
    turnMetadata: JSON.stringify({ request_kind: "prewarm" }),
  };
  const onHttpFallback = vi.fn();
  const context = { pool, entryId: "entry", poolKey: "entry:new-conversation", onHttpFallback };
  return { pool, socket, owner: owner.ws, request, context, api: new CodexApi("test-token", null) };
}

describe("HTTP fallback when WS retention is unavailable", () => {
  it("restores full input after an implicit owner loss before falling back from a full pool", async () => {
    const { request, context, api } = await fixture();
    request.input = [
      { role: "user", content: "first" },
      { role: "assistant", content: "answer" },
      { role: "user", content: "continue" },
    ];
    const proxyRequest = { model: request.model, isStreaming: true, codexRequest: request };
    const snapshot = captureImplicitResumeRequestState(proxyRequest);
    const lifecycle = createImplicitResumeLifecycle({
      request: proxyRequest, snapshot, tag: "Test", acquiredEntryId: "entry",
      affinityMap: { lookupInputTokens: () => null },
      implicitPrevRespId: "resp_lost", continuationInputStart: 2,
      resumeEvaluationInput: {
        implicitPrevRespId: "resp_lost", continuationInputStart: 2, inputLength: 3,
        preferredEntryId: "entry", currentInstructions: request.instructions,
        storedInstructionsHash: createHash("sha256").update(request.instructions).digest("hex"),
        requiredFunctionCallOutputIds: [], storedFunctionCallIds: [],
      },
    });
    lifecycle.activate();
    expect(request.input).toHaveLength(1);
    try {
      await api.createResponse(request, undefined, undefined, context);
      throw new Error("expected missing owner");
    } catch (err) {
      expect(lifecycle.replayFullInputAfterError(err)).toBe(true);
    }
    expect((await api.createResponse(request, undefined, undefined, context)).status).toBe(200);
    const body = JSON.parse(post.mock.calls[0][2]);
    expect(body.input).toEqual(snapshot.input);
    expect(body.instructions).toBe(snapshot.instructions);
    expect(body).not.toHaveProperty("previous_response_id");
    expect(post).toHaveBeenCalledTimes(1);
  });

  it.each([false, true])("serves repeated new conversations with a full pool (busy=%s)", async (busy) => {
    const { pool, request, context, api, owner } = await fixture();
    if (busy) expect(owner.tryAcquire()).toBe(true);
    const controller = new AbortController();
    for (let i = 0; i < 3; i++) {
      const response = await api.createResponse(request, controller.signal, undefined, {
        ...context, poolKey: `entry:new-${i}`,
      });
      expect(response.status).toBe(200);
      expect(await response.text()).toContain("resp_http");
    }
    expect(post).toHaveBeenCalledTimes(3);
    expect(context.onHttpFallback).toHaveBeenCalledWith("capacity");
    const args = post.mock.calls[0];
    expect(JSON.parse(args[2])).toMatchObject({ input: request.input, instructions: request.instructions });
    expect(JSON.parse(args[2])).not.toHaveProperty("previous_response_id");
    expect(args[3]).toBe(controller.signal);
    expect(pool.ownerWsId("resp_old")).toBe(owner.id);
    expect(pool.hasUsableResponseOwner("resp_http")).toBe(false);
    expect(pool.size()).toBe(1);
  });

  it("falls back on a local connection setup failure", async () => {
    const { pool, request, context, api } = await fixture();
    vi.spyOn(pool, "acquire").mockRejectedValueOnce(new Error("connect failed"));
    expect((await api.createResponse(request, undefined, undefined, context)).status).toBe(200);
    expect(context.onHttpFallback).toHaveBeenCalledWith("connection");
    expect(post).toHaveBeenCalledTimes(1);
  });

  it("does not replay a real upstream 429 through HTTP", async () => {
    const { socket, request, context, api } = await fixture();
    const result = api.createResponse(request, undefined, undefined, {
      ...context, poolKey: "entry:old-conversation",
    });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    socket.push({ type: "error", error: { code: "rate_limit_exceeded", message: "upstream limit" } });
    await expect(result).rejects.toMatchObject({ status: 429 });
    expect(post).not.toHaveBeenCalled();
  });

  it("keeps explicit continuation bound to its busy owner", async () => {
    const { owner, request, context, api } = await fixture();
    expect(owner.tryAcquire()).toBe(true);
    request.previous_response_id = "resp_old";
    await expect(api.createResponse(request, undefined, undefined, context)).rejects.toMatchObject({ status: 409 });
    expect(post).not.toHaveBeenCalled();
  });

  it("does not fall back after output has started", async () => {
    const { socket, request, context, api } = await fixture();
    const pending = api.createResponse(request, undefined, undefined, {
      ...context, poolKey: "entry:old-conversation",
    });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    socket.push({ type: "response.output_text.delta", delta: "partial" });
    const response = await pending;
    const reader = response.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toContain("partial");
    socket.emit("error", new Error("connection lost"));
    await expect(reader.read()).rejects.toThrow("connection lost");
    reader.releaseLock();
    expect(post).not.toHaveBeenCalled();
  });

  it("does not downgrade an explicit ID whose owner is gone", async () => {
    const { pool, request, context, api } = await fixture();
    pool.evictByResponseId("resp_old", "transport_closed");
    request.previous_response_id = "resp_old";
    await expect(api.createResponse(request, undefined, undefined, context)).rejects.toMatchObject({ status: 410 });
    expect(post).not.toHaveBeenCalled();
  });

  it("does not start HTTP if the caller already aborted", async () => {
    const { request, context, api } = await fixture();
    const controller = new AbortController();
    controller.abort();
    await expect(api.createResponse(request, controller.signal, undefined, context)).rejects.toBe(controller.signal.reason);
    expect(post).not.toHaveBeenCalled();
  });

  it("does not start HTTP when the client aborts an acquired WS before output", async () => {
    const { request, context, api } = await fixture();
    const controller = new AbortController();
    const pending = api.createResponse(request, controller.signal, undefined, {
      ...context, poolKey: "entry:old-conversation",
    });
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    controller.abort();
    await expect(pending).rejects.toBe(controller.signal.reason);
    expect(post).not.toHaveBeenCalled();
  });

  it("propagates a real HTTP quota error after local WS fallback", async () => {
    const { request, context, api } = await fixture();
    post.mockResolvedValueOnce({
      status: 429, headers: new Headers(), setCookieHeaders: [],
      body: new Response('{"error":{"code":"usage_limit_reached"}}').body,
    });
    await expect(api.createResponse(request, undefined, undefined, context)).rejects.toBeInstanceOf(CodexApiError);
    expect(post).toHaveBeenCalledTimes(1);
  });
});
