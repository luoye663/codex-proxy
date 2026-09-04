/**
 * Tests for client-facing WebSocket support on /v1/responses (issue #681).
 *
 * Unlike the pure-route tests, these spin up a real http.Server (via
 * @hono/node-server) bound to the Hono app and attach a ResponsesWebSocketServer,
 * then drive it with a real `ws` client so upgrade + handshake are exercised.
 * handleProxyRequest is mocked (repo convention) to return a controlled SSE stream.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { serve } from "@hono/node-server";
import { WebSocket } from "ws";
import type { AddressInfo } from "net";
import type { Server } from "http";
import { Hono } from "hono";
import type { HandleProxyRequestOptions } from "@src/routes/shared/proxy-handler-types.js";

// ── Mocks (before imports) ──────────────────────────────────────────

const mockConfig = {
  server: { proxy_api_key: null as string | null },
  model: {
    default: "gpt-5.3-codex",
    default_reasoning_effort: null,
    default_service_tier: null,
    suppress_desktop_directives: false,
  },
  auth: {
    jwt_token: undefined as string | undefined,
    rotation_strategy: "least_used" as const,
    rate_limit_backoff_seconds: 60,
  },
};

vi.mock("@src/config.js", () => ({
  getConfig: vi.fn(() => mockConfig),
}));

vi.mock("@src/paths.js", () => ({
  getDataDir: vi.fn(() => "/tmp/test-responses-ws"),
  getConfigDir: vi.fn(() => "/tmp/test-responses-ws-config"),
}));

vi.mock("fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("fs")>();
  return {
    ...actual,
    readFileSync: vi.fn(() => "models: []"),
    writeFileSync: vi.fn(),
    writeFile: vi.fn(
      (_p: string, _d: string, _e: string, cb: (err: Error | null) => void) =>
        cb(null),
    ),
    existsSync: vi.fn(() => false),
    mkdirSync: vi.fn(),
    renameSync: vi.fn(),
  };
});

vi.mock("js-yaml", () => ({
  default: {
    load: vi.fn(() => ({ models: [], aliases: {} })),
    dump: vi.fn(() => ""),
  },
}));

vi.mock("@src/auth/jwt-utils.js", () => ({
  decodeJwtPayload: vi.fn(() => ({
    exp: Math.floor(Date.now() / 1000) + 3600,
  })),
  extractChatGptAccountId: vi.fn((token: string) => `acct-${token}`),
  extractUserProfile: vi.fn(() => null),
  isTokenExpired: vi.fn(() => false),
}));

vi.mock("@src/models/model-fetcher.js", () => ({
  triggerImmediateRefresh: vi.fn(),
  startModelRefresh: vi.fn(),
  stopModelRefresh: vi.fn(),
}));

vi.mock("@src/utils/retry.js", () => ({
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
}));

// Capture the codexRequest handleProxyRequest receives.
let capturedCodexRequest: unknown = null;
let requestedStreams = 0;

function makeSseResponse(sseText: string): Response {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode(sseText));
        controller.close();
      },
    }),
    { status: 200, headers: { "content-type": "text/event-stream" } },
  );
}

vi.mock("@src/routes/shared/proxy-handler.js", () => ({
  handleProxyRequest: vi.fn(async (options: HandleProxyRequestOptions) => {
    capturedCodexRequest = options.req.codexRequest;
    requestedStreams++;
    return makeSseResponse(
      'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_1"}}\n\n' +
        'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"Hi there"}\n\n' +
        'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1"}}\n\n',
    );
  }),
}));

// ── Imports ─────────────────────────────────────────────────────────

import { AccountPool } from "@src/auth/account-pool.js";
import { ClientKeyPool } from "@src/auth/client-key-pool.js";
import { loadStaticModels } from "@src/models/model-store.js";
import { createResponsesRoutes } from "@src/routes/responses.js";
import { ResponsesWebSocketServer } from "@src/routes/responses-websocket.js";

// ── Helpers ─────────────────────────────────────────────────────────

const RESPONSE_CREATE_BODY = JSON.stringify({
  model: "codex",
  input: [{ role: "user", content: "Hello" }],
  stream: true,
});

/** Open a WS client to /v1/responses. Resolves on open; rejects with
 *  `{ status }` when the server refuses the upgrade, or the transport error. */
function connectClient(
  port: number,
  authHeader?: string,
): Promise<{ ws: WebSocket }> {
  return connectWithHeaders(port, authHeader ? { Authorization: authHeader } : {});
}

/** Open a WS client with arbitrary handshake headers. */
function connectWithHeaders(
  port: number,
  headers: Record<string, string>,
): Promise<{ ws: WebSocket }> {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/v1/responses`, { headers });
  return new Promise((resolve, reject) => {
    ws.once("open", () => resolve({ ws }));
    ws.once("unexpected-response", (_req, res) =>
      reject({ status: res.statusCode }),
    );
    ws.once("error", reject);
  });
}

/** Collect the next N JSON frames received on a socket. */
function receiveJsonFrames(ws: WebSocket, count: number): Promise<unknown[]> {
  const frames: unknown[] = [];
  return new Promise((resolve, reject) => {
    const onMessage = (data: Buffer | string) => {
      const raw = typeof data === "string" ? data : data.toString("utf-8");
      frames.push(JSON.parse(raw));
      if (frames.length >= count) {
        cleanup();
        resolve(frames);
      }
    };
    const onClose = () => {
      cleanup();
      reject(new Error(`socket closed after ${frames.length}/${count} frames`));
    };
    const cleanup = () => {
      ws.off("message", onMessage);
      ws.off("close", onClose);
    };
    ws.on("message", onMessage);
    ws.on("close", onClose);
  });
}

// ── Tests ───────────────────────────────────────────────────────────

describe("client-facing WebSocket on /v1/responses (issue #681)", () => {
  let pool: AccountPool;
  let clientKeyPool: ClientKeyPool;
  let app: Hono;
  let server: Server;
  let wsServer: ResponsesWebSocketServer;
  let port: number;

  beforeEach(async () => {
    vi.clearAllMocks();
    capturedCodexRequest = null;
    requestedStreams = 0;
    mockConfig.server.proxy_api_key = null;
    loadStaticModels();
    pool = new AccountPool();
    pool.addAccount("test-token-1");
    clientKeyPool = new ClientKeyPool();
    app = createResponsesRoutes(pool);
    const handle = serve({ fetch: app.fetch, hostname: "127.0.0.1", port: 0 });
    server = handle as unknown as Server;
    // serve() returns before the listener actually binds (src/index.ts does the
    // same via awaitServerListening) — wait for the listening event first.
    await new Promise<void>((resolve) => server.once("listening", () => resolve()));
    const addr = server.address() as AddressInfo;
    port = addr.port;
    wsServer = new ResponsesWebSocketServer({ server, app, accountPool: pool, clientKeyPool });
  });

  afterEach(async () => {
    await wsServer?.close();
    if (server) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    pool?.destroy();
  });

  it("accepts a WebSocket upgrade on /v1/responses", async () => {
    mockConfig.server.proxy_api_key = "master-key";
    const { ws } = await connectClient(port, "Bearer master-key");
    expect(ws.readyState).toBe(WebSocket.OPEN);
    ws.close();
  });

  it("rejects an unauthenticated upgrade with 401", async () => {
    mockConfig.server.proxy_api_key = "master-key";
    await expect(connectClient(port)).rejects.toMatchObject({ status: 401 });
  });

  it("rejects an upgrade with an invalid Bearer key with 401", async () => {
    mockConfig.server.proxy_api_key = "master-key";
    await expect(connectClient(port, "Bearer wrong-key")).rejects.toMatchObject({
      status: 401,
    });
  });

  it("dispatches a response.create frame and streams events back as WS text frames", async () => {
    const { ws } = await connectClient(port);
    const received = receiveJsonFrames(ws, 3);
    ws.send(RESPONSE_CREATE_BODY);
    const frames = await received;

    expect(requestedStreams).toBe(1);
    const req = capturedCodexRequest as Record<string, unknown>;
    expect(req).toBeDefined();
    expect(req.model).toBe("gpt-5.3-codex");
    expect(req.stream).toBe(true);
    expect(req.useWebSocket).toBe(true);

    expect(frames).toEqual([
      { type: "response.created", response: { id: "resp_1" } },
      { type: "response.output_text.delta", delta: "Hi there" },
      { type: "response.completed", response: { id: "resp_1" } },
    ]);
    ws.close();
  });

  it("supports multiple sequential response.create frames on one socket", async () => {
    const { ws } = await connectClient(port);

    const first = receiveJsonFrames(ws, 3);
    ws.send(RESPONSE_CREATE_BODY);
    await first;

    const second = receiveJsonFrames(ws, 3);
    ws.send(
      JSON.stringify({
        model: "codex",
        input: [{ role: "user", content: "again" }],
        stream: true,
        previous_response_id: "resp_1",
      }),
    );
    const frames = await second;

    expect(requestedStreams).toBe(2);
    const req = capturedCodexRequest as Record<string, unknown>;
    expect(req.previous_response_id).toBe("resp_1");
    expect(frames).toHaveLength(3);
    ws.close();
  });

  it("keeps HTTP POST + SSE working as the fallback (WS server attached)", async () => {
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: RESPONSE_CREATE_BODY,
    });

    expect(res.status).toBe(200);
    const buf = await new Response(res.body).text();
    // The mocked handleProxyRequest returns this SSE text verbatim.
    expect(buf).toContain("event: response.created");
  });

  it("forwards non-SSE error responses back as an error frame", async () => {
    const { handleProxyRequest } = await import("@src/routes/shared/proxy-handler.js");
    const mock = handleProxyRequest as ReturnType<typeof vi.fn>;
    mock.mockImplementationOnce(async () =>
      new Response(
        JSON.stringify({ type: "error", error: { type: "server_error", code: "no_available_accounts", message: "No accounts" } }),
        { status: 503, headers: { "content-type": "application/json" } },
      ),
    );

    const { ws } = await connectClient(port);
    const received = receiveJsonFrames(ws, 1);
    ws.send(RESPONSE_CREATE_BODY);
    const [frame] = await received;

    expect((frame as Record<string, unknown>).type).toBe("error");
    ws.close();
  });

  it("normalizes OpenAI-style {error:{...}} (no top-level type) to a typed error frame", async () => {
    const { handleProxyRequest } = await import("@src/routes/shared/proxy-handler.js");
    const mock = handleProxyRequest as ReturnType<typeof vi.fn>;
    mock.mockImplementationOnce(async () =>
      new Response(
        JSON.stringify({
          error: { message: "Invalid API key provided", type: "invalid_request_error", code: "invalid_api_key" },
        }),
        { status: 401, headers: { "content-type": "application/json" } },
      ),
    );

    const { ws } = await connectClient(port);
    const received = receiveJsonFrames(ws, 1);
    ws.send(RESPONSE_CREATE_BODY);
    const [frame] = await received;

    const f = frame as Record<string, unknown>;
    expect(f.type).toBe("error");
    expect((f.error as Record<string, unknown>).type).toBe("invalid_request_error");
    expect((f.error as Record<string, unknown>).code).toBe("invalid_api_key");
    expect((f.error as Record<string, unknown>).message).toBe("Invalid API key provided");
    ws.close();
  });

  it("preserves response-continuity diagnostics in an error frame", async () => {
    const { handleProxyRequest } = await import("@src/routes/shared/proxy-handler.js");
    const mock = handleProxyRequest as ReturnType<typeof vi.fn>;
    mock.mockImplementationOnce(async () =>
      new Response(
        JSON.stringify({
          type: "error",
          error: {
            message: "Owning WebSocket was closed",
            type: "invalid_request_error",
            code: "ws_response_history_gone",
            continuity_reason: "transport_closed",
            retryable: false,
          },
        }),
        { status: 410, headers: { "content-type": "application/json" } },
      ),
    );

    const { ws } = await connectClient(port);
    const received = receiveJsonFrames(ws, 1);
    ws.send(RESPONSE_CREATE_BODY);
    const [frame] = await received;
    expect(frame).toMatchObject({
      type: "error",
      error: {
        code: "ws_response_history_gone",
        continuity_reason: "transport_closed",
        retryable: false,
        status: 410,
      },
    });
    ws.close();
  });

  it("preserves diagnostics from a non-200 SSE response.failed frame", async () => {
    const { handleProxyRequest } = await import("@src/routes/shared/proxy-handler.js");
    const error = {
      type: "rate_limit_error", code: "ws_response_owner_unavailable",
      continuity_reason: "account_temporarily_unavailable", retryable: true, message: "Owner cooling down",
    };
    vi.mocked(handleProxyRequest).mockImplementationOnce(async () => new Response(
      `event: response.failed\ndata: ${JSON.stringify({ type: "response.failed", response: { error } })}\n\n`,
      { status: 429, headers: { "content-type": "text/event-stream" } },
    ));
    const { ws } = await connectClient(port);
    const received = receiveJsonFrames(ws, 1);
    ws.send(RESPONSE_CREATE_BODY);
    expect((await received)[0]).toMatchObject({ type: "error", error: { ...error, status: 429 } });
    ws.close();
  });

  it("rejects an upgrade with a disabled client key with 401 (validateAccess at handshake)", async () => {
    mockConfig.server.proxy_api_key = "master-key";
    const entry = clientKeyPool.createKey({ name: "disabled-key", key: "ck-disabled" });
    clientKeyPool.updateKey(entry.id, { status: "disabled" });

    await expect(connectClient(port, "Bearer ck-disabled")).rejects.toMatchObject({
      status: 401,
    });
  });

  it("rejects an upgrade with an over-budget client key with 429 (validateAccess at handshake)", async () => {
    mockConfig.server.proxy_api_key = "master-key";
    clientKeyPool.createKey({ name: "broke-key", key: "ck-broke", max_budget_usd: 1 });
    const brokeEntry = clientKeyPool.getByKey("ck-broke");
    brokeEntry!.used_cost_usd = 1.5;

    await expect(connectClient(port, "Bearer ck-broke")).rejects.toMatchObject({
      status: 429,
    });
  });

  it("accepts an upgrade using an x-api-key header (same locations as the POST route)", async () => {
    mockConfig.server.proxy_api_key = "master-key";
    const { ws } = await connectWithHeaders(port, { "x-api-key": "master-key" });
    expect(ws.readyState).toBe(WebSocket.OPEN);
    ws.close();
  });

  it("returns a structured connection_busy error when a frame is sent mid-flight", async () => {
    const { handleProxyRequest } = await import("@src/routes/shared/proxy-handler.js");
    const mock = handleProxyRequest as ReturnType<typeof vi.fn>;
    // Hold the stream open so the connection stays busy while we pipeline.
    mock.mockImplementationOnce(async () =>
      new Response(
        new ReadableStream({
          start(controller) {
            const encoder = new TextEncoder();
            controller.enqueue(
              encoder.encode(
                'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_1"}}\n\n',
              ),
            );
            // Intentionally never close: keeps the first dispatch in flight.
          },
        }),
        { status: 200, headers: { "content-type": "text/event-stream" } },
      ),
    );

    const { ws } = await connectClient(port);
    const first = receiveJsonFrames(ws, 1);
    ws.send(RESPONSE_CREATE_BODY);
    await first;

    const busy = receiveJsonFrames(ws, 1);
    ws.send(RESPONSE_CREATE_BODY);
    const [frame] = await busy;

    const f = frame as Record<string, unknown>;
    expect(f.type).toBe("error");
    expect((f.error as Record<string, unknown>).code).toBe("connection_busy");
    ws.close();
  });
});
