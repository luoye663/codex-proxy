/**
 * Client-facing WebSocket support for `/v1/responses` (issue #681).
 *
 * Newer Codex CLI clients (wire_api="responses") probe `ws://host:port/v1/responses`
 * via a WebSocket upgrade (GET + `Upgrade: websocket`) and fall back to HTTPS
 * POST+SSE when the proxy returns 404. This module wires a `ws` WebSocketServer
 * onto the same underlying node `http.Server` that `@hono/node-server` serves, so
 * upgrades against `/v1/responses` are accepted.
 *
 * The socket itself is only a transport: nothing runs until the client sends a
 * `response.create` JSON frame. Each frame is dispatched through the EXACT same
 * POST `/v1/responses` handler (via `app.request()`), which re-runs the shared
 * account-pool rotation / upstream routing / SSE streaming path. The resulting SSE
 * events are then forwarded back to the client as WebSocket text frames — each frame
 * carries the `data:` JSON payload of one event, mirroring what the Codex backend
 * emits over its own WebSocket. The socket stays open for the next `response.create`
 * frame (multi-turn / `previous_response_id` resume support).
 *
 * HTTP POST + SSE remains the fallback and is untouched.
 */

import { WebSocket, WebSocketServer } from "ws";
import type { Server, IncomingMessage } from "http";
import type { Duplex } from "stream";
import type { Hono } from "hono";
import type { AccountPool } from "../auth/account-pool.js";
import type { ClientKeyPool } from "../auth/client-key-pool.js";
import { getConfig } from "../config.js";
import { parseSSEStream } from "../proxy/codex-sse.js";

/** The only path this server accepts upgrades for. */
const UPGRADE_PATH = "/v1/responses";

const WS_OPEN = 1;

/**
 * How long `close()` waits for a graceful per-socket close handshake before
 * force-terminating remaining clients. Bounds shutdown so half-open or
 * unreachable peers cannot block `server.close()` indefinitely.
 */
const SHUTDOWN_CLOSE_TIMEOUT_MS = 1000;

/**
 * Hop-by-hop / handshake-only headers from the upgrade request that must not be
 * forwarded onto the synthetic POST (Fetch would reject or mis-parse them).
 */
const STRIPPED_HEADERS: ReadonlySet<string> = new Set([
  "connection",
  "content-length",
  "transfer-encoding",
  "host",
  "upgrade",
  "sec-websocket-key",
  "sec-websocket-version",
  "sec-websocket-extensions",
  "sec-websocket-protocol",
]);

export interface ResponsesWebSocketServerOptions {
  /** The node http.Server that @hono/node-server's serve() returned. */
  server: Server;
  /** The mounted Hono app exposing `/v1/responses` (POST). Frames are re-dispatched here. */
  app: Hono;
  accountPool: AccountPool;
  clientKeyPool?: ClientKeyPool;
}

export class ResponsesWebSocketServer {
  private readonly server: Server;
  private readonly app: Hono;
  private readonly accountPool: AccountPool;
  private readonly clientKeyPool?: ClientKeyPool;
  private readonly wss: WebSocketServer;
  private readonly onUpgradeBound: (req: IncomingMessage, socket: Duplex, head: Buffer) => void;
  private closed = false;

  constructor(options: ResponsesWebSocketServerOptions) {
    this.server = options.server;
    this.app = options.app;
    this.accountPool = options.accountPool;
    this.clientKeyPool = options.clientKeyPool;

    this.wss = new WebSocketServer({ noServer: true });
    this.wss.on("connection", (ws, req) => this.handleConnection(ws, req));
    this.wss.on("error", (err) => {
      console.error("[responses-ws] WebSocketServer error:", err);
    });

    this.onUpgradeBound = (req, socket, head) => this.handleUpgrade(req, socket, head);
    this.server.on("upgrade", this.onUpgradeBound);
  }

  /**
   * Detach the upgrade listener and close all client sockets. Called from the
   * existing shutdown path in `src/index.ts`.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.server.off("upgrade", this.onUpgradeBound);
    for (const client of this.wss.clients) {
      try {
        client.close(1000, "server shutdown");
      } catch {
        /* already closing */
      }
    }
    await new Promise<void>((resolve) => {
      let settled = false;
      const done = () => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };
      // Bound the wait: if a peer never completes the close handshake, force-
      // terminate it so shutdown cannot block indefinitely.
      const timer = setTimeout(() => {
        for (const client of this.wss.clients) {
          try {
            client.terminate();
          } catch {
            /* already closed */
          }
        }
        done();
      }, SHUTDOWN_CLOSE_TIMEOUT_MS);
      try {
        this.wss.close(() => {
          clearTimeout(timer);
          done();
        });
      } catch {
        clearTimeout(timer);
        done();
      }
    });
  }

  // ── Upgrade handling ────────────────────────────────────────────

  private handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    let pathname: string;
    try {
      pathname = new URL(req.url ?? "/", "http://localhost").pathname;
    } catch {
      socket.destroy();
      return;
    }

    if (pathname !== UPGRADE_PATH) {
      // Not ours — destroy so the socket doesn't hang (no other upgrade consumer).
      socket.destroy();
      return;
    }

    const auth = this.authorize(req);
    if (!auth.allowed) {
      this.rejectUpgrade(socket, auth.statusCode, auth.message);
      return;
    }

    this.wss.handleUpgrade(req, socket, head, (ws) => this.wss.emit("connection", ws, req));
  }

  /** Same auth semantics as the POST route's `apiKeyAuth` middleware. */
  private authorize(req: IncomingMessage): { allowed: boolean; statusCode: number; message: string } {
    const key = this.extractProxyApiKey(req);
    if (key && this.accountPool.validateProxyApiKey(key)) {
      return { allowed: true, statusCode: 0, message: "" };
    }
    if (key && this.clientKeyPool) {
      // Reuse the HTTP middleware's validation so disabled, expired, over-budget,
      // or token-limited client keys are rejected at the handshake, not later.
      const validation = this.clientKeyPool.validateAccess(key);
      if (validation.allowed) {
        return { allowed: true, statusCode: 0, message: "" };
      }
      return {
        allowed: false,
        statusCode: validation.statusCode ?? 401,
        message: validation.message ?? "Unauthorized",
      };
    }
    // Passthrough / no-auth mode: no master proxy_api_key is configured.
    const config = getConfig();
    if (!config?.server?.proxy_api_key) {
      return { allowed: true, statusCode: 0, message: "" };
    }
    return { allowed: false, statusCode: 401, message: "Invalid proxy API key" };
  }

  /**
   * Extract a proxy API key from the upgrade request, supporting the same
   * locations as the HTTP routes' `extractProxyApiKey()`: `?key=`,
   * `x-goog-api-key`, `x-api-key`, and `Authorization: Bearer`.
   */
  private extractProxyApiKey(req: IncomingMessage): string | null {
    let queryKey: string | null = null;
    try {
      queryKey = new URL(req.url ?? "/", "http://localhost").searchParams.get("key");
    } catch {
      /* fall through to header extraction */
    }
    const googKey = this.headerValue(req.headers["x-goog-api-key"]);
    const xApiKey = this.headerValue(req.headers["x-api-key"]);
    const authHeader = this.headerValue(req.headers.authorization);
    const bearerKey = authHeader ? authHeader.replace(/^bearer\s+/i, "").trim() : null;
    return queryKey ?? googKey ?? xApiKey ?? (bearerKey || null);
  }

  private headerValue(value: string | string[] | undefined): string | null {
    if (typeof value === "string" && value.length > 0) return value;
    if (Array.isArray(value) && value.length > 0) return value[0];
    return null;
  }

  private rejectUpgrade(socket: Duplex, status: number, message: string): void {
    const reason =
      status === 401 ? "Unauthorized"
        : status === 429 ? "Too Many Requests"
          : status === 403 ? "Forbidden"
            : "Error";
    const body = `${message}\n`;
    socket.write(
      `HTTP/1.1 ${status} ${reason}\r\n` +
        "Content-Type: text/plain\r\n" +
        "Connection: close\r\n" +
        `Content-Length: ${Buffer.byteLength(body)}\r\n` +
        "\r\n" +
        body,
    );
    socket.destroy();
  }

  // ── Connection handling ─────────────────────────────────────────

  private handleConnection(ws: WebSocket, req: IncomingMessage): void {
    let busy = false;
    const abortController = new AbortController();

    ws.on("message", (data) => {
      // One in-flight `response.create` per socket at a time.
      if (busy) {
        // Never silently drop a pipelined frame — tell the client the
        // connection is busy with a structured error so it does not wait
        // forever for a response that will never arrive.
        this.sendErrorFrame(
          ws,
          JSON.stringify({
            type: "error",
            error: {
              type: "server_error",
              code: "connection_busy",
              message: "A response.create is already in progress on this connection",
            },
          }),
          409,
        );
        return;
      }
      busy = true;
      const raw = typeof data === "string" ? data : (data as Buffer).toString("utf-8");
      void this.dispatch(ws, req, raw, abortController.signal).finally(() => {
        busy = false;
      });
    });

    // A transport/protocol error on this client must not become an uncaught
    // EventEmitter error (which the process-level handler would rethrow and
    // could terminate the whole proxy). Clean up only this socket.
    ws.on("error", () => {
      try {
        ws.close(1011, "internal error");
      } catch {
        ws.terminate();
      }
    });

    // Client disconnect must cancel the in-flight synthetic request so the
    // upstream stream / account slot is released promptly.
    ws.on("close", () => {
      abortController.abort();
    });
  }

  /**
   * Run one `response.create` JSON frame through the exact POST `/v1/responses`
   * handler and stream the resulting SSE events back as WS text frames.
   */
  private async dispatch(ws: WebSocket, req: IncomingMessage, body: string, signal: AbortSignal): Promise<void> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    for (const [key, value] of Object.entries(req.headers)) {
      if (typeof value === "string" && !STRIPPED_HEADERS.has(key)) {
        headers[key.toLowerCase()] = value;
      }
    }

    let response: Response;
    try {
      response = await this.app.request("/v1/responses", {
        method: "POST",
        headers,
        body,
        signal,
      });
    } catch (err) {
      // Abort is expected when the client disconnects mid-request.
      if (signal.aborted || this.isAbortError(err)) return;
      const message = err instanceof Error ? err.message : String(err);
      this.sendErrorFrame(ws, message);
      return;
    }

    const contentType = response.headers.get("content-type") ?? "";
    if (!contentType.includes("text/event-stream")) {
      const textBody = await response.text();
      this.sendErrorFrame(ws, textBody, response.status);
      return;
    }

    try {
      for await (const event of parseSSEStream(response)) {
        if (ws.readyState !== WS_OPEN || signal.aborted) break;
        if (!response.ok) {
          const data = event.data as Record<string, unknown>;
          const failed = data?.response as Record<string, unknown> | undefined;
          this.sendErrorFrame(ws, JSON.stringify(failed?.error ? { error: failed.error } : data), response.status);
          continue;
        }
        // Forward the JSON payload of each SSE `data:` line.
        ws.send(JSON.stringify(event.data));
      }
    } catch (err) {
      if (signal.aborted || this.isAbortError(err)) return;
      const message = err instanceof Error ? err.message : String(err);
      this.sendErrorFrame(ws, message);
    } finally {
      // Release the upstream connection even when we broke out early.
      try {
        await response.body?.cancel();
      } catch {
        /* already closed */
      }
    }
  }

  /**
   * Send an error as a typed Responses WebSocket error frame (best-effort;
   * keeps the socket alive). All error payloads are normalized to
   * `{type:"error", error:{...}}` so clients can classify terminal errors.
   */
  private sendErrorFrame(ws: WebSocket, rawMessage: string, status?: number): void {
    let errorType = "server_error";
    let code = "proxy_error";
    let message = rawMessage;
    let continuityReason: string | undefined;
    let retryable: boolean | undefined;
    try {
      const parsed = JSON.parse(rawMessage) as Record<string, unknown>;
      const errObj =
        parsed && typeof parsed.error === "object" && parsed.error !== null
          ? (parsed.error as Record<string, unknown>)
          : undefined;
      message =
        typeof errObj?.message === "string"
          ? errObj.message
          : typeof parsed.message === "string"
            ? parsed.message
            : rawMessage;
      if (errObj && typeof errObj.type === "string") errorType = errObj.type;
      if (errObj && typeof errObj.code === "string") code = errObj.code;
      if (errObj && typeof errObj.continuity_reason === "string") {
        continuityReason = errObj.continuity_reason;
      }
      if (errObj && typeof errObj.retryable === "boolean") retryable = errObj.retryable;
    } catch {
      /* keep raw string */
    }
    const text = JSON.stringify({
      type: "error",
      error: {
        type: errorType,
        code,
        message,
        ...(continuityReason ? { continuity_reason: continuityReason } : {}),
        ...(retryable !== undefined ? { retryable } : {}),
        ...(status !== undefined ? { status } : {}),
      },
    });
    if (ws.readyState === WS_OPEN) {
      try {
        ws.send(text);
      } catch {
        /* socket already closed */
      }
    }
  }

  private isAbortError(err: unknown): boolean {
    return err instanceof Error && err.name === "AbortError";
  }
}
