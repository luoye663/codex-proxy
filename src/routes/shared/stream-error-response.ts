import type { Context } from "hono";
import { stream } from "hono/streaming";
import type { StatusCode } from "hono/utils/http-status";
import type { FormatAdapter, ProxyErrorDetails, ProxyRequest } from "./proxy-handler-types.js";

export function canReturnStreamError(req: ProxyRequest, fmt: FormatAdapter): boolean {
  return req.isStreaming && typeof fmt.formatStreamError === "function";
}

export function streamErrorResponse(
  c: Context,
  fmt: FormatAdapter,
  status: number,
  message: string,
  details?: ProxyErrorDetails,
): Response {
  // Local WS ownership/capacity failures happen before a response exists and
  // must expose their real downstream status. Preserve the legacy HTTP 200
  // SSE envelope for ordinary upstream errors that are represented by a
  // terminal response.failed event.
  if (details?.code?.startsWith("ws_")) c.status(status as StatusCode);
  c.header("Content-Type", "text/event-stream");
  c.header("Cache-Control", "no-cache");
  c.header("Connection", "keep-alive");

  return stream(c, async (s) => {
    await s.write(
      (details
        ? fmt.formatStreamError?.(status, message, details)
        : fmt.formatStreamError?.(status, message)) ??
        `data: ${JSON.stringify({ error: { message, type: "stream_error" } })}\n\n`,
    );
  });
}
