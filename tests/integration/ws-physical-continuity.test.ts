import { WebSocketServer, type WebSocket } from "ws";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createWebSocketResponse, type WsCreateRequest } from "@src/proxy/ws-transport.js";
import { PreviousResponseWebSocketError } from "@src/proxy/codex-types.js";
import { WsConnectionPool } from "@src/proxy/ws-pool.js";

interface SeenRequest {
  connection: number;
  payload: WsCreateRequest;
  socket: WebSocket;
}

function request(marker: string, previousResponseId?: string): WsCreateRequest {
  return {
    type: "response.create",
    model: "gpt-test",
    instructions: "test",
    input: [{ role: "user", content: marker }],
    ...(previousResponseId ? { previous_response_id: previousResponseId } : {}),
  };
}

async function drain(response: Response): Promise<string> {
  return response.text();
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("condition timeout");
}

describe("physical WebSocket response continuity", () => {
  let server: WebSocketServer;
  let url: string;
  let pool: WsConnectionPool;
  let connectionCount: number;
  let seen: SeenRequest[];
  let onRequest: (seenRequest: SeenRequest) => void;

  beforeEach(async () => {
    connectionCount = 0;
    seen = [];
    onRequest = () => undefined;
    server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    server.on("connection", (socket) => {
      const connection = ++connectionCount;
      socket.on("message", (raw) => {
        const seenRequest = {
          connection,
          payload: JSON.parse(raw.toString()) as WsCreateRequest,
          socket,
        };
        seen.push(seenRequest);
        onRequest(seenRequest);
      });
    });
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("invalid test server address");
    url = `ws://127.0.0.1:${address.port}`;
    pool = new WsConnectionPool({ enabled: true, maxAgeMs: 60_000, maxPerAccount: 8 }, { startGc: false });
  });

  afterEach(async () => {
    await pool.shutdown();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  function context(poolKey = "entry:conv:variant") {
    return { pool, entryId: "entry", poolKey };
  }

  function complete(item: SeenRequest, responseId: string): void {
    item.socket.send(JSON.stringify({ type: "response.created", response: { id: responseId } }));
    item.socket.send(JSON.stringify({ type: "response.completed", response: { id: responseId } }));
  }

  it("continues the newest response only on the owning physical connection", async () => {
    let sequence = 0;
    onRequest = (item) => complete(item, `resp_${++sequence}`);

    await drain(await createWebSocketResponse(url, {}, request("first"), undefined, null, undefined, context()));
    expect(pool.ownerWsId("resp_1")).not.toBeNull();

    await drain(await createWebSocketResponse(url, {}, request("second", "resp_1"), undefined, null, undefined, context()));
    expect(connectionCount).toBe(1);
    expect(seen.map((item) => item.connection)).toEqual([1, 1]);
    expect(seen[1].payload.previous_response_id).toBe("resp_1");
    expect(pool.ownerWsId("resp_1")).toBeNull();
    expect(pool.ownerWsId("resp_2")).not.toBeNull();

    await expect(
      createWebSocketResponse(url, {}, request("stale", "resp_1"), undefined, null, undefined, context()),
    ).rejects.toBeInstanceOf(PreviousResponseWebSocketError);
    expect(connectionCount).toBe(1);
    expect(seen).toHaveLength(2);
  });

  it("fails closed when the owning connection is busy without opening a one-shot", async () => {
    const held = new AbortController();
    onRequest = (item) => {
      const marker = (item.payload.input[0] as { content?: string }).content;
      if (marker === "first") complete(item, "resp_1");
    };

    await drain(await createWebSocketResponse(url, {}, request("first"), undefined, null, undefined, context()));
    const pending = createWebSocketResponse(
      url, {}, request("held", "resp_1"), held.signal, null, undefined, context(),
    );
    pending.catch(() => undefined);
    await waitFor(() => seen.length === 2);

    await expect(
      createWebSocketResponse(url, {}, request("concurrent", "resp_1"), undefined, null, undefined, context()),
    ).rejects.toMatchObject({ name: "PreviousResponseWebSocketError" });
    expect(connectionCount).toBe(1);
    expect(seen).toHaveLength(2);
    held.abort();
  });

  it("retains a pooled branch when a full-input request races a busy canonical socket", async () => {
    const held = new AbortController();
    let sequence = 0;
    onRequest = (item) => {
      const marker = (item.payload.input[0] as { content?: string }).content;
      if (marker !== "held-root") complete(item, `resp_branch_${++sequence}`);
    };

    const pendingRoot = createWebSocketResponse(
      url, {}, request("held-root"), held.signal, null, undefined, context(),
    );
    pendingRoot.catch(() => undefined);
    await waitFor(() => seen.length === 1);

    await drain(await createWebSocketResponse(
      url, {}, request("branch-root"), undefined, null, undefined, context(),
    ));
    expect(connectionCount).toBe(2);
    expect(pool.ownerWsId("resp_branch_1")).not.toBeNull();

    await drain(await createWebSocketResponse(
      url, {}, request("branch-next", "resp_branch_1"), undefined, null, undefined, context(),
    ));
    expect(connectionCount).toBe(2);
    expect(seen[2].connection).toBe(seen[1].connection);
    held.abort();
  });

  it("fails the root request at pool capacity instead of creating an orphan one-shot response", async () => {
    await pool.shutdown();
    pool = new WsConnectionPool(
      { enabled: true, maxAgeMs: 60_000, maxPerAccount: 1 },
      { startGc: false },
    );
    const held = new AbortController();
    onRequest = () => undefined;

    const pendingRoot = createWebSocketResponse(
      url, {}, request("held-root"), held.signal, null, undefined, context(),
    );
    pendingRoot.catch(() => undefined);
    await waitFor(() => seen.length === 1);

    await expect(createWebSocketResponse(
      url, {}, request("overflow-root"), undefined, null, undefined, context(),
    )).rejects.toMatchObject({
      name: "WsPoolUnavailableError",
      status: 429,
      poolReason: "capacity",
    });
    expect(connectionCount).toBe(1);
    expect(seen).toHaveLength(1);
    held.abort();
  });

  it("fails closed after the owner dies without opening a replacement carrying the old ID", async () => {
    onRequest = (item) => complete(item, "resp_1");
    await drain(await createWebSocketResponse(url, {}, request("first"), undefined, null, undefined, context()));
    server.clients.forEach((socket) => socket.terminate());
    await waitFor(() => pool.ownerWsId("resp_1") === null);

    await expect(
      createWebSocketResponse(url, {}, request("after-death", "resp_1"), undefined, null, undefined, context()),
    ).rejects.toBeInstanceOf(PreviousResponseWebSocketError);
    expect(connectionCount).toBe(1);
    expect(seen).toHaveLength(1);
  });

  it("keeps metadata behind the barrier and rejects a following 400 before returning Response", async () => {
    onRequest = (item) => {
      const marker = (item.payload.input[0] as { content?: string }).content;
      if (marker === "first") {
        complete(item, "resp_1");
        return;
      }
      item.socket.send(JSON.stringify({ type: "response.created", response: { id: "resp_failed" } }));
      item.socket.send(JSON.stringify({ type: "response.in_progress", response: { id: "resp_failed" } }));
      item.socket.send(JSON.stringify({ type: "codex.response.metadata", headers: { "x-test": "1" } }));
      item.socket.send(JSON.stringify({
        type: "error",
        status: 400,
        error: { code: "previous_response_not_found", message: "not found" },
      }));
    };

    await drain(await createWebSocketResponse(url, {}, request("first"), undefined, null, undefined, context()));
    await expect(
      createWebSocketResponse(url, {}, request("second", "resp_1"), undefined, null, undefined, context()),
    ).rejects.toMatchObject({ status: 400 });
    expect(pool.ownerWsId("resp_1")).toBeNull();
    expect(connectionCount).toBe(1);
    expect(seen).toHaveLength(2);
  });
});
