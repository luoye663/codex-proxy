import { EventEmitter } from "events";
import { afterEach, describe, expect, it } from "vitest";
import "@helpers/account-pool-setup.js";
import { createMemoryPersistence } from "@helpers/account-pool-factory.js";
import { createValidJwt } from "@helpers/jwt.js";
import { AccountPool } from "@src/auth/account-pool.js";
import {
  PersistentWs,
  _resetWsPoolForTests,
  getWsPool,
  type PersistentWsHooks,
  type WsLike,
} from "@src/proxy/ws-pool.js";

class MockWs extends EventEmitter implements WsLike {
  readyState = 1;
  send(): void {}
  ping(): void {}
  close(code = 1000, reason = ""): void {
    this.readyState = 3;
    queueMicrotask(() => this.emit("close", code, Buffer.from(reason)));
  }
  push(payload: Record<string, unknown>): void {
    this.emit("message", JSON.stringify(payload));
  }
}

async function seedOwner(accountPool: AccountPool): Promise<{
  entryId: string;
  responseId: string;
}> {
  const entryId = accountPool.addAccount(createValidJwt({ accountId: "acct-owner" }));
  const responseId = "resp_owner";
  const wsPool = getWsPool();
  const mock = new MockWs();
  const acquired = await wsPool.acquire(
    entryId,
    `${entryId}:conversation`,
    async (deps: { entryId: string; poolKey: string; hooks: PersistentWsHooks }) =>
      new PersistentWs({ ...deps, ws: mock }),
  );
  if (!("ws" in acquired)) throw new Error("expected pooled WS");
  const sent = acquired.ws.send({
    request: { type: "response.create", model: "m", instructions: "", input: [] },
    signal: undefined,
    onRateLimits: undefined,
    reused: false,
  });
  mock.push({ type: "response.completed", response: { id: responseId } });
  await sent;
  await new Promise<void>((resolve) => queueMicrotask(resolve));
  return { entryId, responseId };
}

async function waitFor(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setTimeout(resolve, 2));
  }
  throw new Error("condition timeout");
}

describe("AccountPool WebSocket owner retention", () => {
  afterEach(() => _resetWsPoolForTests());

  it("preserves an authenticated owner across access-token refresh", async () => {
    const pool = new AccountPool({ persistence: createMemoryPersistence() });
    const { entryId, responseId } = await seedOwner(pool);

    pool.updateToken(entryId, createValidJwt({ accountId: "acct-owner", expInSeconds: 7200 }));
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    expect(getWsPool().lookupResponseOwner(responseId)).toMatchObject({
      kind: "live",
      entryId,
    });
  });

  it("preserves an owner during temporary 429 exclusion", async () => {
    const pool = new AccountPool({ persistence: createMemoryPersistence() });
    const { entryId, responseId } = await seedOwner(pool);

    pool.applyRateLimit429(entryId, { retryAfterSec: 60 });
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    expect(getWsPool().lookupResponseOwner(responseId)).toMatchObject({
      kind: "live",
      entryId,
    });
  });

  it("tombstones owners when the account is permanently disabled", async () => {
    const pool = new AccountPool({ persistence: createMemoryPersistence() });
    const { entryId, responseId } = await seedOwner(pool);

    pool.markStatus(entryId, "disabled");
    await waitFor(() => getWsPool().lookupResponseOwner(responseId).kind === "gone");

    expect(getWsPool().lookupResponseOwner(responseId)).toMatchObject({
      kind: "gone",
      tombstone: { entryId, reason: "account_disabled" },
    });
  });

  it("tombstones owners when logout clears every account", async () => {
    const pool = new AccountPool({ persistence: createMemoryPersistence() });
    const { entryId, responseId } = await seedOwner(pool);

    pool.clearToken();
    await waitFor(() => getWsPool().lookupResponseOwner(responseId).kind === "gone");

    expect(getWsPool().lookupResponseOwner(responseId)).toMatchObject({
      kind: "gone",
      tombstone: { entryId, reason: "account_removed" },
    });
  });
});
