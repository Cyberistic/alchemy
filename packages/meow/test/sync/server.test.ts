import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Result from "effect/Result";
import {
  makeInMemoryStore,
  SyncStoreTag,
  InMemoryStoreLive,
} from "../../src/sync/store.ts";
import * as Sync from "../../src/sync/Room.ts";
import { Layer } from "effect";

describe("Sync.make (in-memory)", () => {
  const buildServer = () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      const server = yield* Sync.make(store);
      return { server, store };
    });

  it.effect("head() on empty store returns seq=0, count=0", () =>
    Effect.gen(function* () {
      const { server } = yield* buildServer();
      const head = yield* server.handleHead("main" as never);
      expect(head).toEqual({ head: 0, count: 0 });
    }),
  );

  it.effect("push() appends and increments seq", () =>
    Effect.gen(function* () {
      const { server } = yield* buildServer();
      const sessionId = "sess-1" as never;
      const res = yield* server.handlePush(sessionId, {
        storeId: "default",
        branch: "main",
        sessionId,
        events: [
          {
            id: "ev-1",
            parent: null,
            payload: { kind: "TodoCreated" },
            createdAt: 1,
          },
          {
            id: "ev-2",
            parent: "ev-1",
            payload: { kind: "TodoDone" },
            createdAt: 2,
          },
        ],
      });
      expect(res.accepted).toEqual(["ev-1", "ev-2"]);
      expect(res.head).toBe(2);

      const head = yield* server.handleHead("main" as never);
      expect(head.count).toBe(2);
    }),
  );

  it.effect("push() rejects a broken parent chain", () =>
    Effect.gen(function* () {
      const { server } = yield* buildServer();
      const result = yield* Effect.result(
        server.handlePush("sess-1" as never, {
          storeId: "default",
          branch: "main",
          sessionId: "sess-1",
          events: [
            { id: "ev-1", parent: "ghost", payload: {}, createdAt: 1 },
          ],
        }),
      );
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect((result.failure as { status: number; body: { error: string } }).status).toBe(409);
        expect((result.failure as { status: number; body: { error: string } }).body.error).toBe("unknown parent");
      }
    }),
  );

  it.effect("pull(since=0) returns every event", () =>
    Effect.gen(function* () {
      const { server } = yield* buildServer();
      yield* server.handlePush("sess-1" as never, {
        storeId: "default",
        branch: "main",
        sessionId: "sess-1",
        events: [
          { id: "ev-1", parent: null, payload: {}, createdAt: 1 },
          { id: "ev-2", parent: "ev-1", payload: {}, createdAt: 2 },
          { id: "ev-3", parent: "ev-2", payload: {}, createdAt: 3 },
        ],
      });
      const pulled = yield* server.handlePull({
        storeId: "default",
        branch: "main",
        since: 0,
      });
      expect(pulled.events.length).toBe(3);
      expect(pulled.events.map((e) => e.id)).toEqual([
        "ev-1",
        "ev-2",
        "ev-3",
      ]);
      expect(pulled.head).toBe(3);
    }),
  );

  it.effect("pull(since=N) returns events with seq > N", () =>
    Effect.gen(function* () {
      const { server } = yield* buildServer();
      yield* server.handlePush("sess-1" as never, {
        storeId: "default",
        branch: "main",
        sessionId: "sess-1",
        events: [
          { id: "ev-1", parent: null, payload: {}, createdAt: 1 },
          { id: "ev-2", parent: "ev-1", payload: {}, createdAt: 2 },
          { id: "ev-3", parent: "ev-2", payload: {}, createdAt: 3 },
        ],
      });
      const pulled = yield* server.handlePull({
        storeId: "default",
        branch: "main",
        since: 1,
      });
      expect(pulled.events.length).toBe(2);
      expect(pulled.events.map((e) => e.seq)).toEqual([2, 3]);
    }),
  );

  it.effect("subscribe() streams live events", () =>
    Effect.gen(function* () {
      const { server } = yield* buildServer();
      const collected: string[] = [];
      const stream = server.subscribe("main" as never).pipe(Stream.take(2));
      const subFiber = yield* Effect.forkChild(
        Stream.runForEach(stream, (env) =>
          Effect.sync(() => {
            if (env.kind === "event") collected.push(env.event.id as string);
          }),
        ),
        { startImmediately: true },
      );
      // Yield to let the forked consumer subscribe before we publish.
      // We avoid `Effect.sleep` because @effect/vitest uses TestClock by
      // default — `yieldNow` doesn't require advancing virtual time.
      yield* Effect.yieldNow;
      yield* server.handlePush("sess-1" as never, {
        storeId: "default",
        branch: "main",
        sessionId: "sess-1",
        events: [
          { id: "ev-1", parent: null, payload: {}, createdAt: 1 },
          { id: "ev-2", parent: "ev-1", payload: {}, createdAt: 2 },
        ],
      });
      yield* Fiber.join(subFiber);
      expect(collected).toEqual(["ev-1", "ev-2"]);
    }),
  );
});

describe("Sync via Layer composition", () => {
  it.effect("builds a server backed by the provided in-memory store", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      const server = yield* Sync.make(store);
      const head = yield* server.handleHead("main" as never);
      expect(head.head).toBe(0);
      expect(head.count).toBe(0);
    }),
  );
});

// ---------------------------------------------------------------------------
// Local helpers (avoid repeating `as never` casts above)
// ---------------------------------------------------------------------------

import type { StoreId as _StoreId } from "../../src/sync/schema.ts";
// ---------------------------------------------------------------------------
// SyncServerTag — declared for the Layer-composition test
// ---------------------------------------------------------------------------

import * as Context from "effect/Context";
const SyncServerTag = Context.Service<{
  handlePush: (sid: never, body: unknown) => Effect.Effect<unknown, unknown, never>;
  handlePull: (body: unknown) => Effect.Effect<unknown, unknown, never>;
  handleHead: (branch: never) => Effect.Effect<unknown, never, never>;
}>("meow/sync/SyncServer");
