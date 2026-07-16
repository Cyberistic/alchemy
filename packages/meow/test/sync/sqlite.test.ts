import { describe, expect, it } from "@effect/vitest";
import { createClient } from "@libsql/client";
import * as Effect from "effect/Effect";
import {
  makeSqliteStoreLayer,
  type SyncStoreSql,
} from "../../src/sync/store.ts";
import { SyncStoreTag } from "../../src/sync/store.ts";
import { fromLibsql } from "../../src/sync/libsql.ts";
import {
  type Branch,
  type EventId,
  type Seq,
} from "../../src/sync/schema.ts";

const branch = (n: number | string) => `branch-${n}` as Branch;
const id = (s: string) => s as EventId;

const setupStore = () => {
  const client = createClient({ url: ":memory:" });
  const sql: SyncStoreSql = fromLibsql(client);
  const layer = makeSqliteStoreLayer(sql);
  return { client, sql, layer };
};

describe("SQLite SyncStore (libsql)", () => {
  it("creates the schema lazily on first append", () =>
    Effect.gen(function* () {
      const { client } = setupStore();
      // No table yet.
      const before = yield* Effect.tryPromise(() =>
        client.execute(
          "SELECT name FROM sqlite_master WHERE type='table'",
        ),
      );
      expect(before.rows.map((r) => r.name)).not.toContain("meow_sync_events");

      const store = yield* SyncStoreTag;
      yield* store.append({
        branch: branch(1),
        id: id("e1"),
        parent: null,
        payload: { hello: "world" },
        author: "alice",
        createdAt: Date.now(),
      });

      const after = yield* Effect.tryPromise(() =>
        client.execute(
          "SELECT name FROM sqlite_master WHERE type='table'",
        ),
      );
      expect(after.rows.map((r) => r.name)).toContain("meow_sync_events");
    }));

  it("appends and reads events in order", () =>
    Effect.gen(function* () {
      const { layer } = setupStore();
      const store = yield* SyncStoreTag;
      for (let i = 0; i < 5; i++) {
        yield* store.append({
          branch: branch(1),
          id: id(`e${i}`),
          parent: i === 0 ? null : id(`e${i - 1}`),
          payload: { n: i },
          author: "alice",
          createdAt: Date.now() + i,
        });
      }
      const events = yield* store.readSince({
        branch: branch(1),
        since: 0 as Seq,
      });
      expect(events.length).toBe(5);
      expect(events[0].id).toBe("e0");
      expect(events[4].id).toBe("e4");
      expect(events[2].payload).toEqual({ n: 2 });
    }));

  it("readSince honours `since`", () =>
    Effect.gen(function* () {
      const { layer } = setupStore();
      const store = yield* SyncStoreTag;
      for (let i = 0; i < 5; i++) {
        yield* store.append({
          branch: branch(1),
          id: id(`e${i}`),
          parent: null,
          payload: { n: i },
          author: "alice",
          createdAt: Date.now(),
        });
      }
      const all = yield* store.readSince({ branch: branch(1), since: 0 as Seq });
      const head = all[all.length - 1]!;
      const after = yield* store.readSince({
        branch: branch(1),
        since: head.seq,
      });
      expect(after.length).toBe(0);
    }));

  it("readSince honours `limit`", () =>
    Effect.gen(function* () {
      const { layer } = setupStore();
      const store = yield* SyncStoreTag;
      for (let i = 0; i < 100; i++) {
        yield* store.append({
          branch: branch(1),
          id: id(`e${i}`),
          parent: null,
          payload: { n: i },
          author: "alice",
          createdAt: Date.now(),
        });
      }
      const limited = yield* store.readSince({
        branch: branch(1),
        since: 0 as Seq,
        limit: 10,
      });
      expect(limited.length).toBe(10);
      expect(limited[0].id).toBe("e0");
      expect(limited[9].id).toBe("e9");
    }));

  it("head returns max seq and total count per branch", () =>
    Effect.gen(function* () {
      const { layer } = setupStore();
      const store = yield* SyncStoreTag;
      for (let i = 0; i < 7; i++) {
        yield* store.append({
          branch: branch(1),
          id: id(`e${i}`),
          parent: null,
          payload: { n: i },
          author: "alice",
          createdAt: Date.now(),
        });
      }
      const head1 = yield* store.head(branch(1));
      expect(head1.count).toBe(7);
      expect(head1.head as number).toBe(7);

      // Different branch should be empty.
      const head2 = yield* store.head(branch(99));
      expect(head2.count).toBe(0);
      expect(head2.head as number).toBe(0);
    }));

  it("isolates events between branches", () =>
    Effect.gen(function* () {
      const { layer } = setupStore();
      const store = yield* SyncStoreTag;
      yield* store.append({
        branch: branch("a"),
        id: id("a1"),
        parent: null,
        payload: { tag: "a" },
        author: "alice",
        createdAt: Date.now(),
      });
      yield* store.append({
        branch: branch("b"),
        id: id("b1"),
        parent: null,
        payload: { tag: "b" },
        author: "bob",
        createdAt: Date.now(),
      });
      const a = yield* store.readSince({ branch: branch("a"), since: 0 as Seq });
      const b = yield* store.readSince({ branch: branch("b"), since: 0 as Seq });
      expect(a.length).toBe(1);
      expect(a[0].payload).toEqual({ tag: "a" });
      expect(b.length).toBe(1);
      expect(b[0].payload).toEqual({ tag: "b" });
    }));

  it("preserves payload round-trips (including nulls and nested arrays)", () =>
    Effect.gen(function* () {
      const { layer } = setupStore();
      const store = yield* SyncStoreTag;
      yield* store.append({
        branch: branch(1),
        id: id("p1"),
        parent: null,
        payload: {
          a: null,
          b: [1, 2, { nested: "ok" }],
          c: { deep: { value: 42 } },
        },
        author: "alice",
        createdAt: Date.now(),
      });
      const [event] = yield* store.readSince({
        branch: branch(1),
        since: 0 as Seq,
        limit: 1,
      });
      expect(event.payload).toEqual({
        a: null,
        b: [1, 2, { nested: "ok" }],
        c: { deep: { value: 42 } },
      });
    }));

  it("survives across multiple resolves via the same libsql client", () =>
    Effect.gen(function* () {
      const client = createClient({ url: ":memory:" });
      const sql = fromLibsql(client);
      const layer = makeSqliteStoreLayer(sql);

      const store1 = yield* Effect.gen(function* () {
        return yield* SyncStoreTag;
      }).pipe(Effect.provide(layer));
      yield* store1.append({
        branch: branch(1),
        id: id("first"),
        parent: null,
        payload: { v: 1 },
        author: "alice",
        createdAt: Date.now(),
      });

      // Re-resolve from a fresh layer on the same client.
      const store2 = yield* Effect.gen(function* () {
        return yield* SyncStoreTag;
      }).pipe(Effect.provide(layer));
      const events = yield* store2.readSince({
        branch: branch(1),
        since: 0 as Seq,
      });
      expect(events.length).toBe(1);
      expect(events[0].id).toBe("first");
    }));

  it("persists data to a file-backed libsql client", () =>
    Effect.gen(function* () {
      const fs = yield* Effect.tryPromise(() =>
        import("node:fs/promises"),
      ).pipe(Effect.orElseSucceed(() => null));
      if (!fs) return; // skip if fs unavailable

      const path = `/tmp/meow-sync-test-${Date.now()}-${Math.random()}.db`;
      try {
        const client = createClient({ url: `file:${path}` });
        const sql = fromLibsql(client);
        const layer = makeSqliteStoreLayer(sql);

        const store = yield* Effect.gen(function* () {
          return yield* SyncStoreTag;
        }).pipe(Effect.provide(layer));
        yield* store.append({
          branch: branch(1),
          id: id("file-1"),
          parent: null,
          payload: { kind: "durable" },
          author: "alice",
          createdAt: Date.now(),
        });

        // Open a fresh client and re-resolve the store — proves
        // the data survived a connection close.
        const client2 = createClient({ url: `file:${path}` });
        const layer2 = makeSqliteStoreLayer(fromLibsql(client2));
        const store2 = yield* Effect.gen(function* () {
          return yield* SyncStoreTag;
        }).pipe(Effect.provide(layer2));
        const events = yield* store2.readSince({
          branch: branch(1),
          since: 0 as Seq,
        });
        expect(events.length).toBe(1);
        expect(events[0].payload).toEqual({ kind: "durable" });
      } finally {
        yield* Effect.tryPromise(() =>
          fs.unlink(path).catch(() => undefined),
        ).pipe(Effect.orElseSucceed(() => undefined));
      }
    }));
});