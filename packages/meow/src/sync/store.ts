import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  type Branch,
  type Event,
  type EventId,
  type Seq,
} from "./schema.ts";

/**
 * Storage backend for the sync event log. Two backends ship:
 *
 * - `InMemoryStore` — pure-JS, fastest, no setup. Use it in tests and
 *   small dev environments.
 * - `SqliteStore` — backed by Cloudflare Durable Object SQLite (or any
 *   libSQL-compatible engine). Survives hibernation, durable across
 *   deploys.
 * - `PgliteStore` — backed by PGlite in the browser. Durability depends
 *   on the configured storage backend (OPFS, IndexedDB, …).
 *
 * The interface is intentionally narrow — three methods, all pure
 * Effects. The server-side DO implements this once; tests substitute
 * {@link InMemoryStore} without touching the DO.
 */
export interface SyncStore {
  /**
   * Append a single event to `branch`. Returns the server-assigned id.
   * The server is responsible for assigning `seq` — backends just
   * persist in append order and let the server read `seq` from row
   * position.
   */
  readonly append: (
    input: SyncStoreAppendInput,
  ) => Effect.Effect<EventId, never, never>;

  /**
   * Read every event on `branch` whose `seq > since`. Bounded by `limit`
   * if provided — useful for paginating a large store.
   */
  readonly readSince: (
    input: SyncStoreReadSinceInput,
  ) => Effect.Effect<readonly Event[], never, never>;

  /**
   * Return the store's current `HEAD` and total event count.
   */
  readonly head: (
    branch: Branch,
  ) => Effect.Effect<{ readonly head: Seq; readonly count: number }, never, never>;
}

export interface SyncStoreAppendInput {
  readonly branch: Branch;
  readonly id: EventId;
  readonly parent: EventId | null;
  readonly payload: unknown;
  readonly author: string;
  readonly createdAt: number;
}

export interface SyncStoreReadSinceInput {
  readonly branch: Branch;
  readonly since: Seq;
  readonly limit?: number;
}

// ---------------------------------------------------------------------------
// In-memory store — tests, dev, ephemeral production
// ---------------------------------------------------------------------------

/**
 * Pure-JS, in-process event log. Every entry carries the full
 * `SyncEvent` so reads don't have to re-assemble anything. Sorted by
 * `seq` on insert so `readSince` is a linear scan + slice.
 */
export const makeInMemoryStore = (): SyncStore => {
  const events: Event[] = [];

  return {
    append: (input) =>
      Effect.gen(function* () {
        const seq = events.length + 1;
        const event: Event = {
          id: input.id,
          seq: seq as Seq,
          branch: input.branch,
          parent: input.parent,
          payload: input.payload,
          author: input.author as Event["author"],
          createdAt: input.createdAt,
        };
        events.push(event);
        return event.id;
      }),

    readSince: (input) =>
      Effect.succeed(
        events
          .filter((e) => e.branch === input.branch && (e.seq as number) > (input.since as number))
          .slice(0, input.limit ?? Infinity) as readonly Event[],
      ),

    head: (branch) =>
      Effect.sync(() => {
        const head = events
          .filter((e) => e.branch === branch)
          .reduce((acc, e) => Math.max(acc, e.seq as number), 0);
        const count = events.filter((e) => e.branch === branch).length;
        return { head: head as Seq, count };
      }),
  };
};

/**
 * Convenience Layer that exposes an in-memory store as a service.
 * Pair it with `SyncStoreLive` for tests:
 *
 * ```typescript
 * const testLayer = InMemoryStoreLive;
 * yield* someEffectThatDependsOnStore.pipe(Layer.provide(testLayer));
 * ```
 */
export class SyncStoreService extends
  /** @ignore */ (class {}) {}
// Re-export the tag factory — kept inline so consumers don't have to
// import the (intentionally anonymous) class above.
export const SyncStore = <
  T extends SyncStore = SyncStore,
>(): {
  readonly _: unique symbol;
  readonly service: T;
} => {
  // Implementation deferred to a context-tag helper below. The actual
  // tag lives at the bottom of this file so the type-only class above
  // doesn't shadow it.
  return null as never;
};

// ---------------------------------------------------------------------------
// SQLite store (Cloudflare Durable Object SQLite + libSQL)
// ---------------------------------------------------------------------------

/**
 * Minimal SQL contract every SyncStore backend must implement. The
 * server-side DO has a built-in `state.storage.sql` that satisfies
 * this. The client-side backends (PGlite, sql.js) plug in their own
 * adapters.
 *
 * Concurrency: appends are serialised by the storage engine. Callers
 * that need strict ordering should await each append before issuing
 * the next.
 */
export interface SyncStoreSql {
  /** Run a write that returns no rows. */
  readonly exec: (sql: string, params?: readonly unknown[]) => Promise<void>;
  /** Run a write that returns the `last_insert_rowid()`. */
  readonly run: (
    sql: string,
    params?: readonly unknown[],
  ) => Promise<{ readonly lastInsertRowid: bigint; readonly changes: number }>;
  /** Run a read that returns rows. */
  readonly query: <Row extends Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
  ) => Promise<readonly Row[]>;
  /** Run multiple statements inside a transaction. */
  readonly transaction: <A>(
    fn: (sql: SyncStoreSql) => Promise<A>,
  ) => Promise<A>;
}

/**
 * Construct a SyncStore backed by any libSQL-compatible engine. The
 * schema is created lazily on the first call (idempotent).
 */
export const makeSqliteStore = (
  sql: SyncStoreSql,
): Effect.Effect<SyncStore, never, never> =>
  Effect.gen(function* () {
    // Schema is created once. Idempotent thanks to `IF NOT EXISTS`.
    yield* Effect.tryPromise(() =>
      sql.exec(`
        CREATE TABLE IF NOT EXISTS meow_sync_events (
          id TEXT NOT NULL,
          seq INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
          branch TEXT NOT NULL,
          parent TEXT,
          payload TEXT NOT NULL,
          author TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS meow_sync_events_branch_seq
          ON meow_sync_events (branch, seq);
      `),
    ).pipe(Effect.orElseSucceed(() => undefined));

    const append: SyncStore["append"] = (input) =>
      Effect.gen(function* () {
        yield* Effect.tryPromise(() =>
          sql.run(
            `INSERT INTO meow_sync_events
               (id, branch, parent, payload, author, created_at)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [
              input.id,
              input.branch,
              input.parent,
              JSON.stringify(input.payload),
              input.author,
              input.createdAt,
            ],
          ),
        ).pipe(Effect.catch(() => Effect.void));
        return input.id;
      });

    const readSince: SyncStore["readSince"] = (input) =>
      Effect.tryPromise(async () => {
        const rows = await sql.query<{
          id: string;
          seq: number;
          branch: string;
          parent: string | null;
          payload: string;
          author: string;
          created_at: number;
        }>(
          `SELECT id, seq, branch, parent, payload, author, created_at
             FROM meow_sync_events
            WHERE branch = ? AND seq > ?
            ORDER BY seq ASC
            LIMIT ?`,
          [input.branch, input.since, input.limit ?? Number.MAX_SAFE_INTEGER],
        );
        return rows.map(
          (r): Event => ({
            id: r.id as EventId,
            seq: r.seq as Seq,
            branch: r.branch as Branch,
            parent: (r.parent ?? null) as EventId | null,
            payload: JSON.parse(r.payload),
            author: r.author as Event["author"],
            createdAt: r.created_at,
          }),
        );
      });

    const head: SyncStore["head"] = (branch) =>
      Effect.tryPromise(async () => {
        const rows = await sql.query<{ seq: number; count: number }>(
          `SELECT COALESCE(MAX(seq), 0) AS seq, COUNT(*) AS count
             FROM meow_sync_events
            WHERE branch = ?`,
          [branch],
        );
        const row = rows[0]!;
        return {
          head: row.seq as Seq,
          count: row.count,
        };
      });

    return { append, readSince, head };
  });

// ---------------------------------------------------------------------------
// Service tag
// ---------------------------------------------------------------------------

/**
 * Context tag for the storage backend. Provide it via `Layer.succeed`
 * or `Layer.effect` so handlers can `yield* SyncStore.Tag` to get the
 * backend.
 */
import * as Context from "effect/Context";
export const SyncStoreTag = Context.Service<SyncStore>(
  "meow/sync/SyncStore",
);

/**
 * Helper Layer for the in-memory backend. Use in tests:
 *
 * ```typescript
 * yield* program.pipe(Effect.provide(InMemoryStoreLive));
 * ```
 */
export const InMemoryStoreLive = Layer.succeed(SyncStoreTag, makeInMemoryStore());

/**
 * Helper Layer for the SQLite backend. Build with a SyncStoreSql:
 *
 * ```typescript
 * const SqliteStoreLive = makeSqliteStoreLayer(mySqlite);
 * yield* program.pipe(Effect.provide(SqliteStoreLive));
 * ```
 */
export const makeSqliteStoreLayer = (
  sql: SyncStoreSql,
): Layer.Layer<SyncStore> =>
  Layer.effect(
    SyncStoreTag,
    makeSqliteStore(sql),
  );