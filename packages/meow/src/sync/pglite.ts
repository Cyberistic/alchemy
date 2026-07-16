import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  makeSqliteStore as makeStoreFromSql,
  type SyncStore,
  type SyncStoreSql,
  SyncStoreTag,
} from "./store.ts";

/**
 * Minimum PGlite interface meow/sync uses. We define it locally so we
 * don't drag `@electric-sql/pglite` into the type graph of every
 * consumer — any object with these methods works.
 *
 * `loadDataDir` lets PGlite persist to OPFS in the browser; pass
 * `"idb://my-app"` for IndexedDB-backed persistence.
 */
export interface PGliteLike {
  query: <Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ) => Promise<{ rows: readonly Row[] }>;
  exec: (sql: string) => Promise<void>;
  transaction: <A>(fn: (tx: PGliteLike) => Promise<A>) => Promise<A>;
  /**
   * PGlite's `dataDir` option is set at construction; this method
   * exposes it for callers who want to inspect the persistence layer.
   */
  readonly dataDir?: string;
}

const toSqlStore = (db: PGliteLike): SyncStoreSql => ({
  exec: async (sql) => {
    await db.exec(sql);
  },
  run: async (sql, params) => {
    // PGlite doesn't expose lastInsertRowid directly — append a
    // RETURNING clause so we can capture the inserted id.
    const finalSql = sql.toLowerCase().includes("returning")
      ? sql
      : `${sql} RETURNING 1`;
    const result = await db.query<{ id: number }>(
      finalSql,
      params ? [...params] : undefined,
    );
    const lastInsertRowid = BigInt(result.rows[0]?.id ?? 0);
    return { lastInsertRowid, changes: result.rows.length };
  },
  query: <Row extends Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
  ): Promise<readonly Row[]> =>
    db.query<Row>(sql, params ? [...params] : undefined).then((r) => r.rows),
  transaction: async (fn) => {
    return db.transaction(async (tx) => {
      return fn(toSqlStore(tx));
    });
  },
});

/**
 * Build a {@link SyncStore} backed by an in-process PGlite (WASM
 * Postgres). On the server this is the same shape as the SQLite
 * backend; in the browser, pass the result of `new PGlite()` with
 * `dataDir: "idb://meow-sync"` for IndexedDB persistence (or
 * `dataDir: "memory://"` for ephemeral).
 *
 * ```typescript
 * import { PGlite } from "@electric-sql/pglite";
 * import { makePgliteStore } from "meow/sync/pglite";
 *
 * // Browser (persists to IndexedDB):
 * const db = new PGlite("idb://meow-sync");
 * const store = await Effect.runPromise(makePgliteStore(db));
 *
 * // Node / in-memory:
 * const db = new PGlite(); // memory://
 * const store = await Effect.runPromise(makePgliteStore(db));
 * ```
 */
export const makePgliteStore = (
  db: PGliteLike,
): Effect.Effect<SyncStore, never, never> =>
  Effect.gen(function* () {
    yield* Effect.tryPromise(() =>
      db.exec(`
        CREATE TABLE IF NOT EXISTS meow_sync_events (
          id TEXT NOT NULL,
          seq BIGSERIAL PRIMARY KEY,
          branch TEXT NOT NULL,
          parent TEXT,
          payload JSONB NOT NULL,
          author TEXT NOT NULL,
          created_at BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS meow_sync_events_branch_seq
          ON meow_sync_events (branch, seq);
      `),
    ).pipe(Effect.orElseSucceed(() => undefined));

    const sqlStore = toSqlStore(db);
    return yield* makeStoreFromSql(sqlStore);
  });

/**
 * Layer form of {@link makePgliteStore}. Provide it to any handler
 * that needs `SyncStore` in its environment:
 *
 * ```typescript
 * const layer = makePgliteStoreLayer(db);
 * yield* someEffect.pipe(Effect.provide(layer));
 * ```
 */
export const makePgliteStoreLayer = (
  db: PGliteLike,
): Layer.Layer<SyncStore> =>
  Layer.effect(SyncStoreTag, makePgliteStore(db));