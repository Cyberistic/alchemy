import * as Effect from "effect/Effect";
import { type SyncStore, makeSqliteStore, type SyncStoreSql } from "./store.ts";

/**
 * Helpers for using meow/sync with **any** libSQL-compatible engine —
 * Cloudflare Durable Object SQLite, better-sqlite3 in Node, sql.js in the
 * browser, libSQL over HTTP, etc.
 *
 * Wrap your engine in the {@link SyncStoreSql} contract (the three
 * methods `exec`, `run`, `query`, `transaction`) and pass it to
 * {@link makeSqliteStore}. The schema is created lazily on first use.
 *
 * @example Durable Object SQLite
 * ```typescript
 * import { DurableObject } from "cloudflare:workers";
 * import { makeSqliteStoreLayer } from "meow/sync/sqlite";
 *
 * export class Sync_ extends DurableObject {
 *   constructor(state: DurableObjectState, env: any) {
 *     super(state, env);
 *     const sql: SyncStoreSql = {
 *       exec: (q, p) => state.storage.sql.exec(q, ...(p ?? [])),
 *       run: async (q, p) => {
 *         const cursor = state.storage.sql.exec(q, ...(p ?? []));
 *         return { lastInsertRowid: 0n, changes: 0 };
 *       },
 *       query: async (q, p) => state.storage.sql.exec(q, ...(p ?? []))
 *         .toArray() as any,
 *       transaction: async (fn) => fn(state.storage.sql as any),
 *     };
 *     // Provide the layer to handlers:
 *     // Layer.provide(makeSqliteStoreLayer(sql))
 *   }
 * }
 * ```
 *
 * @example libsql / Turso
 * ```typescript
 * import { createClient } from "@libsql/client";
 * import { fromLibsql, makeSqliteStoreLayer } from "meow/sync/sqlite";
 * const sql = fromLibsql(createClient({ url: ":memory:" }));
 * const layer = makeSqliteStoreLayer(sql);
 * ```
 */
export const fromSql = (
  sql: SyncStoreSql,
): Effect.Effect<SyncStore, never, never> => makeSqliteStore(sql);

export { fromLibsql } from "./libsql.ts";