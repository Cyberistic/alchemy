import type { Client, InValue } from "@libsql/client";
import type { SyncStoreSql } from "./store.ts";

/**
 * Wrap a `@libsql/client` {@link Client} as a {@link SyncStoreSql}.
 * Works against local file SQLite, in-memory SQLite (`:memory:`),
 * embedded replicas, and remote libSQL/Turso databases — the same
 * `Client` API everywhere.
 *
 * @example
 * ```typescript
 * import { createClient } from "@libsql/client";
 * import { fromLibsql } from "meow/sync/libsql";
 * import { makeSqliteStoreLayer } from "meow/sync/store";
 *
 * const sql = fromLibsql(createClient({ url: ":memory:" }));
 * const layer = makeSqliteStoreLayer(sql);
 * ```
 */
export const fromLibsql = (client: Client): SyncStoreSql => ({
  exec: async (q, params) => {
    await client.execute({ sql: q, args: (params ?? []) as InValue[] });
  },
  run: async (q, params) => {
    const result = await client.execute({
      sql: q,
      args: (params ?? []) as InValue[],
    });
    return {
      lastInsertRowid:
        typeof result.lastInsertRowid === "bigint"
          ? result.lastInsertRowid
          : BigInt(result.lastInsertRowid ?? 0),
      changes: Number(result.rowsAffected ?? 0),
    };
  },
  query: async <Row extends Record<string, unknown>>(
    q: string,
    params?: readonly unknown[],
  ): Promise<readonly Row[]> => {
    const result = await client.execute({
      sql: q,
      args: (params ?? []) as InValue[],
    });
    return result.rows as unknown as readonly Row[];
  },
  transaction: async <A>(
    fn: (sql: SyncStoreSql) => Promise<A>,
  ): Promise<A> => {
    // libsql exposes `client.transaction()` on the `Client` interface.
    // The closure receives a transactional client — same shape.
    const tx = (client as unknown as {
      transaction: (
        fn: (tx: Client) => Promise<A>,
      ) => Promise<A>;
    }).transaction(async (txClient: Client) => {
      const txSql: SyncStoreSql = fromLibsql(txClient);
      return await fn(txSql);
    });
    return tx;
  },
});