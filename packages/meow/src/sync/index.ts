/**
 * `meow/sync` — git-style local-first sync.
 *
 * Wire-compatible with LiveStore's event-sourcing model:
 *
 * - The server is a Cloudflare Durable Object that owns a per-branch
 *   event log (SQLite or in-memory for tests).
 * - The client (browser or Node) connects over WebSocket for live
 *   events and over fetch for push/pull RPC.
 * - Events are linear per branch; `seq` is the HEAD cursor.
 * - No merges in v1 — single linear chain, last-write-wins. Conflict
 *   handling lives in your materialisers.
 *
 * ```typescript
 * // Server (in a DO):
 * import { Sync } from "meow/sync";
 * import { makeInMemoryStoreLayer } from "meow/sync/store";
 *
 * const program = Sync.make(/* store + hooks *\/);
 *
 * // Client:
 * import { SyncClient } from "meow/sync/client";
 * const client = new SyncClient({ url: "...", store: myLocalStore });
 * await client.push([...]);
 * for await (const event of client.live) { ... }
 * ```
 *
 * Subpaths:
 *
 * - `meow/sync` — server primitive (`Sync.make`, `Sync.host`) + types.
 * - `meow/sync/client` — `SyncClient` for the browser / Node.
 * - `meow/sync/store` — `SyncStore` interface + in-memory + sqlite.
 * - `meow/sync/sqlite` — `fromSql` / `fromLibsql` helpers.
 * - `meow/sync/sqlite-browser` — `fromSqlJs` + IndexedDB persistence.
 * - `meow/sync/pglite` — `makePgliteStore` for in-process Postgres.
 * - `meow/sync/tanstack-db` — `syncToCollection` adapter.
 * - `meow/sync/multi-tab` — leader election + cross-tab event forwarding.
 */
export * as Protocol from "./protocol.ts";
export * as Schema from "./schema.ts";
export * as Room from "./Room.ts";
export * as Store from "./store.ts";
export * as SqliteBrowser from "./sqlite-browser.ts";
export * as Pglite from "./pglite.ts";
export * as MultiTab from "./multi-tab.ts";

export const VERSION = "0.1.0" as const;