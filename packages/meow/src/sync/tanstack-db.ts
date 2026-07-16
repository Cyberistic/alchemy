import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import type { MultiTabCoordinator } from "./multi-tab.ts";
import type { SyncClient } from "./client.ts";
import type { Event } from "./schema.ts";
import type { PGliteLike } from "./pglite.ts";
import type { SqlJsLike } from "./sqlite-browser.ts";

/**
 * Adapter from meow/sync to a TanStack DB `Collection`. Push events
 * arrive on the client's live stream and translate into `Collection`
 * inserts / updates / deletes via the user-supplied mapper.
 *
 * This module is intentionally tiny — TanStack DB's collection API
 * takes whatever shape your app uses, so the mapper is yours. The
 * adapter just wires up the reactive subscription.
 *
 * ```typescript
 * import { createCollection } from "@tanstack/db";
 * import { syncToCollection } from "meow/sync/tanstack-db";
 *
 * const todos = createCollection({ ... });
 * const dispose = syncToCollection({
 *   client,
 *   collection: todos,
 *   insert: (e) => e.payload.kind === "TodoCreated" && {
 *     id: e.payload.id,
 *     title: e.payload.title,
 *   },
 *   update: (e) => e.payload.kind === "TodoUpdated" && {
 *     id: e.payload.id,
 *     patch: e.payload.patch,
 *   },
 *   delete: (e) => e.payload.kind === "TodoDeleted" && e.payload.id,
 * });
 *
 * // Later:
 * dispose();
 * ```
 */
export interface CollectionLike<TInsert, TUpdate, TKey = string> {
  readonly insert: (data: TInsert) => void;
  readonly update: (key: TKey, data: TUpdate) => void;
  readonly delete: (key: TKey) => void;
}

export interface SyncToCollectionOptions<
  TCollection extends CollectionLike<unknown, unknown, string>,
> {
  /**
   * Source of remote events. Provide EITHER a {@link SyncClient}
   * (one tab, one WebSocket) OR a {@link MultiTabCoordinator}
   * (one leader per channel, many follower tabs).
   */
  readonly source: SyncClient | MultiTabCoordinator;
  readonly collection: TCollection;
  /**
   * Decode a sync event into a Collection insert. Return `null` to
   * skip the event.
   */
  readonly insert: (event: Event) => Parameters<TCollection["insert"]>[0] | null;
  /**
   * Decode a sync event into a Collection update. Return `null` to
   * skip.
   */
  readonly update?: (event: Event) =>
    | readonly [string, Parameters<TCollection["update"]>[1]]
    | null;
  /**
   * Decode a sync event into a Collection delete key. Return `null`
   * to skip.
   */
  readonly delete?: (event: Event) => string | null;
}

const liveStream = (
  source: SyncClient | MultiTabCoordinator,
): Stream.Stream<Event, never, never> => source.live;

/**
 * Subscribe a TanStack DB collection to a sync source. Returns a
 * `dispose` function that interrupts the subscription.
 */
export const syncToCollection = <
  TCollection extends CollectionLike<unknown, unknown, string>,
>(
  options: SyncToCollectionOptions<TCollection>,
): (() => void) => {
  const subscription = liveStream(options.source);
  const fiber = Effect.runFork(
    Stream.runForEach(subscription, (event) =>
      Effect.sync(() => {
        const insert = options.insert(event);
        if (insert !== null) {
          (options.collection.insert as (d: unknown) => void)(insert);
          return;
        }
        if (options.update) {
          const update = options.update(event);
          if (update !== null) {
            (options.collection.update as (k: string, d: unknown) => void)(
              update[0],
              update[1],
            );
            return;
          }
        }
        if (options.delete) {
          const del = options.delete(event);
          if (del !== null) {
            (options.collection.delete as (k: string) => void)(del);
          }
        }
      }),
    ),
  );
  return () => {
    Effect.runFork(Fiber.interrupt(fiber));
  };
};

/**
 * Re-export of `source.live` as an Effect Stream. Use this when you
 * want to drive your own materialiser without going through TanStack
 * DB.
 */
export const liveEvents = (
  source: SyncClient | MultiTabCoordinator,
): Stream.Stream<Event, never, never> => liveStream(source);

// ---------------------------------------------------------------------------
// Convenience: backend wiring for the common "browser app" shape
// ---------------------------------------------------------------------------

/**
 * Pre-built helpers for the two common browser backends. Pick the
 * one that matches your stack:
 *
 * - **`pglite`** — WASM Postgres with OPFS persistence. Best when
 *   you want full SQL, JSONB, or Postgres-specific features.
 * - **`sql.js`** — WASM SQLite. Lightest weight. Persistence via
 *   {@link import("./sqlite-browser.ts").withIndexedDbPersistence}.
 *
 * Both delegate to `syncToCollection` with a typed wrapper so the
 * collection mapper signature is enforced.
 */
export interface CollectionWiringOptions<
  TCollection extends CollectionLike<unknown, unknown, string>,
> extends SyncToCollectionOptions<TCollection> {
  /**
   * Override the live stream — useful in tests where you want to
   * skip the WebSocket entirely and feed events straight in.
   */
  readonly events?: Stream.Stream<Event, never, never>;
}

/**
 * Wire a collection to a PGlite-backed sync. The source can be any
 * SyncClient or MultiTabCoordinator; we just hand the live stream
 * to {@link syncToCollection}.
 *
 * ```typescript
 * import { PGlite } from "@electric-sql/pglite";
 * import { wirePgliteCollection } from "meow/sync/tanstack-db";
 *
 * const db = new PGlite("idb://meow-sync");
 * const store = await Effect.runPromise(makePgliteStore(db));
 * const client = new SyncClient({ url, store });
 * const dispose = wirePgliteCollection({
 *   source: client,
 *   collection: todos,
 *   insert: (e) => e.payload.kind === "TodoCreated" ? { id: e.payload.id, ... } : null,
 * });
 * ```
 */
export const wirePgliteCollection = <
  TCollection extends CollectionLike<unknown, unknown, string>,
>(
  options: CollectionWiringOptions<TCollection> & {
    readonly db?: PGliteLike;
  },
): (() => void) => {
  void options.db; // reserved for future direct-store hooks (e.g. materializers)
  return syncToCollection(options);
};

/**
 * Wire a collection to a sql.js-backed sync. Same shape as
 * {@link wirePgliteCollection}.
 */
export const wireSqliteCollection = <
  TCollection extends CollectionLike<unknown, unknown, string>,
>(
  options: CollectionWiringOptions<TCollection> & {
    readonly db?: SqlJsLike;
  },
): (() => void) => {
  void options.db;
  return syncToCollection(options);
};