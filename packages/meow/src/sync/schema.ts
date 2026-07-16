import { Schema } from "effect";

/**
 * Git-style identifier for a single event (the equivalent of a git
 * commit hash). Server-assigned, monotonically increasing per store,
 * opaque to clients.
 */
export const EventId = Schema.String.pipe(Schema.brand("meow/EventId"));
export type EventId = Schema.Schema.Type<typeof EventId>;

/**
 * Sequence number — the store's monotonically increasing pointer. Each
 * accepted event gets the next seq (1, 2, 3, …). `pull?since=N` returns
 * every event with `seq > N`. Equivalent to a git `HEAD`.
 *
 * Branded for type safety but unconstrained at the schema layer (the
 * store layer guarantees monotonicity at write time).
 */
export const Seq = Schema.Number.pipe(Schema.brand("meow/Seq"));
export type Seq = Schema.Schema.Type<typeof Seq>;

/**
 * Wire identifier for the store (the durable object's `getByName` arg).
 * In LiveStore / Firestore parlance this is called a "store id".
 */
export const StoreId = Schema.String.pipe(Schema.brand("meow/StoreId"));
export type StoreId = Schema.Schema.Type<typeof StoreId>;

/**
 * Branch identifier — equivalent to a git branch name. Each push /
 * pull operates on a single branch. Clients typically have one
 * (`main`); advanced users can spin up throwaway branches for
 * speculative work, exactly like git.
 */
export const Branch = Schema.String.pipe(Schema.brand("meow/Branch"));
export type Branch = Schema.Schema.Type<typeof Branch>;

/**
 * Session id — opaque per-connection tag the server uses to attribute
 * writes ("alice's tab pushed these events") and to GC idle sessions
 * on a timer. Clients don't need to interpret it.
 */
export const SessionId = Schema.String.pipe(Schema.brand("meow/SessionId"));
export type SessionId = Schema.Schema.Type<typeof SessionId>;

/**
 * Arbitrary user-defined payload. Replace with a `Schema.TaggedUnion`
 * in the concrete integration (e.g. `TodoCreated | TodoUpdated | TodoDeleted`).
 */
export const Payload = Schema.Unknown;
export type Payload = Schema.Schema.Type<typeof Payload>;

/**
 * A single committed event on the store. Mirrors a git commit:
 *
 * - `id` — opaque server-assigned hash.
 * - `seq` — store-monotonic sequence number (`HEAD` at the moment of
 *   the event).
 * - `branch` — branch name.
 * - `parent` — `id` of the previous event on this branch (the
 *   "parent commit"). Forms a linked list per branch.
 * - `payload` — the user's data.
 * - `author` — session id that wrote the event.
 * - `createdAt` — ms-since-epoch.
 */
export const Event = Schema.Struct({
  id: EventId,
  seq: Seq,
  branch: Branch,
  parent: Schema.NullOr(EventId),
  payload: Payload,
  author: SessionId,
  createdAt: Schema.Number,
});
export type Event = Schema.Schema.Type<typeof Event>;

/**
 * Typed errors emitted by the sync server and client.
 */
export class SyncPushRejected extends Schema.TaggedErrorClass<SyncPushRejected>()(
  "meow/SyncPushRejected",
  {
    reason: Schema.String,
    rejectedIds: Schema.Array(EventId),
  },
) {}

export class SyncPullFailed extends Schema.TaggedErrorClass<SyncPullFailed>()(
  "meow/SyncPullFailed",
  {
    reason: Schema.String,
  },
) {}

export class SyncConflict extends Schema.TaggedErrorClass<SyncConflict>()(
  "meow/SyncConflict",
  {
    reason: Schema.String,
    ours: Schema.Array(EventId),
    theirs: Schema.Array(EventId),
  },
) {}