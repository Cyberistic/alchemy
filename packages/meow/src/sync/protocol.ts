import { Schema } from "effect";
import {
  Branch,
  Event,
  EventId,
  Seq,
  SessionId,
  StoreId,
} from "./schema.ts";

/**
 * Wire envelopes the browser / server worker sends over HTTP. All
 * envelopes are JSON-serialisable; the runtime decodes them with
 * `Schema.decodeUnknownEffect` at the boundary.
 */

// ---------------------------------------------------------------------------
// Client → Server (HTTP)
// ---------------------------------------------------------------------------

export const PushRequest = Schema.Struct({
  storeId: StoreId,
  branch: Branch,
  sessionId: SessionId,
  /**
   * Events to commit, in causal order. The server validates the chain
   * (`parent[i+1] === events[i].id`) and rejects the batch with
   * `SyncPushRejected` if it doesn't.
   */
  events: Schema.Array(Schema.Struct({
    id: EventId,
    parent: Schema.NullOr(EventId),
    payload: Schema.Unknown,
    createdAt: Schema.Number,
  })),
});
export type PushRequest = Schema.Schema.Type<typeof PushRequest>;

export const PushResponse = Schema.Struct({
  accepted: Schema.Array(EventId),
  /** Server's `HEAD` after applying the batch. */
  head: Seq,
});
export type PushResponse = Schema.Schema.Type<typeof PushResponse>;

export const PullRequest = Schema.Struct({
  storeId: StoreId,
  branch: Branch,
  /**
   * Pull every event with `seq > since`. Default: pull everything.
   */
  since: Schema.optionalKey(Seq),
  /** Hard cap on the number of events returned in one batch. */
  limit: Schema.optionalKey(Schema.Number),
});
export type PullRequest = Schema.Schema.Type<typeof PullRequest>;

export const PullResponse = Schema.Struct({
  events: Schema.Array(Event),
  /** Server's `HEAD` after the pull. Pass as `since` on the next pull. */
  head: Seq,
});
export type PullResponse = Schema.Schema.Type<typeof PullResponse>;

export const HeadResponse = Schema.Struct({
  head: Seq,
  /** Total events on the branch. */
  count: Schema.Number,
});
export type HeadResponse = Schema.Schema.Type<typeof HeadResponse>;

// ---------------------------------------------------------------------------
// Server → Client (WebSocket live stream)
// ---------------------------------------------------------------------------

export const LiveEnvelope = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("hello"),
    sessionId: SessionId,
    head: Seq,
  }),
  Schema.Struct({
    kind: Schema.Literal("event"),
    event: Event,
  }),
  Schema.Struct({
    kind: Schema.Literal("ping"),
  }),
  Schema.Struct({
    kind: Schema.Literal("pong"),
  }),
]);
export type LiveEnvelope = Schema.Schema.Type<typeof LiveEnvelope>;

export const ClientLiveEnvelope = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("subscribe"),
    branch: Branch,
  }),
  Schema.Struct({
    kind: Schema.Literal("ping"),
  }),
]);
export type ClientLiveEnvelope = Schema.Schema.Type<typeof ClientLiveEnvelope>;

// ---------------------------------------------------------------------------
// HTTP error envelopes (rejection reasons)
// ---------------------------------------------------------------------------

export const HttpErrorBody = Schema.Struct({
  error: Schema.String,
  detail: Schema.optionalKey(Schema.Unknown),
});
export type HttpErrorBody = Schema.Schema.Type<typeof HttpErrorBody>;