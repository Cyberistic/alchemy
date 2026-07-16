import type * as cf from "@cloudflare/workers-types";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/**
 * Topic name — any non-empty string. We don't brand it because users
 * typically compose topics from request data (URL params, env vars,
 * entity ids) and a brand would force them to cast at every callsite.
 */
export type Topic = string;

/**
 * Server-assigned event id (UUID).
 */
export type EventId = string & { readonly __brand: "BroadcastEventId" };

export const EventId = (raw: string): EventId => raw as EventId;

/**
 * Default shard count. The user can override per `Broadcast` instance via
 * {@link BroadcastOptions.shards}. Cloudflare's default DO namespace
 * limit is 100k instances per account; 16 shards × N logical namespaces
 * leaves ample headroom.
 */
export const DEFAULT_SHARDS = 4 as const;

/**
 * Server-side envelope for a single published event. The shape is
 * stable across versions and is what subscribers receive over the
 * live WebSocket.
 */
export interface BroadcastEnvelope<T = unknown> {
  readonly kind: "event";
  readonly topic: Topic;
  readonly id: EventId;
  readonly ts: number;
  readonly event: T;
}

/**
 * Wire format for `POST /publish`. The body is the user event
 * verbatim — there's no per-event envelope, so the consumer code
 * decides the shape (`yield* Topic.publish("chat:room-1", { kind,
 * from, text })`).
 */
export const PublishRequest = Schema.Struct({
  topic: Schema.String,
  event: Schema.Unknown,
});
export type PublishRequest = Schema.Schema.Type<typeof PublishRequest>;

export const PublishResponse = Schema.Struct({
  accepted: Schema.Number,
  id: Schema.String,
});
export type PublishResponse = Schema.Schema.Type<typeof PublishResponse>;

/**
 * WebSocket subscribe envelope. Sent by the client on connect (or on
 * a `subscribe` message) to register interest in one or more topics.
 */
export const ClientSubscribeMessage = Schema.Struct({
  kind: Schema.Literal("subscribe"),
  topics: Schema.Array(Schema.String),
});
export type ClientSubscribeMessage = Schema.Schema.Type<typeof ClientSubscribeMessage>;

export const ClientUnsubscribeMessage = Schema.Struct({
  kind: Schema.Literal("unsubscribe"),
  topics: Schema.Array(Schema.String),
});
export type ClientUnsubscribeMessage = Schema.Schema.Type<typeof ClientUnsubscribeMessage>;

export const ClientPingMessage = Schema.Struct({
  kind: Schema.Literal("ping"),
});
export type ClientPingMessage = Schema.Schema.Type<typeof ClientPingMessage>;

export type ClientMessage =
  | ClientSubscribeMessage
  | ClientUnsubscribeMessage
  | ClientPingMessage;

/**
 * Server-to-client envelope on the live WebSocket. A `hello` is sent
 * once on connect, an `event` for each published event, a `pong` in
 * response to a `ping`, and an `error` for malformed input.
 */
export type ServerMessage<T = unknown> =
  | { readonly kind: "hello"; readonly sessionId: string }
  | BroadcastEnvelope<T>
  | { readonly kind: "pong" }
  | { readonly kind: "error"; readonly error: string };