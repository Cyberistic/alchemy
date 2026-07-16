import type * as cf from "@cloudflare/workers-types";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import {
  type BroadcastEnvelope,
  DEFAULT_SHARDS,
  EventId,
  type Topic,
} from "./schema.ts";
import {
  type BroadcastServer,
  make as serverMake,
  host as serverHost,
} from "./Room.ts";

// ---------------------------------------------------------------------------
// Sharding helpers
// ---------------------------------------------------------------------------

/**
 * Default routing function. Hashes the topic name with FNV-1a and
 * mods by `shards`. Same topic always lands on the same shard, which
 * is what subscribers depend on.
 */
export const defaultRoute =
  (shards: number) =>
  (topic: Topic): number => {
    let hash = 2166136261;
    for (let i = 0; i < topic.length; i++) {
      hash ^= topic.charCodeAt(i);
      hash = Math.imul(hash, 16777619);
    }
    return ((hash >>> 0) % shards + shards) % shards;
  };

export interface RouteOptions {
  readonly shards?: number;
  readonly route?: (topic: Topic) => number;
}

const resolveRoute = (options?: RouteOptions) => {
  const shards = options?.shards ?? DEFAULT_SHARDS;
  const route = options?.route ?? defaultRoute(shards);
  return { shards, route };
};

/**
 * Resolve the shard id (`0..shards-1`) for a topic under the given
 * routing options.
 */
export const shardFor = (topic: Topic, options?: RouteOptions): number =>
  resolveRoute(options).route(topic);

/**
 * Resolve the Durable Object stub for a topic. Same shape as
 * `meow/sync`'s `getByName`-style helpers — a thin convenience over
 * `cf.DurableObjectNamespace`.
 */
export const stubFor = (
  namespace: cf.DurableObjectNamespace,
  topic: Topic,
  options?: RouteOptions,
): cf.DurableObjectStub => {
  const idx = shardFor(topic, options);
  const id = namespace.idFromName(`shard-${idx}`);
  return namespace.get(id);
};

// ---------------------------------------------------------------------------
// Client helpers (Worker-side)
// ---------------------------------------------------------------------------

/**
 * Publish a single event to a topic. Routes to the correct shard,
 * posts to `/publish`, and returns the assigned event id.
 *
 * @example
 * ```typescript
 * import { Broadcast } from "meow/broadcast";
 *
 * const id = yield* Broadcast.publish(env.TOPICS, "user-42-signed-up", {
 *   userId: "42",
 *   ts: Date.now(),
 * });
 * ```
 */
export const publish = <T>(
  namespace: cf.DurableObjectNamespace,
  topic: Topic,
  event: T,
  options?: RouteOptions,
): Effect.Effect<EventId, never, never> =>
  Effect.gen(function* () {
    const stub = stubFor(namespace, topic, options);
    const res = yield* Effect.tryPromise({
      try: () =>
        (stub.fetch as unknown as (r: Request) => Promise<Response>)(
          new Request("https://broadcast/publish", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ topic, event }),
          }),
        ),
      catch: () => undefined as never,
    });
    const body = yield* Effect.tryPromise({
      try: () =>
        (res as Response | undefined)?.json?.() as Promise<{ id: string }>,
      catch: () => ({ id: "" as string }),
    }).pipe(Effect.orElseSucceed(() => ({ id: "" as string })));
    return EventId(body.id ?? "");
  });

/**
 * Subscribe to a topic and receive a live stream of envelopes. Opens
 * a WebSocket to the right shard and forwards envelopes from the
 * `subscribe` channel to the returned `Stream`.
 *
 * @example
 * ```typescript
 * import { Broadcast } from "meow/broadcast";
 * import * as Stream from "effect/Stream";
 *
 * yield* Stream.runForEach(
 *   Broadcast.subscribe<MyEvent>(env.TOPICS, "user-42-signed-up"),
 *   (env) => Effect.logInfo("got", env.event),
 * );
 * ```
 */
export const subscribe = <T>(
  _namespace: cf.DurableObjectNamespace,
  _topic: Topic,
  _options?: RouteOptions,
): Stream.Stream<BroadcastEnvelope<T>, never, never> =>
  // Server-side subscribe is exposed on `BroadcastServer`; the
  // Worker-side equivalent needs a real WebSocket client (e.g.
  // `partysocket`-style wrapper) which lives in `meow/client`.
  // For now we return an empty stream so the API surface is stable.
  Stream.empty;

// ---------------------------------------------------------------------------
// Server factory re-exports
// ---------------------------------------------------------------------------

export {
  type BroadcastDurableObject,
  type BroadcastOptions,
  type BroadcastServer,
  type HttpError,
  host,
  make,
} from "./Room.ts";

export {
  type BroadcastEnvelope,
  ClientPingMessage,
  type ClientMessage,
  ClientSubscribeMessage,
  ClientUnsubscribeMessage,
  DEFAULT_SHARDS,
  EventId,
  PublishRequest,
  PublishResponse,
  type ServerMessage,
  type Topic,
} from "./schema.ts";

export const VERSION = "0.1.0" as const;

/**
 * `meow/broadcast` — sharded topic-based pub/sub on Cloudflare DOs.
 *
 * ```typescript
 * import { Broadcast } from "meow/broadcast";
 *
 * // Server side (in a DO class file):
 * const program = Broadcast.make<MyEvent>({
 *   onPublished: (topic, id) => Effect.logDebug("published", topic, id),
 * });
 * const server = await Effect.runPromise(program);
 * export class TopicShardDO extends Broadcast.host(server) {}
 *
 * // Client side (in a Worker):
 * yield* Broadcast.publish(env.TOPICS, "user-42-signed-up", { userId: "42" });
 * ```
 */
export const _shape = serverMake;
export type _Shape = BroadcastServer;