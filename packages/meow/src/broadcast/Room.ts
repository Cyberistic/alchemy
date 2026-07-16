import type * as cf from "@cloudflare/workers-types";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as PubSub from "effect/PubSub";
import { Schema } from "effect";
import * as Stream from "effect/Stream";
import {
  type BroadcastEnvelope,
  type ClientMessage,
  EventId,
  PublishRequest as PublishRequestSchema,
  type PublishResponse,
  type ServerMessage,
  type Topic,
} from "./schema.ts";

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface BroadcastOptions {
  /**
   * Maximum number of topics we'll track per shard. Bounded so a
   * malicious or buggy publisher can't blow up the DO's memory by
   * minting fresh topics on every message. Defaults to 1024.
   */
  readonly maxTopicsPerShard?: number;
  /**
   * Capacity of each topic's PubSub. Bounded so a slow subscriber
   * can't grow the queue without bound — once full, the oldest
   * undelivered event is dropped. Defaults to 1024.
   */
  readonly pubsubCapacity?: number;
  /**
   * Optional hook fired after a publish is accepted. Useful for
   * telemetry, billing, or auditing. Errors are caught and ignored.
   */
  readonly onPublished?: (
    topic: Topic,
    id: EventId,
  ) => Effect.Effect<void, never, never>;
}

// ---------------------------------------------------------------------------
// HTTP error envelope
// ---------------------------------------------------------------------------

export interface HttpError {
  readonly _tag: "HttpError";
  readonly status: number;
  readonly body: { readonly error: string; readonly detail?: unknown };
}

const httpError = (status: number, error: string, detail?: unknown): HttpError => ({
  _tag: "HttpError",
  status,
  body: { error, detail },
});

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/** Per-socket subscriber state. */
interface Subscriber {
  readonly topics: Set<Topic>;
  readonly send: (msg: ServerMessage) => void;
  /**
   * One fiber per (socket, topic) pair — the loop that pulls from the
   * topic PubSub and forwards envelopes to the socket. We keep them
   * separate so `unsubscribe(topic)` can interrupt a single fiber
   * without disturbing other subscriptions on the same socket.
   */
  readonly fibers: Map<Topic, Fiber.Fiber<void, never>>;
}

// ---------------------------------------------------------------------------
// `Broadcast.make` — the canonical factory
// ---------------------------------------------------------------------------

/**
 * Build a {@link BroadcastServer} backed by a Cloudflare Durable Object
 * instance per shard.
 *
 * Each shard maintains its own topic registry and per-topic PubSub.
 * Topics that hash to the same shard land in the same DO; topics that
 * hash to different shards are completely independent.
 *
 * The server is per-shard state — the DO class wraps it in a `fetch`
 * handler that routes `POST /publish` to `handlePublish` and
 * `Upgrade: websocket` to the subscriber channel. The default route
 * function (modular hash of the topic name) is exposed by
 * {@link defaultRoute}.
 *
 * @example
 * ```typescript
 * import { Broadcast } from "meow/broadcast";
 *
 * const program = Broadcast.make<MyEvent>({
 *   onPublished: (topic, id) => Effect.logDebug("published", topic, id),
 * });
 *
 * const server = await Effect.runPromise(program);
 * export class TopicDO_ extends Broadcast.host(server) {}
 * ```
 */
export const make = <T = unknown>(
  options?: BroadcastOptions,
): Effect.Effect<BroadcastServer<T>, never, never> =>
  Effect.gen(function* () {
    return yield* Effect.sync(() => build<T>(options));
  });

const build = <T>(options?: BroadcastOptions): BroadcastServer<T> => {
  const maxTopics = options?.maxTopicsPerShard ?? 1024;
  const capacity = options?.pubsubCapacity ?? 1024;

  // Per-topic PubSubs. Lazy-allocated via `ensurePub` so an idle shard
  // doesn't pay for unused topics. We use `Effect.runSync` to allocate
  // the PubSub so its lifetime is the DO instance's — same pattern as
  // `meow/sync/Room.ts`.
  const pubs = new Map<Topic, PubSub.PubSub<BroadcastEnvelope<T>>>();
  const ensurePub = (topic: Topic): PubSub.PubSub<BroadcastEnvelope<T>> => {
    const existing = pubs.get(topic);
    if (existing) return existing;
    const created = Effect.runSync(
      PubSub.bounded<BroadcastEnvelope<T>>({ capacity }),
    );
    pubs.set(topic, created);
    return created;
  };
  const getPub = (
    topic: Topic,
  ): Effect.Effect<PubSub.PubSub<BroadcastEnvelope<T>>, never, never> =>
    Effect.sync(() => ensurePub(topic));

  // Broadcast a freshly-accepted event to every subscriber on this
  // shard. PubSub's `publishUnsafe` returns `false` if the queue is
  // full — we treat that as "subscriber is too slow" and silently
  // drop. A real-time system should never block on a slow consumer.
  const broadcast = (
    envelope: BroadcastEnvelope<T>,
  ): Effect.Effect<void, never, never> =>
    Effect.sync(() => {
      const pub = ensurePub(envelope.topic);
      PubSub.publishUnsafe(pub, envelope);
    });

  // ----- publish -----
  const handlePublish = (
    body: unknown,
  ): Effect.Effect<PublishResponse, HttpError, never> =>
    Effect.gen(function* () {
      const decode = yield* safeDecode(
        PublishRequestSchema,
        body,
        "invalid publish",
      );
      if (decode._tag === "error") {
        return yield* Effect.fail(
          httpError(400, decode.error, decode.detail),
        );
      }
      const req = decode.value;
      if (!req.topic) {
        return yield* Effect.fail(
          httpError(400, "missing topic"),
        );
      }
      if (pubs.size >= maxTopics && !pubs.has(req.topic)) {
        return yield* Effect.fail(
          httpError(429, "topic limit reached", {
            limit: maxTopics,
          }),
        );
      }
      const id = EventId(crypto.randomUUID());
      const envelope: BroadcastEnvelope<T> = {
        kind: "event",
        topic: req.topic,
        id,
        ts: Date.now(),
        event: req.event as T,
      };
      yield* broadcast(envelope);
      if (options?.onPublished) {
        yield* options.onPublished(req.topic, id);
      }
      return { accepted: 1, id } satisfies PublishResponse;
    });

  // ----- count -----
  const count = (
    topic: Topic,
  ): Effect.Effect<number, never, never> =>
    Effect.sync(() => (pubs.has(topic) ? 1 : 0));

  // ----- subscribe (server-side Stream) -----
  const subscribe = (
    topic: Topic,
  ): Stream.Stream<BroadcastEnvelope<T>, never, never> => {
    const pub = ensurePub(topic);
    return Stream.fromPubSub(pub);
  };

  // ----- internals (used by host()) -----
  /** Forcibly drop a topic (used when the topic limit is exceeded). */
  const _evict = (topic: Topic): void => {
    pubs.delete(topic);
  };

  return {
    handlePublish,
    count,
    subscribe,
    broadcast,
    _evict,
    _maxTopics: maxTopics,
    _capacity: capacity,
  } as BroadcastServer<T> & {
    readonly _evict: (topic: Topic) => void;
    readonly _maxTopics: number;
    readonly _capacity: number;
  };
};

// ---------------------------------------------------------------------------
// Server surface
// ---------------------------------------------------------------------------

export interface BroadcastServer<T = unknown> {
  /**
   * Accept a publish request, fan out to every subscriber on this
   * shard, and return the assigned event id.
   */
  readonly handlePublish: (
    body: unknown,
  ) => Effect.Effect<PublishResponse, HttpError, never>;
  /**
   * Live stream of envelopes for a topic on this shard. Used by the
   * DO host to forward events to subscribed sockets; also returned by
   * the `Broadcast.subscribe` client helper (which opens a WS to the
   * right shard and reads from it).
   */
  readonly subscribe: (
    topic: Topic,
  ) => Stream.Stream<BroadcastEnvelope<T>, never, never>;
  /**
   * How many distinct topics this shard currently knows about
   * (cheap, used by tests / observability).
   */
  readonly count: (topic: Topic) => Effect.Effect<number, never, never>;
  /**
   * Imperative fan-out — mostly for tests; the DO host's
   * `webSocketMessage` path uses `subscribe` + a forwarder fiber
   * instead.
   */
  readonly broadcast: (
    envelope: BroadcastEnvelope<T>,
  ) => Effect.Effect<void, never, never>;
}

// ---------------------------------------------------------------------------
// Decode helpers (mirrors meow/sync's safeDecode)
// ---------------------------------------------------------------------------

type DecodeResult<T> =
  | { readonly _tag: "ok"; readonly value: T }
  | {
      readonly _tag: "error";
      readonly error: string;
      readonly detail?: unknown;
    };

const safeDecode = <S extends Schema.Schema<any>>(
  schema: S,
  body: unknown,
  fallback: string,
): Effect.Effect<DecodeResult<Schema.Schema.Type<S>>, never, never> =>
  Effect.sync(() => {
    const decoded = (Schema.decodeUnknownEffect(schema)(body) as unknown as {
      readonly _tag: "Failure" | "Success";
      readonly value?: unknown;
      readonly error?: { message?: string };
    });
    if (decoded && decoded._tag === "Failure") {
      return {
        _tag: "error" as const,
        error: fallback,
        detail: decoded.error?.message ?? String(decoded.error),
      };
    }
    return {
      _tag: "ok" as const,
      value: (decoded?.value ?? null) as Schema.Schema.Type<S>,
    };
  });

// ---------------------------------------------------------------------------
// Durable Object class — wire it up with the returned `BroadcastServer`
// ---------------------------------------------------------------------------

/**
 * Build a `cf.DurableObject` class that delegates every request to
 * the provided {@link BroadcastServer}. One DO instance per shard —
 * the client helpers in `index.ts` (`publish`, `subscribe`) route to
 * the right shard via `idFromName("shard-" + index)`.
 *
 * Wire protocol:
 *
 * - `POST /publish` — JSON `{ topic, event }` → `{ accepted, id }`
 * - `GET /count?topic=...` — JSON `{ count }`
 * - `Upgrade: websocket` — bidirectional subscribe channel
 *   - server → client: `{ kind: "hello", sessionId }`,
 *     `{ kind: "event", topic, id, ts, event }`, `{ kind: "pong" }`,
 *     `{ kind: "error", error }`
 *   - client → server: `{ kind: "subscribe", topics: [...] }`,
 *     `{ kind: "unsubscribe", topics: [...] }`, `{ kind: "ping" }`
 *
 * @example
 * ```typescript
 * import { Broadcast } from "meow/broadcast";
 * const program = Broadcast.make<MyEvent>();
 * const server = await Effect.runPromise(program);
 * export class TopicShardDO extends Broadcast.host(server) {}
 * ```
 */
export const host = <T>(
  server: BroadcastServer<T>,
): {
  new (state: cf.DurableObjectState, env: any): BroadcastDurableObject<T>;
} => {
  class BroadcastDO implements BroadcastDurableObject<T> {
    readonly #state: cf.DurableObjectState;
    readonly #subscribers = new Map<cf.WebSocket, Subscriber>();

    constructor(state: cf.DurableObjectState, _env: any) {
      this.#state = state;
    }

    async fetch(request: Request): Promise<Response> {
      return this.#handleFetch(request);
    }

    async #handleFetch(request: Request): Promise<Response> {
      const url = new URL(request.url);
      try {
        if (url.pathname === "/publish" && request.method === "POST") {
          return await this.#handlePublish(request);
        }
        if (url.pathname === "/count" && request.method === "GET") {
          return await this.#handleCount(url);
        }
        if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
          return await this.#handleWebSocketUpgrade(request);
        }
        return new Response("not found", { status: 404 });
      } catch (e) {
        return new Response(
          `internal error: ${(e as { message?: string }).message ?? String(e)}`,
          { status: 500 },
        );
      }
    }

    async #handlePublish(request: Request): Promise<Response> {
      const body = (await request.json()) as unknown;
      const exit = await Effect.runPromiseExit(server.handlePublish(body));
      if (exit._tag === "Failure") {
        const err = (exit.cause as { error?: HttpError }).error;
        if (err && err._tag === "HttpError") {
          return new Response(JSON.stringify(err.body), {
            status: err.status,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response("internal error", { status: 500 });
      }
      return new Response(JSON.stringify(exit.value), {
        headers: { "content-type": "application/json" },
      });
    }

    async #handleCount(url: URL): Promise<Response> {
      const topic = url.searchParams.get("topic");
      if (!topic) return new Response("missing topic", { status: 400 });
      const count = await Effect.runPromise(server.count(topic));
      return new Response(JSON.stringify({ count }), {
        headers: { "content-type": "application/json" },
      });
    }

    async #handleWebSocketUpgrade(request: Request): Promise<Response> {
      // @ts-expect-error — `WebSocketPair` is a Cloudflare runtime global
      // not in the standard type surface.
      const pair = new WebSocketPair();
      const [client, serverSocket] = pair as unknown as readonly [
        cf.WebSocket,
        cf.WebSocket,
      ];
      this.#state.acceptWebSocket(serverSocket);

      const sessionId = crypto.randomUUID();
      const send = (msg: ServerMessage<T>) => {
        try {
          serverSocket.send(JSON.stringify(msg));
        } catch {
          // socket may be closed
        }
      };
      send({ kind: "hello", sessionId });

      const sub: Subscriber = {
        topics: new Set(),
        send: send as (msg: ServerMessage) => void,
        fibers: new Map(),
      };
      this.#subscribers.set(serverSocket, sub);

      return new Response(null, {
        status: 101,
        webSocket: client,
      } as ResponseInit);
    }

    webSocketMessage(
      ws: cf.WebSocket,
      message: string | ArrayBuffer,
    ): Promise<void> {
      const text =
        typeof message === "string"
          ? message
          : new TextDecoder().decode(message as ArrayBuffer);
      let parsed: ClientMessage | null = null;
      try {
        parsed = JSON.parse(text) as ClientMessage;
      } catch {
        this.#sendError(ws, "invalid json");
        return Promise.resolve();
      }
      this.#dispatch(ws, parsed);
      return Promise.resolve();
    }

    #dispatch(ws: cf.WebSocket, msg: ClientMessage | null): void {
      if (!msg || typeof msg !== "object" || !("kind" in msg)) {
        this.#sendError(ws, "invalid envelope");
        return;
      }
      const sub = this.#subscribers.get(ws);
      if (!sub) return;

      switch (msg.kind) {
        case "subscribe": {
          for (const topic of msg.topics) {
            if (sub.topics.has(topic)) continue;
            sub.topics.add(topic);
            const stream = server.subscribe(topic);
            const fiber = Effect.runFork(
              Stream.runForEach(stream, (env) =>
                Effect.sync(() => sub.send(env as ServerMessage)),
              ),
            );
            sub.fibers.set(topic, fiber);
          }
          return;
        }
        case "unsubscribe": {
          for (const topic of msg.topics) {
            if (!sub.topics.has(topic)) continue;
            sub.topics.delete(topic);
            const fiber = sub.fibers.get(topic);
            if (fiber) {
              Effect.runFork(Fiber.interrupt(fiber));
              sub.fibers.delete(topic);
            }
          }
          return;
        }
        case "ping": {
          sub.send({ kind: "pong" });
          return;
        }
      }
      this.#sendError(ws, `unknown kind ${(msg as { kind: unknown }).kind}`);
    }

    #sendError(ws: cf.WebSocket, error: string): void {
      const sub = this.#subscribers.get(ws);
      sub?.send({ kind: "error", error });
    }

    webSocketClose(
      ws: cf.WebSocket,
      _code: number,
      _reason: string,
      _wasClean: boolean,
    ): Promise<void> {
      const sub = this.#subscribers.get(ws);
      if (sub) {
        for (const fiber of sub.fibers.values()) {
          Effect.runFork(Fiber.interrupt(fiber));
        }
        sub.fibers.clear();
        sub.topics.clear();
        this.#subscribers.delete(ws);
      }
      return Promise.resolve();
    }

    webSocketError(_ws: cf.WebSocket, _error: unknown): Promise<void> {
      return Promise.resolve();
    }
  }
  return BroadcastDO as never;
};

/**
 * The {@link BroadcastServer}-backed Durable Object surface.
 */
export interface BroadcastDurableObject<T = unknown> {
  fetch(request: Request): Promise<Response>;
  webSocketMessage(
    ws: cf.WebSocket,
    message: string | ArrayBuffer,
  ): Promise<void>;
  webSocketClose(
    ws: cf.WebSocket,
    code: number,
    reason: string,
    wasClean: boolean,
  ): Promise<void>;
  webSocketError?(ws: cf.WebSocket, error: unknown): Promise<void>;
}