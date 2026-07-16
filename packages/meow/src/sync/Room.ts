import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { Schema } from "effect";
import type * as cf from "@cloudflare/workers-types";
import { decodeEnvelope, fromCfWebSocket } from "../core/attachment.ts";
import {
  ClientLiveEnvelope,
  HeadResponse,
  HttpErrorBody,
  LiveEnvelope,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
} from "./protocol.ts";
import {
  type Branch,
  type Event,
  type EventId,
  type Seq,
  type SessionId,
} from "./schema.ts";
import {
  type SyncStore,
  type SyncStoreAppendInput,
} from "./store.ts";

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface SyncOptions {
  /**
   * Optional hook fired when the server accepts an event. Useful for
   * telemetry, billing, or auditing.
   */
  readonly onEventAccepted?: (
    event: Event,
  ) => Effect.Effect<void, never, never>;
}

// ---------------------------------------------------------------------------
// HTTP error envelope
// ---------------------------------------------------------------------------

export interface HttpError {
  readonly _tag: "HttpError";
  readonly status: number;
  readonly body: HttpErrorBody;
}

const httpError = (status: number, error: string, detail?: unknown): HttpError => ({
  _tag: "HttpError",
  status,
  body: { error, detail },
});

// ---------------------------------------------------------------------------
// `Sync.make` — the canonical factory
// ---------------------------------------------------------------------------

/**
 * Build the implementation for a Sync Durable Object. Drop it into a
 * `cf.DurableObject` subclass that delegates `fetch` /
 * `webSocketMessage` / `webSocketClose` to the returned object.
 *
 * The DO is a single DO instance per `storeId` (via `getByName`); the
 * Sync logic itself is store-agnostic, so the same class can host any
 * number of stores — `storeId` is supplied in every request.
 *
 * @resource
 * @product Workers
 * @category Realtime
 */
export const make = (
  store: SyncStore,
  options?: SyncOptions,
): Effect.Effect<SyncServer, never, never> =>
  Effect.gen(function* () {
    return yield* Effect.sync(() => build(store, options));
  });

const build = (store: SyncStore, options?: SyncOptions): SyncServer => {
  // Per-branch PubSub for live event broadcast. A branch's PubSub is
  // created lazily on the first subscribe. We run `PubSub.bounded`
  // through `Effect.runSync` so the PubSub lives in the module
  // closure rather than any caller scope — DO instance teardown is
  // our de-facto lifetime.
  const pubs = new Map<Branch, PubSub.PubSub<LiveEnvelope>>();
  const ensurePub = (branch: Branch): PubSub.PubSub<LiveEnvelope> => {
    const existing = pubs.get(branch);
    if (existing) return existing;
    const created = Effect.runSync(
      PubSub.bounded<LiveEnvelope>({ capacity: 1024 }),
    );
    pubs.set(branch, created);
    return created;
  };
  const getPub = (
    branch: Branch,
  ): Effect.Effect<PubSub.PubSub<LiveEnvelope>, never, never> =>
    Effect.sync(() => ensurePub(branch));

  // Broadcast a freshly-accepted event to every subscriber.
  const broadcast = (event: Event): Effect.Effect<void, never, never> =>
    Effect.sync(() => {
      const pub = ensurePub(event.branch);
      PubSub.publishUnsafe(pub, { kind: "event", event });
    });

  // Read a single event by id, returning null if not found.
  const readOne = (
    branch: Branch,
    id: EventId,
  ): Effect.Effect<Event | null, never, never> =>
    Effect.gen(function* () {
      const events = yield* store.readSince({ branch, since: 0 as Seq, limit: 1000 });
      return events.find((e) => e.id === id) ?? null;
    });

  // ----- push -----
  const handlePush = (
    sessionId: SessionId,
    body: unknown,
  ): Effect.Effect<PushResponse, HttpError, never> =>
    Effect.gen(function* () {
      const decode = yield* decodePush(body);
      if (decode._tag === "error") {
        return yield* Effect.fail(httpError(400, decode.error, decode.detail));
      }
      const req = decode.value;

      // Validate the parent chain. The first event in a batch may
      // have `parent === null` (bootstrap) or `parent` matching the
      // current `head`. Subsequent events must chain: parent[i] ===
      // events[i-1].id.
      let prev: EventId | null = null;
      for (const ev of req.events) {
        if (prev !== null && ev.parent !== prev) {
          return yield* Effect.fail(
            httpError(409, "parent chain broken", {
              eventId: ev.id,
              expected: prev,
              got: ev.parent,
            }),
          );
        }
        if (ev.parent === null) {
          const branchHead = yield* store.head(req.branch);
          if (branchHead.count > 0) {
            return yield* Effect.fail(
              httpError(409, "branch not empty", { head: branchHead.head }),
            );
          }
        } else {
          const known = yield* store.readSince({
            branch: req.branch,
            since: 0 as Seq,
            limit: 1,
          });
          if (ev.parent !== prev && known[0]?.id !== ev.parent) {
            return yield* Effect.fail(
              httpError(409, "unknown parent", {
                eventId: ev.id,
                parent: ev.parent,
              }),
            );
          }
        }
        prev = ev.id;
      }

      const accepted: EventId[] = [];
      for (const ev of req.events) {
        const input: SyncStoreAppendInput = {
          branch: req.branch,
          id: ev.id,
          parent: ev.parent,
          payload: ev.payload,
          author: sessionId,
          createdAt: ev.createdAt,
        };
        yield* store.append(input);
        accepted.push(ev.id);
      }
      const headAfter = yield* store.head(req.branch);

      for (const id of accepted) {
        const event = yield* readOne(req.branch, id);
        if (event) {
          yield* broadcast(event);
          if (options?.onEventAccepted) {
            yield* options.onEventAccepted(event);
          }
        }
      }

      return {
        accepted,
        head: headAfter.head,
      } satisfies PushResponse;
    });

  // ----- pull -----
  const handlePull = (
    body: unknown,
  ): Effect.Effect<PullResponse, HttpError, never> =>
    Effect.gen(function* () {
      const decode = yield* decodePull(body);
      if (decode._tag === "error") {
        return yield* Effect.fail(httpError(400, decode.error, decode.detail));
      }
      const req = decode.value;
      const events = yield* store.readSince({
        branch: req.branch,
        since: req.since ?? (0 as Seq),
        limit: req.limit,
      });
      const head = yield* store.head(req.branch);
      return {
        events: events as readonly Event[],
        head: head.head,
      } satisfies PullResponse;
    });

  // ----- head -----
  const handleHead = (
    branch: Branch,
  ): Effect.Effect<HeadResponse, never, never> =>
    Effect.gen(function* () {
      const h = yield* store.head(branch);
      return { head: h.head, count: h.count } satisfies HeadResponse;
    });

  return {
    handlePush,
    handlePull,
    handleHead,
    /**
     * Stream of live envelopes for a given branch. Subscribers see
     * every event accepted on this branch going forward (existing
     * events are pulled, not pushed).
     */
    subscribe: (branch: Branch): Stream.Stream<LiveEnvelope, never, never> => {
      const pub = ensurePub(branch);
      return Stream.fromPubSub(pub);
    },
    broadcast,
  };
};

// ---------------------------------------------------------------------------
// Sync server surface
// ---------------------------------------------------------------------------

export interface SyncServer {
  readonly handlePush: (
    sessionId: SessionId,
    body: unknown,
  ) => Effect.Effect<PushResponse, HttpError, never>;
  readonly handlePull: (
    body: unknown,
  ) => Effect.Effect<PullResponse, HttpError, never>;
  readonly handleHead: (
    branch: Branch,
  ) => Effect.Effect<HeadResponse, never, never>;
  readonly subscribe: (
    branch: Branch,
  ) => Stream.Stream<LiveEnvelope, never, never>;
  readonly broadcast: (event: Event) => Effect.Effect<void, never, never>;
}

// ---------------------------------------------------------------------------
// Decode helpers — wrap Schema with HTTP-friendly error envelopes
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
): Effect.Effect<
  DecodeResult<Schema.Schema.Type<S>>,
  { readonly _tag: "error"; readonly error: string; readonly detail?: unknown },
  never
> =>
  Effect.gen(function* () {
    const decoded = yield* (Schema.decodeUnknownEffect(schema)(body) as unknown as Effect.Effect<
      Schema.Schema.Type<S>,
      { readonly message?: string },
      never
    >).pipe(
      Effect.mapError((e) => ({
        _tag: "error" as const,
        error: fallback,
        detail: e.message ?? String(e),
      })),
    );
    return { _tag: "ok" as const, value: decoded };
  });

const decodePush = (
  body: unknown,
): Effect.Effect<
  DecodeResult<Schema.Schema.Type<typeof PushRequest>>,
  never,
  never
> =>
  safeDecode(PushRequest, body, "invalid push") as Effect.Effect<
    DecodeResult<Schema.Schema.Type<typeof PushRequest>>,
    never,
    never
  >;

const decodePull = (
  body: unknown,
): Effect.Effect<
  DecodeResult<Schema.Schema.Type<typeof PullRequest>>,
  never,
  never
> =>
  safeDecode(PullRequest, body, "invalid pull") as Effect.Effect<
    DecodeResult<Schema.Schema.Type<typeof PullRequest>>,
    never,
    never
  >;

// ---------------------------------------------------------------------------
// Durable Object class — wire it up with the returned `SyncServer`
// ---------------------------------------------------------------------------

/**
 * Build a `cf.DurableObject` class that delegates every request to
 * the provided {@link SyncServer}. Pair with the in-memory or SQLite
 * store, then wire it up in `wrangler.toml`.
 *
 * @example
 * ```typescript
 * import { Sync } from "meow/sync";
 * import { makeInMemoryStore } from "meow/sync/store";
 *
 * const server = Sync.make(makeInMemoryStore());
 * const SyncDO = Sync.host(server);
 * export class SyncStore_ extends SyncDO {}
 * ```
 */
export const host = (
  server: SyncServer,
): {
  new (state: cf.DurableObjectState, env: any): SyncDurableObject;
} => {
  class SyncDO implements SyncDurableObject {
    readonly #state: cf.DurableObjectState;
    /** Per-branch active subscriber fibers, keyed by cf.WebSocket identity. */
    readonly #subscribers = new Map<cf.WebSocket, Fiber.Fiber<void, never>>();
    /** Per-branch live senders. */
    readonly #sockets = new Map<cf.WebSocket, { send: (env: LiveEnvelope) => void }>();

    constructor(state: cf.DurableObjectState, _env: any) {
      this.#state = state;
    }

    async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);
      if (url.pathname === "/push" && request.method === "POST") {
        return await this.#handlePush(request);
      }
      if (url.pathname === "/pull") {
        return await this.#handlePull(request);
      }
      if (url.pathname === "/head") {
        const branch = url.searchParams.get("branch");
        if (!branch) return new Response("missing branch", { status: 400 });
        const result = await Effect.runPromise(
          server.handleHead(branch as Branch),
        );
        return new Response(JSON.stringify(result), {
          headers: { "content-type": "application/json" },
        });
      }
      if (request.headers.get("Upgrade")?.toLowerCase() === "websocket") {
        return await this.#handleWebSocket(request);
      }
      return new Response("not found", { status: 404 });
    }

    async #handlePush(request: Request): Promise<Response> {
      const body = (await request.json()) as unknown;
      const sessionId =
        (request.headers.get("x-meow-session") as SessionId) ??
        (crypto.randomUUID() as SessionId);
      const exit = await Effect.runPromiseExit(
        server.handlePush(sessionId, body),
      );
      if (exit._tag === "Failure") {
        const error = (exit.cause as { error?: HttpError }).error;
        if (error && error._tag === "HttpError") {
          return new Response(JSON.stringify(error.body), {
            status: error.status,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response("internal error", { status: 500 });
      }
      return new Response(JSON.stringify(exit.value), {
        headers: { "content-type": "application/json" },
      });
    }

    async #handlePull(request: Request): Promise<Response> {
      const url = new URL(request.url);
      const body = {
        storeId: url.searchParams.get("storeId") ?? "default",
        branch: (url.searchParams.get("branch") ?? "main") as Branch,
        since: url.searchParams.get("since")
          ? Number(url.searchParams.get("since"))
          : undefined,
        limit: url.searchParams.get("limit")
          ? Number(url.searchParams.get("limit"))
          : undefined,
      };
      const exit = await Effect.runPromiseExit(server.handlePull(body));
      if (exit._tag === "Failure") {
        const error = (exit.cause as { error?: HttpError }).error;
        if (error && error._tag === "HttpError") {
          return new Response(JSON.stringify(error.body), {
            status: error.status,
            headers: { "content-type": "application/json" },
          });
        }
        return new Response("internal error", { status: 500 });
      }
      return new Response(JSON.stringify(exit.value), {
        headers: { "content-type": "application/json" },
      });
    }

    async #handleWebSocket(request: Request): Promise<Response> {
      // @ts-expect-error — `WebSocketPair` is a Cloudflare runtime global
      // not in the standard type surface.
      const pair = new WebSocketPair();
      const [client, server2] = pair as unknown as readonly [
        cf.WebSocket,
        cf.WebSocket,
      ];
      this.#state.acceptWebSocket(server2);

      const sessionId = crypto.randomUUID() as SessionId;
      const head = await Effect.runPromise(server.handleHead("main" as Branch));
      const helloEnvelope: LiveEnvelope = {
        kind: "hello",
        sessionId,
        head: head.head,
      };

      const send = (env: LiveEnvelope) => {
        try {
          server2.send(JSON.stringify(env));
        } catch {
          // socket may be closed
        }
      };
      this.#sockets.set(server2, { send });
      send(helloEnvelope);

      return new Response(null, {
        status: 101,
        webSocket: client,
      } as ResponseInit);
    }

    async webSocketMessage(
      ws: cf.WebSocket,
      message: string | ArrayBuffer,
    ): Promise<void> {
      const text =
        typeof message === "string"
          ? message
          : new TextDecoder().decode(message as ArrayBuffer);
      const envelope = decodeEnvelope<ClientLiveEnvelope>(text);
      if (!envelope) return;
      switch (envelope.kind) {
        case "ping":
          this.#sockets.get(ws)?.send({ kind: "pong" });
          return;
        case "subscribe": {
          const stream = server.subscribe(envelope.branch);
          const fiber = Effect.runFork(
            Stream.runForEach(stream, (env) =>
              Effect.sync(() => this.#sockets.get(ws)?.send(env)),
            ),
          );
          this.#subscribers.set(ws, fiber);
          return;
        }
      }
    }
    async webSocketClose(
      ws: cf.WebSocket,
      _code: number,
      _reason: string,
      _wasClean: boolean,
    ): Promise<void> {
      const fiber = this.#subscribers.get(ws);
      if (fiber) {
        Effect.runFork(Fiber.interrupt(fiber));
        this.#subscribers.delete(ws);
      }
      this.#sockets.delete(ws);
    }
  }
  return SyncDO as never;
};

/**
 * The {@link SyncServer}-backed Durable Object surface.
 */
export interface SyncDurableObject {
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

// ---------------------------------------------------------------------------
// Layer helper (kept for parity with `Presence.host`-style usage)
// ---------------------------------------------------------------------------

export const live = (
  server: SyncServer,
): Layer.Layer<never> => Layer.empty as never;