import type * as cf from "@cloudflare/workers-types";
import * as Effect from "effect/Effect";
import * as Duration from "effect/Duration";
import * as Fiber from "effect/Fiber";
import {
  type HeartbeatConfig,
  type HeartbeatOptions,
  isStale,
  resolveServerHeartbeat,
} from "../core/purr.ts";
import {
  type PresenceAttachment,
  type PresenceClientEnvelope,
  type PresenceServerEnvelope,
} from "../core/protocol.ts";
import {
  allow,
  type AuthContext,
  type PresenceHooks,
  type PresenceLogEvent,
  type RateLimitCounters,
} from "../core/hooks.ts";
import {
  type PrivateRouteTable,
  deregisterConnection,
  registerConnection,
  resolveAuthId,
  resolveConnection,
} from "../core/routes.ts";
import {
  decodeEnvelope,
  fromCfWebSocket,
  type MeowSocket,
} from "../core/attachment.ts";

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface PresenceOptions<TState, TMessage = unknown> {
  /**
   * Optional hook called once per new WebSocket connection. The returned
   * value becomes this connection's initial presence and is broadcast to
   * everyone else as a `presence` envelope. Return `undefined` to leave the
   * initial state as `null`.
   */
  readonly initialState?: (
    request: Request,
  ) => Effect.Effect<TState | undefined>;
  readonly hooks?: PresenceHooks<TState, TMessage>;
  readonly heartbeat?: HeartbeatOptions;
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

interface RateWindow {
  inboundLastSecond: number[];
  inboundLastMinute: number[];
  broadcastsLastMinute: number[];
  privatesLastMinute: number[];
}

interface SerialisedAuth {
  readonly id: string;
  readonly name?: string;
  readonly claims?: Readonly<Record<string, unknown>>;
}

const authToAttachment = (auth: AuthContext | undefined): SerialisedAuth | undefined =>
  auth ? { id: auth.id, name: auth.name, claims: auth.claims } : undefined;

// ---------------------------------------------------------------------------
// Public RPC shape (extracted from the protocol)
// ---------------------------------------------------------------------------

export interface PresenceShape<TState, TMessage> {
  count(): number;
  connections(): readonly string[];
  authIds(): readonly string[];
  getState(id: string): TState | undefined;
  broadcast(data: TMessage): Effect.Effect<void, never, never>;
  sendPrivate(
    to: string,
    data: TMessage,
    from?: string,
  ): Effect.Effect<number, never, never>;
  kick(
    id: string,
    code?: number,
    reason?: string,
  ): Effect.Effect<void, never, never>;
  setState(id: string, state: TState): Effect.Effect<void, never, never>;
}

// ---------------------------------------------------------------------------
// `Presence.make` — the canonical factory (distilled-only, no alchemy)
// ---------------------------------------------------------------------------

/**
 * Build a Cloudflare Durable Object class for a Presence room.
 *
 * The returned class implements `cf.DurableObject` directly and depends
 * only on `effect` + `@cloudflare/workers-types` + `@distilled.cloud/cloudflare`
 * (the distilled types) — no Alchemy runtime required.
 *
 * ```toml
 * # wrangler.toml
 * [[durable_objects.bindings]]
 * name = "ROOM"
 * class_name = "Room"
 *
 * [[migrations]]
 * tag = "v1"
 * new_sqlite_classes = ["Room"]
 * ```
 *
 * @example Defining a Room
 * ```typescript
 * import { Presence } from "meow/presence";
 *
 * export interface CursorState {
 *   cursor: { x: number; y: number } | null;
 *   name: string;
 *   color: string;
 * }
 *
 * export class Room extends Presence.make<CursorState, never>({
 *   heartbeat: { interval: "30 seconds", timeout: "45 seconds" },
 * }) {}
 * ```
 *
 * @example Worker host
 * ```typescript
 * import { host } from "meow/presence";
 * export default { fetch: (req, env) => host(req, env, {
 *   namespace: env.ROOM,
 *   room: (r) => new URL(r.url).pathname.split("/")[2] ?? "lobby",
 * }) };
 * ```
 */
export const make = <TState = unknown, TMessage = unknown>(
  options?: PresenceOptions<TState, TMessage>,
): new (
  state: cf.DurableObjectState,
  env: any,
) => DurableObjectRoom<TState, TMessage> => {
  const hooks = options?.hooks;
  const heartbeat = options?.heartbeat
    ? resolveServerHeartbeat(options.heartbeat)
    : undefined;
  const initialStateHook = options?.initialState;

  class RoomDO implements DurableObjectRoom<TState, TMessage> {
    readonly #state: cf.DurableObjectState;
    readonly #sockets = new Map<string, MeowSocket>();
    readonly #authByConnection = new Map<string, AuthContext | undefined>();
    readonly #connectionsByAuth = new Map<string, readonly string[]>();
    readonly #routes: PrivateRouteTable = {
      connectionsById: this.#authByConnection,
      connectionsByAuth: this.#connectionsByAuth,
    };
    readonly #rateWindows = new Map<string, RateWindow>();

    // The heartbeat runs as a forever-fiber in the DO instance's lifetime.
    // workerd's isolate teardown is our de-facto scope — there is no
    // close() to call here.
    readonly #heartbeatFiber: Fiber.Fiber<void, never>;

    constructor(state: cf.DurableObjectState, _env: any) {
      this.#state = state;

      // Rehydrate sockets. The DO bridge runs the constructor under
      // `state.blockConcurrencyWhile`, so by the time any `fetch` lands
      // the map is populated. We don't need to await anything here.
      for (const socket of state.getWebSockets()) {
        const meta = socket.deserializeAttachment() as
          | PresenceAttachment<TState>
          | null;
        if (meta) {
          this.#sockets.set(meta.id, fromCfWebSocket(socket, meta));
        }
      }

      if (heartbeat) {
        this.#heartbeatFiber = Effect.runFork(this.#heartbeatSweep());
      } else {
        this.#heartbeatFiber = Effect.runFork(Effect.never);
      }
    }

    // -------------------------------------------------------------------
    // Lifecycle: fetch
    // -------------------------------------------------------------------

    fetch(request: Request): Promise<Response> {
      return Effect.runPromise(this.#handleFetch(request));
    }

    #handleFetch(
      request: Request,
    ): Effect.Effect<Response, never, never> {
      const program = Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          const url = new URL(request.url);

          // RPC endpoints (`/__presence/<method>`) take the plain-HTTP
          // path. Everything else falls through to WebSocket upgrade.
          if (url.pathname.startsWith("/__presence/")) {
            return yield* this.#handleRpc(url, request);
          }

          if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
            return new Response("not found", { status: 404 });
          }

          return yield* this.#handleWebSocketUpgrade(request);
        },
      );
      return Effect.catch(program, (e) =>
        Effect.sync(
          () =>
            new Response(
              `internal error: ${(e as { message?: string }).message ?? String(e)}`,
              { status: 500 },
            ),
        ),
      );
    }

    #handleRpc(
      url: URL,
      request: Request,
    ): Effect.Effect<Response, never, never> {
      const method = url.pathname.slice("/__presence/".length);
      return Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          let body: unknown = undefined;
          if (request.method !== "GET" && request.method !== "HEAD") {
            body = yield* Effect.tryPromise({
              try: () => request.json() as Promise<unknown>,
              catch: () => undefined,
            }).pipe(Effect.orElseSucceed(() => undefined));
          }
          let value: unknown;
          switch (method) {
            case "count":
              value = this.count();
              break;
            case "connections":
              value = this.connections();
              break;
            case "authIds":
              value = this.authIds();
              break;
            case "getState": {
              const id = url.searchParams.get("id");
              if (!id) return new Response("missing id", { status: 400 });
              value = this.getState(id);
              break;
            }
            case "broadcast":
              value = yield* this.broadcast(body as TMessage);
              break;
            case "sendPrivate": {
              const args = body as { to: string; data: TMessage; from?: string };
              value = yield* this.sendPrivate(args.to, args.data, args.from);
              break;
            }
            case "kick": {
              const id = url.searchParams.get("id");
              if (!id) return new Response("missing id", { status: 400 });
              const code = Number(url.searchParams.get("code") ?? "1000");
              const reason = url.searchParams.get("reason") ?? "kicked";
              value = yield* this.kick(id, code, reason);
              break;
            }
            case "setState": {
              const id = url.searchParams.get("id");
              if (!id) return new Response("missing id", { status: 400 });
              value = yield* this.setState(id, body as TState);
              break;
            }
            default:
              return new Response(`unknown rpc ${method}`, { status: 404 });
          }
          return new Response(JSON.stringify({ ok: true, value }), {
            headers: { "content-type": "application/json" },
          });
        },
      );
    }

    #handleWebSocketUpgrade(
      request: Request,
    ): Effect.Effect<Response, never, never> {
      return Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          // Auth — reject if the hook returns undefined or throws.
          let auth: AuthContext | undefined;
          if (hooks?.authenticate) {
            const result = yield* hooks
              .authenticate(request)
              .pipe(Effect.catchCause(() => Effect.succeed(undefined)));
            if (!result) {
              yield* invokeHook(hooks?.log, { kind: "auth-rejected" });
              return new Response("unauthorized", { status: 401 });
            }
            auth = result;
          }

          // Accept the WebSocket upgrade. We construct a WebSocketPair
          // manually because the alchemy-flavored `upgrade()` helper is no
          // longer available in distilled-only mode.
          // @ts-expect-error — `WebSocketPair` is a Cloudflare runtime global
          // not in the standard type surface.
          const pair = new WebSocketPair();
          const [client, server] = pair as unknown as readonly [
            cf.WebSocket,
            cf.WebSocket,
          ];
          const sock = fromCfWebSocket(server, undefined as never);

          const id = crypto.randomUUID();

          // Seed initial state from the `initialState` hook.
          const seed = hooks?.initialState
            ? yield* hooks.initialState(request, auth)
            : initialStateHook
              ? yield* initialStateHook(request)
              : undefined;
          const seedState = (seed ?? null) as TState;

          sock.serializeAttachment({
            id,
            state: seedState,
            lastPurr: heartbeat ? Date.now() : undefined,
            authId: auth?.id,
          } satisfies PresenceAttachment<TState>);
          this.#sockets.set(id, sock);

          // Update the auth routing tables.
          const next = registerConnection(this.#routes, id, auth);
          for (const [k, v] of next.connectionsById) {
            if (v === undefined) {
              this.#authByConnection.delete(k);
            } else {
              this.#authByConnection.set(k, v);
            }
          }
          for (const [k, v] of next.connectionsByAuth) {
            this.#connectionsByAuth.set(k, [...v]);
          }

          // Tell the runtime we want to handle the socket's lifecycle.
          yield* Effect.sync(() => this.#state.acceptWebSocket(server));

          // Send the initial snapshot.
          const peers: Record<string, TState> = {};
          for (const [peerId, peerSock] of this.#sockets) {
            if (peerId === id) continue;
            const meta =
              peerSock.deserializeAttachment() as PresenceAttachment<TState> | null;
            if (meta) peers[peerId] = meta.state;
          }
          yield* Effect.sync(() =>
            this.#send(sock, { kind: "init", self: id, peers }),
          );
          yield* invokeHook(hooks?.log, {
            kind: "connect",
            id,
            authId: auth?.id,
          });

          // Notify the rest of the room.
          yield* Effect.sync(() =>
            this.#broadcast(
              { kind: "presence", id, state: seedState },
              sock,
            ),
          );

          return new Response(null, { status: 101, webSocket: client } as ResponseInit);
        },
      );
    }

    // -------------------------------------------------------------------
    // Lifecycle: webSocketMessage
    // -------------------------------------------------------------------

    webSocketMessage(
      ws: cf.WebSocket,
      raw: string | ArrayBuffer,
    ): Promise<void> {
      return Effect.runPromise(this.#handleMessage(ws, raw));
    }

    #handleMessage(
      ws: cf.WebSocket,
      raw: string | ArrayBuffer,
    ): Effect.Effect<void, never, never> {
      return Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          const meta = ws.deserializeAttachment() as
            | PresenceAttachment<TState>
            | null;
          if (!meta) return;
          const sock = this.#sockets.get(meta.id);
          if (!sock) return;

          // Update lastPurr on every inbound message — purr piggy-backs on
          // every message, so we don't need a separate tracker.
          if (heartbeat) {
            yield* Effect.sync(() =>
              ws.serializeAttachment({ ...meta, lastPurr: Date.now() }),
            );
          }

          const envelope = decodeEnvelope<
            PresenceClientEnvelope<TState, TMessage>
          >(raw);
          if (!envelope) return;

          const counters = this.#rateLimitCounters(meta.id);
          const decision = hooks?.rateLimit
            ? yield* hooks.rateLimit({
                id: meta.id,
                kind: envelope.kind,
                authId: meta.authId,
                counters,
              })
            : allow;
          if (!decision.allow) {
            yield* invokeHook(hooks?.onThrottled, {
              id: meta.id,
              kind: envelope.kind,
              retryAfterMs: decision.retryAfterMs,
            });
            yield* invokeHook(hooks?.log, {
              kind: "throttled",
              id: meta.id,
              kind_: envelope.kind,
            });
            yield* Effect.sync(() =>
              this.#send(sock, {
                kind: "throttled",
                retryAfterMs: decision.retryAfterMs,
              }),
            );
            return;
          }
          yield* Effect.sync(() => this.#recordInbound(meta.id));

          const bytes = typeof raw === "string" ? raw.length : raw.byteLength;
          yield* invokeHook(hooks?.onMessage, {
            id: meta.id,
            kind: envelope.kind,
            bytes,
            auth: resolveConnection(this.#routes, meta.id),
          });

          yield* Effect.sync(() =>
            this.#dispatch(meta, sock, envelope, bytes),
          );
        },
      );
    }

    #dispatch(
      meta: PresenceAttachment<TState>,
      sock: MeowSocket,
      envelope: PresenceClientEnvelope<TState, TMessage>,
      bytes: number,
    ): void {
      switch (envelope.kind) {
        case "state": {
          meta.state = envelope.state;
          sock.serializeAttachment(meta);
          const recipients = this.#broadcast(
            { kind: "presence", id: meta.id, state: envelope.state },
            sock,
          );
          Effect.runFork(
            invokeHook(hooks?.onBroadcast, {
              kind: "state",
              from: meta.id,
              recipientCount: recipients,
              bytes,
            }),
          );
          return;
        }
        case "message": {
          this.#recordBroadcast(meta.id);
          const recipients = this.#broadcast({
            kind: "message",
            from: meta.id,
            data: envelope.data,
          });
          if (envelope.id) {
            this.#send(sock, { kind: "ack", id: envelope.id });
          }
          Effect.runFork(
            invokeHook(hooks?.onBroadcast, {
              kind: "message",
              from: meta.id,
              recipientCount: recipients,
              bytes,
            }),
          );
          return;
        }
        case "private": {
          this.#recordPrivate(meta.id);
          const delivered = this.#sendPrivate(envelope.to, {
            kind: "private",
            from: meta.id,
            data: envelope.data,
          });
          if (envelope.id) {
            this.#send(sock, { kind: "ack", id: envelope.id });
          }
          Effect.runFork(
            invokeHook(hooks?.onBroadcast, {
              kind: "private",
              from: meta.id,
              recipientCount: delivered,
              bytes,
            }),
          );
          return;
        }
        case "purr": {
          this.#send(sock, { kind: "purr" });
          return;
        }
      }
    }

    // -------------------------------------------------------------------
    // Lifecycle: webSocketClose / webSocketError
    // -------------------------------------------------------------------

    webSocketClose(
      ws: cf.WebSocket,
      code: number,
      reason: string,
      _wasClean: boolean,
    ): Promise<void> {
      return Effect.runPromise(this.#handleClose(ws, code, reason));
    }

    #handleClose(
      ws: cf.WebSocket,
      code: number,
      reason: string,
    ): Effect.Effect<void, never, never> {
      return Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          const meta = ws.deserializeAttachment() as
            | PresenceAttachment<TState>
            | null;
          if (!meta) return;
          yield* Effect.sync(() => {
            this.#sockets.delete(meta.id);
            this.#rateWindows.delete(meta.id);
          });
          yield* Effect.sync(() => {
            const next = deregisterConnection(this.#routes, meta.id);
            for (const [k, v] of next.table.connectionsById) {
              this.#authByConnection.set(k, v);
            }
            for (const [k, v] of next.table.connectionsByAuth) {
              this.#connectionsByAuth.set(k, [...v]);
            }
          });
          yield* Effect.sync(() =>
            this.#broadcast({ kind: "absence", id: meta.id }),
          );
          yield* invokeHook(hooks?.onDisconnect, {
            id: meta.id,
            code,
            reason,
          });
          yield* invokeHook(hooks?.log, {
            kind: "disconnect",
            id: meta.id,
            code,
            reason,
          });
        },
      );
    }

    webSocketError(_ws: cf.WebSocket, _error: unknown): Promise<void> {
      // Errors are followed by `close`, which handles cleanup.
      return Promise.resolve();
    }

    // -------------------------------------------------------------------
    // Schemaless RPC (also reachable via `/__presence/<method>`)
    // -------------------------------------------------------------------

    count(): number {
      return this.#sockets.size;
    }
    connections(): readonly string[] {
      return [...this.#sockets.keys()];
    }
    authIds(): readonly string[] {
      return [...this.#connectionsByAuth.keys()];
    }
    getState(id: string): TState | undefined {
      const sock = this.#sockets.get(id);
      if (!sock) return undefined;
      const meta = sock.deserializeAttachment() as PresenceAttachment<TState> | null;
      return meta?.state;
    }
    broadcast(data: TMessage): Effect.Effect<void, never, never> {
      return Effect.sync(() =>
        this.#broadcast({ kind: "message", from: "*", data }),
      );
    }
    sendPrivate(
      to: string,
      data: TMessage,
      from?: string,
    ): Effect.Effect<number, never, never> {
      return Effect.sync(() =>
        this.#sendPrivate(to, {
          kind: "private",
          from: from ?? "*",
          data,
        }),
      );
    }
    kick(
      id: string,
      code: number = 1000,
      reason: string = "kicked",
    ): Effect.Effect<void, never, never> {
      return Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          const sock = this.#sockets.get(id);
          if (!sock) return;
          yield* Effect.sync(() => {
            try {
              sock.raw.close(code, reason);
            } catch {
              // already closed
            }
            this.#sockets.delete(id);
          });
          yield* Effect.sync(() => this.#broadcast({ kind: "absence", id }));
        },
      );
    }
    setState(
      id: string,
      state: TState,
    ): Effect.Effect<void, never, never> {
      return Effect.gen(
        { self: this as RoomDO },
        function* (this: RoomDO) {
          const sock = this.#sockets.get(id);
          if (!sock) return;
          const meta =
            sock.deserializeAttachment() as PresenceAttachment<TState> | null;
          if (!meta) return;
          yield* Effect.sync(() => {
            meta.state = state;
            sock.serializeAttachment(meta);
          });
          yield* Effect.sync(() =>
            this.#broadcast({ kind: "presence", id, state }, sock),
          );
        },
      );
    }

    // -------------------------------------------------------------------
    // Internals
    // -------------------------------------------------------------------

    #send(
      target: MeowSocket,
      envelope: PresenceServerEnvelope<TState, TMessage>,
    ): void {
      try {
        target.send(JSON.stringify(envelope));
      } catch {
        // close handler will sweep
      }
    }

    #broadcast(
      envelope: PresenceServerEnvelope<TState, TMessage>,
      except?: MeowSocket,
    ): number {
      const payload = JSON.stringify(envelope);
      let count = 0;
      for (const socket of this.#sockets.values()) {
        if (socket === except) continue;
        try {
          socket.send(payload);
          count += 1;
        } catch {
          // close handler will sweep
        }
      }
      return count;
    }

    #sendPrivate(
      toAuthId: string,
      envelope: PresenceServerEnvelope<TState, TMessage>,
    ): number {
      const targets = resolveAuthId(this.#routes, toAuthId);
      if (targets.length === 0) return 0;
      const payload = JSON.stringify(envelope);
      let count = 0;
      for (const id of targets) {
        const socket = this.#sockets.get(id);
        if (!socket) continue;
        try {
          socket.send(payload);
          count += 1;
        } catch {
          // close handler will sweep
        }
      }
      return count;
    }

    #rateLimitCounters(id: string): RateLimitCounters {
      const now = Date.now();
      let w = this.#rateWindows.get(id);
      if (!w) {
        w = {
          inboundLastSecond: [],
          inboundLastMinute: [],
          broadcastsLastMinute: [],
          privatesLastMinute: [],
        };
        this.#rateWindows.set(id, w);
      }
      const oneSecAgo = now - 1_000;
      const oneMinAgo = now - 60_000;
      w.inboundLastSecond = w.inboundLastSecond.filter(
        (t) => t > oneSecAgo,
      );
      w.inboundLastMinute = w.inboundLastMinute.filter(
        (t) => t > oneMinAgo,
      );
      w.broadcastsLastMinute = w.broadcastsLastMinute.filter(
        (t) => t > oneMinAgo,
      );
      w.privatesLastMinute = w.privatesLastMinute.filter(
        (t) => t > oneMinAgo,
      );
      return {
        inboundLastSecond: w.inboundLastSecond.length,
        inboundLastMinute: w.inboundLastMinute.length,
        broadcastsLastMinute: w.broadcastsLastMinute.length,
        privatesLastMinute: w.privatesLastMinute.length,
      };
    }
    #recordInbound(id: string): void {
      const w = this.#rateWindows.get(id);
      if (!w) return;
      const now = Date.now();
      w.inboundLastSecond.push(now);
      w.inboundLastMinute.push(now);
    }
    #recordBroadcast(id: string): void {
      const w = this.#rateWindows.get(id);
      if (!w) return;
      w.broadcastsLastMinute.push(Date.now());
    }
    #recordPrivate(id: string): void {
      const w = this.#rateWindows.get(id);
      if (!w) return;
      w.privatesLastMinute.push(Date.now());
    }

    #heartbeatSweep(): Effect.Effect<void, never, never> {
      const interval = heartbeat?.interval;
      const timeout = heartbeat?.timeout;
      if (!interval || !timeout) return Effect.void;
      const timeoutMs = Duration.toMillis(timeout);
      const self = this;
      const tick = Effect.gen(function* () {
        const now = Date.now();
        for (const [id, sock] of self.#sockets) {
          const meta = sock.deserializeAttachment() as
            | PresenceAttachment<TState>
            | null;
          const lastSeen = meta?.lastPurr;
          if (isStale(lastSeen, timeoutMs, now)) {
            yield* invokeHook(hooks?.log, { kind: "heartbeat-timeout", id });
            yield* Effect.sync(() => {
              try {
                sock.raw.close(1001, "purr timeout");
              } catch {
                // already closed
              }
              self.#sockets.delete(id);
            });
            yield* Effect.sync(() =>
              self.#broadcast({ kind: "absence", id }),
            );
          }
        }
        yield* Effect.sleep(interval);
      });
      return tick.pipe(Effect.forever);
    }
  }

  return RoomDO as unknown as new (
    state: cf.DurableObjectState,
    env: any,
  ) => DurableObjectRoom<TState, TMessage>;
};

// ---------------------------------------------------------------------------
// `invokeHook` — safely run an optional hook Effect, swallow errors
// ---------------------------------------------------------------------------

const invokeHook = <Args extends readonly unknown[]>(
  hook: ((...args: Args) => Effect.Effect<unknown, unknown, never>) | undefined,
  ...args: Args
): Effect.Effect<void, never, never> =>
  hook
    ? Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          hook(...args).pipe(Effect.catch(() => Effect.void)),
        );
        yield* Fiber.join(fiber);
      }).pipe(Effect.ignore)
    : Effect.void;

// ---------------------------------------------------------------------------
// Durable Object surface
// ---------------------------------------------------------------------------

export interface DurableObjectRoom<TState, TMessage>
  extends PresenceShape<TState, TMessage> {
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
// Worker host — minimal HTTP → DO bridge, no alchemy dep
// ---------------------------------------------------------------------------

export interface HostOptions {
  /**
   * Resolve the room id from an incoming request. Typical shapes:
   *
   * - path segment: `(req) => new URL(req.url).pathname.split("/")[2]`
   * - query param:  `(req) => new URL(req.url).searchParams.get("room")`
   * - header:       `(req) => req.headers.get("x-room") ?? "lobby"`
   */
  readonly room: (request: Request) => string;
  /**
   * Fallback for non-WebSocket requests. Default returns 404.
   */
  readonly fallback?: (request: Request) => Response | Promise<Response>;
}

/**
 * Build a Worker `fetch` handler that upgrades every WebSocket request
 * to the matching Presence room via the provided Durable Object
 * namespace. The returned function is a plain `(req, env) => Response |
 * Promise<Response>` — drop it into any `cf.Worker` script that exports
 * `default { fetch }`.
 *
 * @example
 * ```typescript
 * import { Presence, host } from "meow/presence";
 *
 * export class Room extends Presence.make<MyState, MyMessage>() {}
 *
 * export default {
 *   fetch: (req, env) => host(req, env, {
 *     namespace: env.ROOM,
 *     room: (r) => new URL(r.url).pathname.split("/")[2] ?? "lobby",
 *   }),
 * };
 * ```
 */
export const host = (
  request: Request,
  _env: { readonly [k: string]: unknown },
  options: HostOptions & { readonly namespace: cf.DurableObjectNamespace },
): Promise<Response> | Response => {
  if (
    request.headers.get("Upgrade")?.toLowerCase() !== "websocket"
  ) {
    if (options.fallback) return options.fallback(request);
    return new Response("not found", { status: 404 });
  }
  const roomId = options.room(request);
  const id = options.namespace.idFromName(roomId);
  const stub = options.namespace.get(id);
  return (stub.fetch as unknown as (r: Request) => Promise<Response>)(request);
};