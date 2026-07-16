import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";

/**
 * Authentication context returned from {@link PresenceHooks.authenticate}.
 * Yield it from a Connection handler to access the authenticated user
 * (or anything else the hook chose to attach) further down the chain.
 */
export interface AuthContext {
  /** Stable identifier for the authenticated principal. */
  readonly id: string;
  /** Optional display name. */
  readonly name?: string;
  /** Free-form claims bag. */
  readonly claims?: Readonly<Record<string, unknown>>;
}

/**
 * Telemetry span hook. Called at the start and end of every connection,
 * every inbound envelope, and every broadcast — so the user can wire up
 * OpenTelemetry / Axiom / Datadog without touching meow code.
 */
export interface TelemetryHook {
  readonly onConnectionOpen?: (
    info: { id: string; remoteAddress?: string },
  ) => Effect.Effect<void, never, never>;
  readonly onConnectionClose?: (
    info: { id: string; code: number; reason: string },
  ) => Effect.Effect<void, never, never>;
  readonly onMessage?: (
    info: { id: string; kind: string; bytes: number },
  ) => Effect.Effect<void, never, never>;
  readonly onBroadcast?: (
    info: { kind: string; recipientCount: number; bytes: number },
  ) => Effect.Effect<void, never, never>;
}

// ---------------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------------

/**
 * Sliding-window counters maintained per connection. meow tracks these
 * internally and hands them to your {@link PresenceHooks.rateLimit} hook
 * so you can implement whatever policy makes sense (token bucket, leaky
 * bucket, fixed window, your own cloud provider).
 */
export interface RateLimitCounters {
  /** Inbound envelopes from this connection in the last 1s window. */
  readonly inboundLastSecond: number;
  /** Inbound envelopes from this connection in the last 60s window. */
  readonly inboundLastMinute: number;
  /** Outbound broadcasts originated by this connection in the last 60s. */
  readonly broadcastsLastMinute: number;
  /** Private messages originated by this connection in the last 60s. */
  readonly privatesLastMinute: number;
}

/**
 * Result of a rate-limit check.
 *
 * - `allow: true` — accept the envelope.
 * - `allow: false` — drop silently (the connection is *not* closed by
 *   meow on a single drop, to keep honest clients running).
 * - `retryAfterMs` — optional hint, surfaced to the client via the
 *   `{ kind: "throttled" }` server envelope so the client can back off.
 */
export interface RateLimitDecision {
  readonly allow: boolean;
  readonly retryAfterMs?: number;
}

const defaultRateLimitDecision = (): RateLimitDecision => ({ allow: true });

/**
 * Hook surface every meow primitive (presence, broadcast, sync, when)
 * accepts. All hooks are optional — pass only what you need.
 *
 * Hooks are *Effects*, not callbacks. That means you can:
 *
 * - read configuration with `yield* Config.string(...)`,
 * - log structured data with `Effect.logInfo`,
 * - call out to an external service with `Effect.tryPromise(...)`,
 * - chain them across hooks with `Effect.andThen(...)`,
 * - share a long-lived client with `Layer.effect(...)`.
 *
 * @section Authorisation
 * ```typescript
 * const hooks: PresenceHooks<MyState, MyMessage> = {
 *   authenticate: (request) =>
 *     Effect.gen(function* () {
 *       const token = request.headers.get("authorization")?.slice(7);
 *       if (!token) return undefined;
 *       const claims = yield* verifyJwt(token);
 *       return { id: claims.sub, name: claims.name };
 *     }),
 * };
 * ```
 *
 * @section Rate limiting
 * ```typescript
 * const hooks: PresenceHooks<MyState, MyMessage> = {
 *   rateLimit: ({ kind, counters }) =>
 *     Effect.succeed(
 *       counters.broadcastsLastMinute > 60
 *         ? { allow: false, retryAfterMs: 60_000 }
 *         : { allow: true },
 *     ),
 * };
 * ```
 *
 * @section Logging
 * ```typescript
 * const hooks: PresenceHooks<MyState, MyMessage> = {
 *   log: (event) =>
 *     Effect.gen(function* () {
 *       yield* Effect.logInfo("presence event").pipe(
 *         Effect.annotateLogs("event", event.kind),
 *       );
 *     }),
 * };
 * ```
 */
export interface PresenceHooks<TState, TMessage> {
  /**
   * Allow / deny a WebSocket upgrade. Returning `undefined` rejects
   * the connection (the upgrade is closed with 1008 "policy
   * violation"). Throw to reject with a different close code.
   */
  readonly authenticate?: (
    request: Request,
  ) => Effect.Effect<AuthContext | undefined, unknown, never>;

  /**
   * Optional seed for a connection's initial presence state. Returning
   * `undefined` leaves the initial state as `null`. Override the
   * default per-connection random colour / name pair here.
   */
  readonly initialState?: (
    request: Request,
    auth: AuthContext | undefined,
  ) => Effect.Effect<TState | undefined, never, never>;

  /**
   * Rate-limit gate for inbound client envelopes. Return
   * `{ allow: true }` to accept, `{ allow: false }` to drop (the
   * server surfaces `{ kind: "throttled", retryAfterMs }` to the
   * client but does *not* close the connection on a single drop).
   *
   * Per-envelope kind policies you might enforce:
   *
   * - `purr` — almost never limit (heartbeats).
   * - `state` — cap at ~30/sec (presence updates beyond that are
   *   indistinguishable to the human eye anyway).
   * - `message` — typically ~10/sec (chat spam protection).
   * - `private` — tightest limit (~5/sec) since DMs amplify cost.
   *
   * Default: `() => Effect.succeed({ allow: true })`.
   */
  readonly rateLimit?: (
    info: {
      readonly id: string;
      readonly kind: "state" | "message" | "private" | "purr";
      readonly authId: string | undefined;
      readonly counters: RateLimitCounters;
    },
  ) => Effect.Effect<RateLimitDecision, never, never>;

  /**
   * Fires after every accepted inbound client envelope. Use this for
   * input validation, message-rate accounting (in addition to the
   * `rateLimit` gate above), or transformation.
   */
  readonly onMessage?: (
    info: {
      readonly id: string;
      readonly kind: "state" | "message" | "private" | "purr";
      readonly bytes: number;
      readonly auth: AuthContext | undefined;
    },
  ) => Effect.Effect<void, never, never>;

  /**
   * Fires when a connection is closed (clean or not). Use for cleanup,
   * presence-tier billing, or analytics.
   */
  readonly onDisconnect?: (
    info: { readonly id: string; readonly code: number; readonly reason: string },
  ) => Effect.Effect<void, never, never>;

  /**
   * Fires for every outbound broadcast / DM. Use this for logging
   * or throttling at the egress side.
   */
  readonly onBroadcast?: (
    info: {
      readonly kind: "state" | "message" | "private";
      readonly from: string | undefined;
      readonly recipientCount: number;
      readonly bytes: number;
    },
  ) => Effect.Effect<void, never, never>;

  /**
   * Fires when an inbound envelope was dropped by `rateLimit`. meow
   * also emits a `{ kind: "throttled" }` envelope back to the client
   * (so it can back off) — this hook is for the server-side audit log.
   */
  readonly onThrottled?: (
    info: {
      readonly id: string;
      readonly kind: "state" | "message" | "private" | "purr";
      readonly retryAfterMs?: number;
    },
  ) => Effect.Effect<void, never, never>;

  /** Structured logging. Receives a tagged event for every important step. */
  readonly log?: (event: PresenceLogEvent) => Effect.Effect<void, never, never>;

  /** Telemetry. See {@link TelemetryHook}. */
  readonly telemetry?: TelemetryHook;
}

/**
 * Structured log event. Tagged for exhaustive `match` in the user's
 * log handler — adding a new variant forces every consumer to decide
 * what to do with it.
 */
export type PresenceLogEvent =
  | { readonly kind: "connect"; readonly id: string; readonly authId?: string }
  | { readonly kind: "auth-rejected"; readonly remoteAddress?: string }
  | { readonly kind: "disconnect"; readonly id: string; readonly code: number; readonly reason: string }
  | { readonly kind: "heartbeat-timeout"; readonly id: string }
  | { readonly kind: "broadcast"; readonly kind_: "state" | "message" | "private"; readonly recipients: number }
  | { readonly kind: "throttled"; readonly id: string; readonly kind_: "state" | "message" | "private" | "purr" }
  | { readonly kind: "error"; readonly cause: unknown };

/**
 * No-op rate-limit decision — accepts everything. The default when
 * `rateLimit` isn't configured.
 */
export const allow: RateLimitDecision = { allow: true };

/**
 * Helper to deny with a `retryAfterMs` hint — `{ allow: false,
 * retryAfterMs: 1000 }` reads more clearly than the literal.
 */
export const deny = (retryAfterMs?: number): RateLimitDecision => ({
  allow: false,
  retryAfterMs,
});

/**
 * Default rate-limit policy: `state` capped at 30/sec, `message` at
 * 10/sec, `private` at 5/sec, `purr` uncapped. The 1-minute window
 * allows 10x those rates in aggregate.
 *
 * Use this as the default in `Presence.make({ hooks: { rateLimit:
 * defaultRateLimit } })` — or write your own.
 */
export const defaultRateLimit = (info: {
  readonly kind: "state" | "message" | "private" | "purr";
  readonly counters: RateLimitCounters;
}): RateLimitDecision => {
  switch (info.kind) {
    case "purr":
      return allow;
    case "state":
      return info.counters.inboundLastSecond >= 30
        ? deny(1_000)
        : allow;
    case "message":
      return info.counters.inboundLastSecond >= 10
        ? deny(1_000)
        : allow;
    case "private":
      return info.counters.inboundLastSecond >= 5
        ? deny(2_000)
        : allow;
  }
};

/**
 * Server envelope sent back to the client when an inbound envelope
 * was rate-limited. Lets the client back off intelligently rather
 * than spinning.
 *
 * @internal
 */
export const throttledEnvelope = (retryAfterMs?: number) => ({
  kind: "throttled" as const,
  retryAfterMs,
});

/**
 * Shape of `{ kind: "throttled", ... }` server envelopes.
 *
 * @internal
 */
export interface ThrottledServerEnvelope {
  readonly kind: "throttled";
  readonly retryAfterMs?: number;
}

// ---------------------------------------------------------------------------
// Service tag — provides hooks to handlers
// ---------------------------------------------------------------------------

/**
 * Context tag carrying the user's hook set. Build with `Layer.succeed`:
 *
 * ```typescript
 * const MyHooksLive = Layer.succeed(PresenceHooksService, {
 *   authenticate: ...,
 *   rateLimit: defaultRateLimit,
 *   log: (e) => Effect.logInfo("presence", e),
 * });
 * ```
 */
export const PresenceHooksService =
  Context.Service<PresenceHooks<unknown, unknown>>("meow/PresenceHooks");

/**
 * Helper Layer that exposes a no-op hook set. Useful as the base for
 * environments that don't need hooks (tests, dev sandboxes):
 *
 * ```typescript
 * const RoomLive = Room.make(...).pipe(Layer.provide(noopHooksLayer));
 * ```
 */
export const noopHooksLayer = Layer.succeed(PresenceHooksService, {
  // Empty — every hook member is undefined, so meow's `yield* hooks?.x?.()`
  // calls short-circuit cleanly.
} satisfies PresenceHooks<never, never>);

// Import Layer late so it doesn't pollute the top-level namespace.
import * as Layer from "effect/Layer";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * Marker type that prevents `Request` from leaking the `Body` stream
 * into typed RPC error unions. Used by `authenticate` so the failure
 * type stays `unknown` rather than `HttpServerError`.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface AuthError extends HttpServerError {}

export const isAuthError = (e: unknown): e is AuthError =>
  e instanceof Error && e.name === "AuthError";

export const reject = (reason: string): Effect.Effect<never, AuthError, never> =>
  Effect.fail(new Error(reason) as AuthError);