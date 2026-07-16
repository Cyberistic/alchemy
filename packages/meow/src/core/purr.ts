import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

/**
 * Heartbeat options for a Presence room.
 *
 * Heartbeats (the cat's `purr`) keep both sides honest about which
 * connections are still alive. Without them, a tab that closes without
 * a graceful WebSocket close stays in the peer's map until the next state
 * update overwrites it — fine for cursors that change constantly, terrible
 * for any state that lingers (presence, sessions, locks).
 *
 * @section Server-side
 * `Presence.make({ heartbeat: { interval: "30 seconds" } })` tracks a
 * `lastPurr` timestamp per connection in the WebSocket attachment. Every
 * `interval`, any connection whose `lastPurr` is older than `timeout` is
 * `kick`ed with a 1001 "purr timeout" close code. Idle connections that
 * haven't received any message yet fall back to the connection's own
 * `lastSeen` (initialised on connect).
 *
 * @section Client-side
 * `new PresenceClient({ heartbeat: { interval: "25 seconds" } })` sends a
 * `{ kind: "purr" }` envelope every `interval`. If no `{ kind: "purr" }`
 * reply arrives within `timeout`, the client treats the connection as
 * dead and forces a reconnect — never waiting on TCP keepalives that
 * browsers don't tune well for Cloudflare's edge.
 *
 * @section Defaults
 * With no `heartbeat` option, no heartbeats run. Connections live until
 * they close themselves or are kicked by RPC.
 */
export interface HeartbeatOptions {
  /**
   * Interval between heartbeats. Defaults to `30 seconds` server-side,
   * `25 seconds` client-side (offset so the client pings before the server
   * would otherwise declare the connection stale).
   */
  readonly interval?: string;
  /**
   * How long a connection can go without a heartbeat before it's declared
   * dead. Defaults to `45 seconds` (server) / `35 seconds` (client).
   * Must be greater than `interval`.
   */
  readonly timeout?: string;
}

export interface HeartbeatConfig {
  readonly interval: Duration.Duration;
  readonly timeout: Duration.Duration;
}

import * as Duration from "effect/Duration";

const parseDuration = (
  s: string | undefined,
  fallback: Duration.Duration,
): Duration.Duration => {
  if (!s) return fallback;
  // Try parsing as a human-readable string ("30 seconds", "1 minute").
  const match = s.match(/^(\d+(?:\.\d+)?)\s*(millis?|seconds?|minutes?|hours?)$/i);
  if (match) {
    const n = Number(match[1]);
    const unit = match[2].toLowerCase();
    if (unit.startsWith("milli")) return Duration.millis(n);
    if (unit.startsWith("second")) return Duration.seconds(n);
    if (unit.startsWith("minute")) return Duration.minutes(n);
    if (unit.startsWith("hour")) return Duration.hours(n);
  }
  // Plain number → milliseconds.
  const asNumber = Number(s);
  if (!Number.isNaN(asNumber)) return Duration.millis(asNumber);
  return fallback;
};

export const resolveServerHeartbeat = (
  options?: HeartbeatOptions,
): HeartbeatConfig => ({
  interval: parseDuration(
    options?.interval,
    Duration.seconds(30),
  ),
  timeout: parseDuration(
    options?.timeout,
    Duration.seconds(45),
  ),
});

export const resolveClientHeartbeat = (
  options?: HeartbeatOptions,
): HeartbeatConfig => ({
  interval: parseDuration(
    options?.interval,
    Duration.seconds(25),
  ),
  timeout: parseDuration(
    options?.timeout,
    Duration.seconds(35),
  ),
});

/**
 * Build an Effect that periodically checks for stale connections.
 *
 * `onCheck` runs every `interval`; pass a closure that walks the active
 * connection map and kicks anyone whose `lastPurr` is older than
 * `timeout`. Used internally by `Presence.make`; exported for advanced
 * cases where you want to run the heartbeat on your own schedule (e.g.
 * piggy-backing on `alarm` instead of `setInterval`).
 *
 * @example Custom heartbeat with logging
 * ```typescript
 * Effect.gen(function* () {
 *   yield* purrLoop({
 *     interval: Duration.seconds(15),
 *     timeout: Duration.seconds(45),
 *     onCheck: (now) =>
 *       Effect.gen(function* () {
 *         for (const [id, lastSeen] of connections) {
 *           if (now - lastSeen > 45_000) {
 *             yield* Effect.logWarning(`kicking stale connection ${id}`);
 *             yield* kick(id, 1001, "purr timeout");
 *           }
 *         }
 *       }),
 *   });
 * });
 * ```
 */
export const purrLoop = (config: {
  readonly interval: Duration.Duration;
  readonly timeout: Duration.Duration;
  readonly onCheck: (now: number) => Effect.Effect<void, never, never>;
}): Effect.Effect<void, never, never> =>
  Effect.gen(function* () {
    yield* Effect.logDebug(
      `purr: starting heartbeat (interval=${Duration.toMillis(config.interval)}ms, timeout=${Duration.toMillis(config.timeout)}ms)`,
    );
    const sweep = Effect.sync(() => Date.now()).pipe(
      Effect.flatMap((now) => config.onCheck(now)),
    );
    yield* sweep.pipe(
      Effect.repeat({ schedule: Schedule.spaced(config.interval) }),
    );
  });

/**
 * Update a connection's `lastPurr` timestamp to `now`.
 */
export const touchLastPurr = (now: number = Date.now()): { lastPurr: number } => ({
  lastPurr: now,
});

/**
 * Whether `lastSeen` is older than the heartbeat timeout.
 */
export const isStale = (
  lastSeen: number | undefined,
  timeoutMs: number,
  now: number = Date.now(),
): boolean => lastSeen === undefined || now - lastSeen > timeoutMs;