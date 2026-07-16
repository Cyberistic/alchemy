/**
 * `meow` — realtime primitives for Cloudflare Workers, named after cats.
 *
 * Currently shipping:
 *
 * - `meow/core` — wire protocol, purr heartbeat, hook system, attachment
 *   and routing helpers. Distilled-only (no Alchemy dep).
 * - `meow/alchemy` — convenience wrapper that exposes the same primitives
 *   to projects already using Alchemy Effect.
 * - `meow/presence` — rooms with WebSocket hibernation, typed presence
 *   state, peer map, broadcast, private messaging, and a heartbeat
 *   (`purr`) that keeps both sides honest.
 * - `meow/presence/client` — browser WebSocket client with auto-reconnect
 *   and purr watchdog.
 * - `meow/presence/hooks` — `usePresence` React hook.
 * - `meow/broadcast` — N-shard topic-based pub/sub on Cloudflare DOs.
 * - `meow/sync` — git-style state sync (SQLite / PGlite / TanStack DB).
 * - `meow/client` — extracted browser client with cursor smoothing helpers.
 */
export * as Core from "./core/index.ts";
export * as Alchemy from "./alchemy/index.ts";
export * as Presence from "./presence/index.ts";
export * as Broadcast from "./broadcast/index.ts";
export * as Sync from "./sync/index.ts";
export * as Client from "./client/index.ts";