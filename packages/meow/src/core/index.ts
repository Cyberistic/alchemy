/**
 * `meow/core` — shared primitives every meow library consumes.
 *
 * Distilled-only (depends on `effect` and `@distilled.cloud/cloudflare`
 * type defs, no Alchemy) so any DO implementation — Alchemy-flavored,
 * vanilla workerd, even a different cloud — can pull from this and
 * skip re-implementing the wire protocol.
 *
 * What's in here:
 *
 * - `protocol.ts` — wire envelope types (`PresenceClientEnvelope`,
 *   `PresenceServerEnvelope`, `PresencePrivateEnvelope`).
 * - `purr.ts` — heartbeat helpers (`HeartbeatOptions`, `isStale`,
 *   `resolveServerHeartbeat`, `resolveClientHeartbeat`).
 * - `attachment.ts` — typed cf.WebSocket adapter and JSON envelope
 *   encode / decode helpers used by every server-side library.
 * - `routes.ts` — private-message routing tables (connection id ⇄
 *   auth id).
 * - `hooks.ts` — typed hook surface for `auth`, `log`, `telemetry`,
 *   and lifecycle events.
 */
export * from "./protocol.ts";
export * from "./purr.ts";
export * from "./attachment.ts";
export * from "./routes.ts";
export * from "./hooks.ts";