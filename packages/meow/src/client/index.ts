/**
 * `meow/client` — the standalone browser-side client (auto-reconnecting WebSocket + smoothing helpers)
 * with generic shape interpolation helpers.
 *
 * ```typescript
 * import { PresenceClient, smoothed, lerpVec2 } from "meow/client";
 *
 * const client = new PresenceClient({ host: "wss://...", room: "lobby" });
 *
 * type Cursor = { x: number; y: number };
 * const myCursor = smoothed<Cursor>({
 *   initial: { x: 0, y: 0 },
 *   interpolate: lerpVec2,
 *   duration: 80,
 * });
 *
 * client.peers.subscribe((peers) => {
 *   for (const [id, peer] of Object.entries(peers)) {
 *     // per-peer smoother...
 *   }
 * });
 * ```
 */
export {
  PresenceClient,
  presenceUrl,
  type PresenceClientOptions,
  type PresenceConnectionState,
  type PresencePeer,
} from "../presence/Client.ts";

export {
  type ConnectionStatus,
  type MessageRef,
  type MessageStatus,
} from "./status.ts";

export {
  type Vec2,
  type Lerp,
  type FieldInterpolators,
  type Smoothed,
  type SmoothedOptions,
  type PeerSmoothers,
  easeOutCubic,
  linear,
  lerpNumber,
  lerpVec2,
  lerpAngle,
  snapLerp,
  smoothed,
  smoothedCursor,
  peerSmoothers,
} from "./smooth.ts";

export const SMOOTH_VERSION = "0.2.0" as const;
export const CLIENT_VERSION = "0.2.0" as const;