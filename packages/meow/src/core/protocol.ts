/**
 * Per-connection presence state, persisted via `serializeAttachment` so it
 * survives Durable Object hibernation. JSON-serialisable.
 *
 * @internal
 */
export interface PresenceAttachment<TState> {
  readonly id: string;
  state: TState;
  /** ms since epoch when this connection last sent a heartbeat (server-side). */
  readonly lastPurr?: number;
  /**
   * Auth-id from the `authenticate` hook, if any. Used to route private
   * messages to the right recipient without exposing the connection's
   * random uuid to the sender.
   */
  readonly authId?: string;
}

/**
 * Envelope a browser sends to the room.
 *
 * - `state` — replace this connection's presence state and broadcast the new
 *   value to every other connection.
 * - `message` — relay an arbitrary JSON payload to every other connection.
 *   The server does not interpret `data`. Pass `id` if you want server
 *   ack-tracking for outbox reconciliation.
 * - `private` — send an arbitrary JSON payload to **one** peer only (the
 *   peer whose `authId` matches `to`). The server never broadcasts a
 *   private envelope to the room. Pass `id` for outbox ack-tracking.
 * - `purr` — heartbeat ping. The server replies with a matching `purr`
 *   envelope; absence of replies within the configured timeout is treated
 *   as a dead connection (and the connection is closed on the server).
 */
export type PresenceClientEnvelope<TState, TMessage> =
  | { readonly kind: "state"; readonly state: TState }
  | {
      readonly kind: "message";
      readonly data: TMessage;
      /** Optional client-generated id, echoed back in `{ kind: "ack" }`. */
      readonly id?: string;
    }
  | {
      readonly kind: "private";
      readonly to: string;
      readonly data: TMessage;
      readonly id?: string;
    }
  | { readonly kind: "purr" };

/**
 * Envelope the room sends back.
 *
 * - `init` — exactly once on connect; `self` is this connection's id and
 *   `peers` is a `{ id → state }` map of every other connection in the room.
 * - `presence` — a peer's state was set (on join or in response to a `state`).
 * - `absence` — a peer has disconnected.
 * - `message` — a peer relayed an arbitrary message.
 * - `private` — a peer relayed a private message targeted at this
 *   connection (only ever arrives on the connection whose `authId` is
 *   `to`).
 * - `ack` — the server received your `message` / `private` envelope
 *   with the matching `id`. Use this to mark outbox entries `sent`.
 * - `throttled` — the server's `rateLimit` hook rejected your last
 *   envelope. Back off and retry after `retryAfterMs` if provided.
 * - `purr` — heartbeat pong. Sent on receipt of a client `purr` envelope.
 */
export type PresenceServerEnvelope<TState, TMessage> =
  | {
      readonly kind: "init";
      readonly self: string;
      readonly peers: Readonly<Record<string, TState>>;
    }
  | { readonly kind: "presence"; readonly id: string; readonly state: TState }
  | { readonly kind: "absence"; readonly id: string }
  | { readonly kind: "message"; readonly from: string; readonly data: TMessage }
  | {
      readonly kind: "private";
      readonly from: string;
      readonly data: TMessage;
    }
  | { readonly kind: "ack"; readonly id: string }
  | {
      readonly kind: "throttled";
      readonly retryAfterMs?: number;
    }
  | { readonly kind: "purr" };

/**
 * Schemaless RPC methods every Worker / DO gets when it binds a Presence
 * room. Methods are typed stubs — no JSON, no schema, just Effects.
 */
export interface PresenceRpc<TState, TMessage> {
  /** Number of currently-connected WebSockets in this room. */
  readonly count: () => import("effect/Effect").Effect<number>;
  /** Ids of every currently-connected WebSocket in this room. */
  readonly connections: () => import("effect/Effect").Effect<readonly string[]>;
  /**
   * Auth-ids of every connection in the room. Useful for picking a
   * recipient when sending a private message — see {@link sendPrivate}.
   */
  readonly authIds: () => import("effect/Effect").Effect<readonly string[]>;
  /** Read the persisted presence state of one connection. */
  readonly getState: (
    id: string,
  ) => import("effect/Effect").Effect<TState | undefined>;
  /**
   * Broadcast an arbitrary message to every connected client. Server tags
   * outgoing envelopes with `{ kind: "message", from: "*", data }`.
   */
  readonly broadcast: (data: TMessage) => import("effect/Effect").Effect<void>;
  /**
   * Send a private message to every connection whose `authId` is
   * `to`. Dropped silently if no such connection exists. Returned
   * Effect resolves to the number of recipients reached.
   */
  readonly sendPrivate: (
    to: string,
    data: TMessage,
    from?: string,
  ) => import("effect/Effect").Effect<number>;
  /** Forcibly close one connection. Idempotent. */
  readonly kick: (
    id: string,
    code?: number,
    reason?: string,
  ) => import("effect/Effect").Effect<void>;
  /**
   * Replace the persisted presence state for one connection (e.g. from a
   * Worker after an auth change) and broadcast the update to every other
   * client.
   */
  readonly setState: (
    id: string,
    state: TState,
  ) => import("effect/Effect").Effect<void>;
}