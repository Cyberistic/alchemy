/**
 * Connection lifecycle state. Exposed both at the {@link PresenceClient}
 * level (as `.status`) and to React consumers (via `useConnectionStatus`).
 *
 * - `syncing` — socket is open but we haven't received the server's
 *   `init` envelope yet. Typically lasts a single round-trip.
 * - `online` — connected and ready. Outbound envelopes will be
 *   delivered; inbound envelopes will arrive.
 * - `reconnecting` — socket closed, auto-reconnect in progress.
 * - `offline` — closed and no auto-reconnect (or all retries exhausted).
 */
export type ConnectionStatus =
  | "syncing"
  | "online"
  | "reconnecting"
  | "offline";

/**
 * Lifecycle of an outbound message tracked by the outbox.
 *
 * - `pending` — added to the outbox but the socket isn't open yet. Will
 *   be flushed on reconnect.
 * - `sending` — passed to `WebSocket.send()`. Will move to `sent` as
 *   soon as the underlying socket reports it left the buffer, or to
 *   `failed` if `send` throws.
 * - `sent` — the server has acked (received the matching `id`).
 *   Consumers can render ✓ / "Sent".
 * - `failed` — `send` threw, the socket closed mid-send, or the server
 *   sent a `{ kind: "throttled" }` response. Consumers should show a
 *   retry button.
 * - `cancelled` — user explicitly dropped the message (rare).
 */
export type MessageStatus =
  | "pending"
  | "sending"
  | "sent"
  | "failed"
  | "cancelled";

/**
 * A tracked outbound message.
 *
 * `id` is the client-generated UUID. `data` is whatever the user
 * passed to `client.send(data)` — preserved verbatim so retry can
 * resend exactly the same bytes.
 *
 * `attempts` is bumped on every retry so the UI can show "Retry #2"
 * or rate-limit on the client side.
 */
export interface MessageRef<T> {
  readonly id: string;
  readonly data: T;
  readonly status: MessageStatus;
  readonly attempts: number;
  readonly queuedAt: number;
  readonly sentAt?: number;
  readonly ackedAt?: number;
  readonly failedAt?: number;
  readonly lastError?: string;
}