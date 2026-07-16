/**
 * WebSocket-bearing WebSocket-like wrapper around `cf.WebSocket` from
 * `@cloudflare/workers-types`. The DO bridges receives a `cf.WebSocket`,
 * but the rest of meow operates on this typed wrapper so it can be unit
 * tested without spinning up a real workerd isolate.
 *
 * `attachment` is a serializable per-connection blob written through
 * `serializeAttachment`. meow uses it to track each connection's
 * presence state across hibernation.
 *
 * @internal
 */
export interface MeowSocket<TAttachment = unknown> {
  readonly raw: CfWebSocket;
  send(data: string | ArrayBuffer | Uint8Array): void;
  close(code?: number, reason?: string): void;
  serializeAttachment<T>(value: T): void;
  deserializeAttachment<T>(): T | null;
  readonly attachment: TAttachment;
}

/** Minimal Cloudflare WebSocket surface — the only methods meow needs. */
export interface CfWebSocket {
  send(data: string | ArrayBuffer | Uint8Array): void;
  close(code?: number, reason?: string): void;
  serializeAttachment(attachment: unknown): void;
  deserializeAttachment(): unknown;
}

/**
 * Minimal cf.WebSocket adapter — the only surface meow touches. The
 * alchemy / workerd bridges wrap the real socket with this shape.
 */
export const fromCfWebSocket = <TAttachment = unknown>(
  ws: any,
  defaultAttachment: TAttachment,
): MeowSocket<TAttachment> => ({
  raw: ws,
  send: (data) => ws.send(data),
  close: (code = 1000, reason) => ws.close(code, reason),
  serializeAttachment: <T>(value: T) => ws.serializeAttachment(value),
  deserializeAttachment: <T>() => ws.deserializeAttachment() as T | null,
  attachment: defaultAttachment,
});

/**
 * Build a connection-id from the durable object name (the room id) +
 * a random per-connection suffix. Persists across hibernation via
 * the underlying cf.WebSocket's serializeAttachment.
 *
 * Pure helper, exported so tests can use it without a real socket.
 */
export const generateConnectionId = (): string =>
  globalThis.crypto.randomUUID();

/**
 * Encode a JSON-serialisable envelope to a string. Helpers around
 * `JSON.stringify` exist so we can swap in a schema codec later (e.g.
 * MessagePack, Effect Schema) without touching every call site.
 */
export const encodeEnvelope = (value: unknown): string =>
  JSON.stringify(value);

/**
 * Decode a JSON envelope. Returns `null` on malformed input so callers
 * can drop the message and move on.
 */
export const decodeEnvelope = <T>(data: string | ArrayBuffer | Uint8Array): T | null => {
  const text =
    typeof data === "string"
      ? data
      : new TextDecoder().decode(
          data instanceof ArrayBuffer ? new Uint8Array(data) : data,
        );
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
};