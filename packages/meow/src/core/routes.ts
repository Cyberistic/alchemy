/**
 * Private messaging — routing layer.
 *
 * Presence's room model is "broadcast to everyone in the same DO
 * instance". Sometimes you want to DM a single peer — typing
 * indicators, mentions, draft sharing — without leaking the message
 * to the rest of the room. That's what {@link PresenceEnvelope.private}
 * is for.
 *
 * The server keeps a small in-memory table mapping `connectionId → authId`
 * (whatever the `authenticate` hook returned), and a reverse
 * `authId → connectionId[]` so a sender can target a peer by `authId`
 * without having to learn the peer's random connection UUID.
 *
 * Both maps survive hibernation because they're written into the
 * Durable Object's `cf.DurableObjectState` storage — they ride out
 * evictions just like the connection attachments do.
 */

import type { AuthContext } from "./hooks.ts";

/**
 * In-memory view of the room's private-routing tables. Rebuilt on
 * hibernation from the persisted maps in DO storage.
 *
 * @internal
 */
export interface PrivateRouteTable {
  /** All connections currently in the room, by connection id. */
  readonly connectionsById: ReadonlyMap<string, AuthContext | undefined>;
  /** Reverse index — many connections may share the same auth id (multi-tab). */
  readonly connectionsByAuth: ReadonlyMap<string, readonly string[]>;
}

/**
 * Register a connection. Updates both forward and reverse indices.
 */
export const registerConnection = (
  table: PrivateRouteTable,
  connectionId: string,
  auth: AuthContext | undefined,
): PrivateRouteTable => {
  const connectionsById: Map<string, AuthContext | undefined> = new Map(
    table.connectionsById,
  );
  const connectionsByAuth: Map<string, string[]> = new Map();
  for (const [k, v] of table.connectionsByAuth) {
    connectionsByAuth.set(k, [...v]);
  }
  connectionsById.set(connectionId, auth);
  if (auth) {
    const existing = connectionsByAuth.get(auth.id) ?? [];
    connectionsByAuth.set(auth.id, [...existing, connectionId]);
  }
  return { connectionsById, connectionsByAuth };
};

/**
 * Deregister a connection. Returns the updated table and the auth
 * context that was attached (if any), so callers can emit a
 * `disconnect` log event without re-reading the attachment.
 */
export const deregisterConnection = (
  table: PrivateRouteTable,
  connectionId: string,
): { table: PrivateRouteTable; auth: AuthContext | undefined } => {
  const auth = table.connectionsById.get(connectionId);
  const connectionsById: Map<string, AuthContext | undefined> = new Map(
    table.connectionsById,
  );
  connectionsById.delete(connectionId);
  const connectionsByAuth: Map<string, string[]> = new Map();
  for (const [k, v] of table.connectionsByAuth) {
    connectionsByAuth.set(k, [...v]);
  }
  if (auth) {
    const list = connectionsByAuth.get(auth.id) ?? [];
    const next = list.filter((id) => id !== connectionId);
    if (next.length > 0) {
      connectionsByAuth.set(auth.id, next);
    } else {
      connectionsByAuth.delete(auth.id);
    }
  }
  return { table: { connectionsById, connectionsByAuth }, auth };
};

/**
 * Resolve `authId → [connectionId, ...]`. Returns an empty array if
 * the peer isn't in the room.
 */
export const resolveAuthId = (
  table: PrivateRouteTable,
  authId: string,
): readonly string[] => table.connectionsByAuth.get(authId) ?? [];

/**
 * Resolve `connectionId → auth`. Returns `undefined` if the connection
 * isn't registered (e.g. it disconnected between when the server
 * started routing a private message and when delivery happened).
 */
export const resolveConnection = (
  table: PrivateRouteTable,
  connectionId: string,
): AuthContext | undefined => table.connectionsById.get(connectionId);