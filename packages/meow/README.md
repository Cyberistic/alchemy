# meow 🐱

Realtime primitives for Cloudflare Workers, named after cats.

- **`meow/presence`** — rooms with WebSocket hibernation,
  typed presence state, peer map, broadcast, and a heartbeat (`purr`) that
  keeps both sides honest about which connections are alive.
- **`meow/sync`** — _coming soon_: LiveStore-style CRDT-backed document
  sync on top of `meow/presence`.

## Why a separate package?

`meow` ships with only the runtime pieces you need to add realtime to a
Cloudflare Worker. It uses the [Alchemy](https://alchemy.run) Effect
framework for the Durable Object + Worker plumbing — schemaless RPC for
the control plane, WebSocket hibernation for the data plane — but
deliberately doesn't pull in the rest of the IaC engine. The eventual
goal is to depend on `@distilled.cloud/cloudflare` alone.

## Install

```bash
bun add meow alchemy effect
```

## Quick start — `meow/presence`

### 1. Define a room

```typescript
// src/room.ts
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import { Presence } from "meow/presence";

export interface CursorState {
  cursor: { x: number; y: number } | null;
  name: string;
  color: string;
}

export class Room extends Alchemy.DurableObject<Room>()(
  "Room",
  Presence.make<CursorState, never>({
    initialState: (req) =>
      Effect.succeed({
        cursor: null,
        name: new URL(req.url).searchParams.get("name") ?? "anon",
        color: new URL(req.url).searchParams.get("color") ?? "#888",
      }),
  }),
) {}
```

### 2. Forward browser WebSockets to the room

```typescript
// src/worker.ts
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import { Presence } from "meow/presence";
import Room from "./room.ts";

export default Alchemy.Worker<Worker, {}, Room>()(
  "Worker",
  { main: import.meta.url },
  Effect.gen(function* () {
    return {
      fetch: Presence.forward({
        roomClass: Room,
        room: (req) =>
          new URL(req.url).pathname.split("/")[2] ?? "lobby",
      }),
    };
  }),
);
```

### 3. Connect from the browser

```typescript
import { PresenceClient, presenceUrl } from "meow/presence/client";

const client = new PresenceClient<CursorState, never>({
  url: presenceUrl({
    host: window.location.host,
    class: "Room",
    room: "lobby",
    protocol: window.location.protocol === "https:" ? "wss" : "ws",
  }),
});

client.connect();

window.addEventListener("mousemove", (e) =>
  client.update({
    cursor: { x: e.clientX, y: e.clientY },
    name: "alice",
    color: "#888",
  }),
);

const unsub = client.subscribe((peers) => {
  for (const [id, state] of peers) {
    if (state.cursor) renderCursor(id, state);
  }
});

// Cleanup:
window.addEventListener("beforeunload", () => client.close());
```

### 4. Or use the React hook

```tsx
import { usePresence } from "meow/presence/hooks";

export function Cursors({ room }: { room: string }) {
  const { peers, update } = usePresence<CursorState, never>({
    url: presenceUrl({ host: window.location.host, class: "Room", room }),
  });

  useEffect(() => {
    const onMove = (e: MouseEvent) =>
      update({ cursor: { x: e.clientX, y: e.clientY }, name: "alice", color: "#888" });
    window.addEventListener("mousemove", onMove);
    return () => window.removeEventListener("mousemove", onMove);
  }, [update]);

  return (
    <>
      {[...peers.entries()].map(([id, s]) => (
        s.cursor && <Cursor key={id} x={s.cursor.x} y={s.cursor.y} color={s.color} />
      ))}
    </>
  );
}
```

## purr — heartbeat

Cats purr when they're content. Connections purr when they're alive.

By default a Presence room does **not** run heartbeats — connections stay
open until they close themselves or are kicked via RPC. That's fine for
cursors that change on every mousemove, terrible for anything that
lingers (presence, sessions, locks, "who's viewing this document").

Enable heartbeats on both sides to keep the peer map honest:

```typescript
// Server: room DO
Presence.make<MyState, never>({
  heartbeat: {
    interval: "30 seconds",  // server checks this often
    timeout: "45 seconds",   // ...and kicks anything older
  },
});

// Client: PresenceClient
new PresenceClient<MyState, never>({
  url,
  heartbeat: {
    interval: "25 seconds",  // client pings slightly faster than the
    timeout: "35 seconds",   // server's check so it sees the pong first
  },
});
```

When the client misses two purrs (timeout exceeded), it closes the
socket and lets its reconnect loop kick in. When the server notices a
stale connection, it `kick`s with close code 1001 and reason `"purr
timeout"` and broadcasts an `absence` envelope.

### Wire protocol (purr envelopes)

```typescript
// client → server
{ kind: "purr" }

// server → client (echoed immediately on receipt)
{ kind: "purr" }
```

## Wire protocol (full)

```typescript
// client → server
{ kind: "state",   state: TState }                  // replace own state, broadcast
{ kind: "message", data: TMessage }                 // relay arbitrary JSON
{ kind: "purr" }                                    // heartbeat ping

// server → client
{ kind: "init",     self: string, peers: { id: TState } }   // once on connect
{ kind: "presence", id: string, state: TState }             // peer joined or updated
{ kind: "absence",  id: string }                           // peer disconnected
{ kind: "message",  from: string, data: TMessage }          // relayed message
{ kind: "purr" }                                            // heartbeat pong
```

## Control plane (schemaless RPC)

Bind the room from another Worker / DO and call typed RPC methods:

```typescript
const room = Room.from(Worker);            // local binding
const lobby = yield* room.getByName("lobby");

yield* lobby.broadcast({ text: "server hello" });
yield* lobby.count();                      // Effect<number>
yield* lobby.connections();                // Effect<readonly string[]>
yield* lobby.getState("xyz789");           // Effect<CursorState | undefined>
yield* lobby.setState("xyz789", { ... });  // Effect<void>
yield* lobby.kick("xyz789");               // Effect<void>
```

## API surface

| Path | Description |
| --- | --- |
| `meow` | Top-level: re-exports `Presence` and `Sync` namespaces |
| `meow/presence` | Server (`make`, `forward`) + browser (`PresenceClient`) + React hook |
| `meow/presence/client` | `PresenceClient` + `presenceUrl` (browser only) |
| `meow/presence/hooks` | `usePresence` React hook (browser only) |
| `meow/sync` | _coming soon_ |

## Roadmap

- [x] `Presence.make<TState, TMessage>(options?)`
- [x] `Presence.forward({ roomClass, room, fallback? })`
- [x] `PresenceClient<TState, TMessage>` browser client
- [x] `usePresence<TState, TMessage>(options)` React hook
- [x] `purr` heartbeat on both sides
- [ ] Reconnect resilience — currently exponential backoff with jitter
- [ ] Cross-script binding helpers (`Presence.from(hostWorker)` shorthand)
- [ ] `meow/sync` CRDT-backed document sync
- [ ] Drop the `alchemy` dependency — depend only on `@distilled.cloud/cloudflare` and `effect`