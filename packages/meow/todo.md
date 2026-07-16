# meow — Roadmap

Realtime primitives for Cloudflare Workers, named after cats.
Built on top of [Alchemy Effect](https://alchemy.run)'s schemaless RPC + Effect,
and targeted at eventual standalone publication on `@distilled.cloud/cloudflare`
alone.

## Goals

1. **Standalone package** with subpath exports: `meow`, `meow/core`,
   `meow/alchemy`, `meow/presence`, `meow/broadcast`, `meow/sync`,
   `meow/client`.
2. **No hard dependency on the full Alchemy IaC framework** —
   `meow/core` and all `meow/*` primitives depend only on
   `@distilled.cloud/cloudflare` + `effect`. `meow/alchemy` is an
   optional integration layer for projects already using Alchemy.
3. **Hookable everywhere** — every library exposes a typed hook system
   for `auth`, `logging`, `telemetry`, etc. Hooks are simple Effect
   functions the user plugs in once at the resource layer.
4. **First-class private messaging** — `presence` and `broadcast`
   distinguish "room broadcast" from "peer-to-peer DM" via a typed
   routing layer.
5. **Smoothed presence by default** — `meow/client` ships easing
   helpers for cursor / position interpolation so consumers don't have
   to reach for `perfect-cursors`.

## Packages

| Path | Status | Depends on | Notes |
| --- | --- | --- | --- |
| `meow/core` | done | `effect`, `@distilled.cloud/cloudflare` | Wire protocol, purr heartbeat, hook system, presence attachment helpers |
| `meow/alchemy` | done | `meow/core`, `alchemy` | `meow.alchemy()` provider — registers all DO classes via the Alchemy DO factory |
| `meow/presence` | done | `meow/core`, `alchemy` (transitively via `meow/alchemy`) | Rooms: hibernation, peer map, broadcast, RPC |
| `meow/broadcast` | in progress | `meow/core` | N-shard pub/sub with topic-based fan-out |
| `meow/sync` | done | `meow/core` | Git-style state sync with SQLite / PGlite / TanStack DB backends |
| `meow/client` | done | `meow/core` | Auto-reconnecting browser client + cursor smoothing |

For `meow/when` (scheduling): we use Cloudflare's native cron triggers
via the workerd cron trigger handler — no need for a custom scheduler.