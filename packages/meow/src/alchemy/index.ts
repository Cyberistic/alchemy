/**
 * `meow/alchemy` — convenience layer for projects already using
 * [Alchemy Effect](https://alchemy.run).
 *
 * Re-exports `meow/presence`'s Alchemy-flavored factory so it lives
 * next to the rest of the Alchemy provider tree. Importing this
 * package pulls in `alchemy` as a transitive dependency — `meow/core`
 * does not.
 *
 * ```typescript
 * import * as Meow from "meow/alchemy";
 * import * as Cloudflare from "alchemy/Cloudflare";
 *
 * class Room extends Cloudflare.DurableObject<Room>()(
 *   "Room",
 *   Meow.Presence.make<MyState, MyMessage>({ heartbeat: {...} }),
 * ) {}
 * ```
 */
export * as Presence from "../presence/index.ts";
export * as Core from "../core/index.ts";