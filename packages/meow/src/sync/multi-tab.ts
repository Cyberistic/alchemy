import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import type { SyncClient } from "./client.ts";
import type { Event } from "./schema.ts";

/**
 * Multi-tab coordination for {@link SyncClient}.
 *
 * When many tabs of the same app are open, each opening its own
 * WebSocket wastes server resources and amplifies per-event broadcast
 * traffic. This module elects **one leader** per `channel`; the
 * leader owns the WebSocket, and followers receive events via the
 * browser's `BroadcastChannel` API.
 *
 * The protocol is a tiny ping-pong:
 *
 * - **Leader → followers**: every accepted remote event is forwarded
 *   on the channel as `{ kind: "event", event }`.
 * - **Follower → leader**: on connect, a follower announces itself;
 *   the leader responds with its current HEAD so the follower can
 *   catch up via a `pull`.
 * - **Heartbeats**: leaders emit a `heartbeat` every `heartbeatMs`;
 *   if a follower doesn't see one for `leaseMs`, it triggers a new
 *   election.
 *
 * @example
 * ```typescript
 * import { MultiTab } from "meow/sync/multi-tab";
 *
 * const client = new SyncClient({ url: "wss://...", store });
 * const coord = MultiTab.create({
 *   channel: "my-app-sync",
 *   client,
 *   heartbeatMs: 2_000,
 *   leaseMs: 6_000,
 * });
 * // Read `coord.live` like a normal Stream:
 * Stream.runForEach(coord.live, (e) => Effect.log(e.event));
 * ```
 */
export interface MultiTabOptions {
  /**
   * Channel name. Tabs that share a channel coordinate together.
   * Distinct apps (or distinct documents within an app) should use
   * distinct channel names — e.g. `${userId}/${docId}`.
   */
  readonly channel: string;
  /**
   * The {@link SyncClient} the leader uses. Followers receive events
   * over the BroadcastChannel without opening their own WebSocket;
   * if a follower needs to push, it can construct a separate
   * client and bypass the coordinator.
   */
  readonly client: SyncClient;
  /** Leader heartbeat interval (ms). Default `2000`. */
  readonly heartbeatMs?: number;
  /**
   * Lease timeout (ms). If a follower doesn't see a heartbeat within
   * this window, it considers the leader dead and starts a new
   * election. Default `6000`.
   */
  readonly leaseMs?: number;
  /** Override the BroadcastChannel constructor (for tests). */
  readonly BroadcastChannel?: typeof BroadcastChannel;
  /** Override the randomUUID used to mint tab ids (for tests). */
  readonly randomUUID?: () => string;
  /** Override `performance.now()` (for tests). */
  readonly now?: () => number;
}

interface BroadcastEventMsg {
  readonly kind: "event";
  readonly event: Event;
}
interface BroadcastHelloMsg {
  readonly kind: "hello";
  readonly tabId: string;
}
interface BroadcastHeartbeatMsg {
  readonly kind: "heartbeat";
  readonly tabId: string;
  readonly head: number;
  readonly at: number;
}
interface BroadcastHeadMsg {
  readonly kind: "head";
  readonly tabId: string;
  readonly head: number;
}
interface BroadcastGoodbyeMsg {
  readonly kind: "goodbye";
  readonly tabId: string;
}
type BroadcastMessage =
  | BroadcastEventMsg
  | BroadcastHelloMsg
  | BroadcastHeartbeatMsg
  | BroadcastHeadMsg
  | BroadcastGoodbyeMsg;

export interface MultiTabCoordinator {
  /**
   * Reactive stream of remote events. Identical shape to
   * `SyncClient.live` but only includes events from the leader's
   * WebSocket. Followers can read this without opening their own.
   */
  readonly live: Stream.Stream<Event, never, never>;
  /** True iff this tab currently holds the leader role. */
  readonly isLeader: () => boolean;
  /** Head sequence number as last reported by the leader. */
  readonly head: () => number;
  /** Stop coordinating — close the BroadcastChannel and stop heartbeats. */
  readonly stop: () => void;
}

/**
 * Create a coordinator and start it. The returned handle is ready
 * to consume events from `coordinator.live`.
 */
export const create = (options: MultiTabOptions): MultiTabCoordinator => {
  return Effect.runSync(make(options));
};

const make = (
  options: MultiTabOptions,
): Effect.Effect<MultiTabCoordinator, never, never> =>
  Effect.sync(() => {
    const BC = options.BroadcastChannel ?? globalThis.BroadcastChannel;
    if (!BC) {
      throw new Error("MultiTab: BroadcastChannel is not available");
    }
    const id = (options.randomUUID ?? crypto.randomUUID.bind(crypto))();
    const now = options.now ?? (() => performance.now());
    const heartbeatMs = options.heartbeatMs ?? 2_000;
    const leaseMs = options.leaseMs ?? 6_000;

    const channel = new BC(options.channel);
    let leaderId: string | null = null;
    let head: number = 0;
    let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    let electionTimer: ReturnType<typeof setTimeout> | null = null;
    let leaseTimer: ReturnType<typeof setTimeout> | null = null;
    let stopped = false;

    const pubSub: PubSub.PubSub<Event> = Effect.runSync(
      PubSub.bounded<Event>({ capacity: 1024 }),
    );

    const becomeLeader = (): void => {
      if (stopped) return;
      if (leaderId === id) return;
      leaderId = id;
      // Start the leader's WebSocket.
      options.client.connect();
      // Begin heartbeats.
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      heartbeatTimer = setInterval(() => {
        if (stopped) return;
        channel.postMessage({
          kind: "heartbeat",
          tabId: id,
          head,
          at: now(),
        } satisfies BroadcastHeartbeatMsg);
      }, heartbeatMs);
    };

    const becomeFollower = (newLeaderId: string): void => {
      if (stopped) return;
      leaderId = newLeaderId;
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
      }
    };

    const armLease = (): void => {
      if (leaseTimer) clearTimeout(leaseTimer);
      leaseTimer = setTimeout(() => {
        if (stopped) return;
        if (leaderId !== null && leaderId !== id) {
          // Leader went silent — start a fresh election.
          leaderId = null;
          runElection();
        }
      }, leaseMs);
    };

    const runElection = (): void => {
      if (stopped) return;
      if (electionTimer) clearTimeout(electionTimer);
      electionTimer = setTimeout(() => {
        if (stopped) return;
        channel.postMessage({
          kind: "hello",
          tabId: id,
        } satisfies BroadcastHelloMsg);
        // No leader claimed in 100ms → become leader ourselves.
        setTimeout(() => {
          if (stopped) return;
          if (leaderId === null) becomeLeader();
        }, 100);
      }, 0);
    };

    const onMessage = (event: MessageEvent): void => {
      const msg = event.data as BroadcastMessage;
      switch (msg.kind) {
        case "hello": {
          if (msg.tabId === id) return;
          if (leaderId === id) {
            // We're leader — tell the newcomer where the head is.
            channel.postMessage({
              kind: "head",
              tabId: id,
              head,
            } satisfies BroadcastHeadMsg);
          } else if (leaderId === null) {
            // Race for leadership — first to broadcast wins.
            becomeLeader();
          }
          return;
        }
        case "head": {
          if (leaderId === msg.tabId) head = Math.max(head, msg.head);
          return;
        }
        case "heartbeat": {
          if (msg.tabId === id) return;
          becomeFollower(msg.tabId);
          head = Math.max(head, msg.head);
          armLease();
          return;
        }
        case "event": {
          // Broadcast the event to local subscribers.
          Effect.runFork(PubSub.publish(pubSub, msg.event));
          return;
        }
        case "goodbye": {
          if (leaderId === msg.tabId) {
            leaderId = null;
            runElection();
          }
          return;
        }
      }
    };

    channel.addEventListener("message", onMessage);

    // Forward leader's events to local subscribers AND to the
    // BroadcastChannel for followers. We fork a fiber that pulls
    // from the SyncClient's live stream and fans out.
    Effect.runFork(
      Stream.runForEach(options.client.live, (event) =>
        Effect.sync(() => {
          Effect.runFork(PubSub.publish(pubSub, event));
          if (leaderId === id) {
            channel.postMessage({
              kind: "event",
              event,
            } satisfies BroadcastEventMsg);
          }
        }),
      ),
    );

    // Initial election — request leadership.
    runElection();

    return {
      live: Stream.fromPubSub(pubSub),
      isLeader: () => leaderId === id,
      head: () => head,
      stop: () => {
        stopped = true;
        if (heartbeatTimer) clearInterval(heartbeatTimer);
        if (electionTimer) clearTimeout(electionTimer);
        if (leaseTimer) clearTimeout(leaseTimer);
        if (leaderId === id) {
          channel.postMessage({
            kind: "goodbye",
            tabId: id,
          } satisfies BroadcastGoodbyeMsg);
        }
        channel.removeEventListener("message", onMessage);
        channel.close();
      },
    } satisfies MultiTabCoordinator;
  });