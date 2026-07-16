import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as MultiTab from "../../src/sync/multi-tab.ts";
import type {
  Branch,
  Event,
  EventId,
  Seq,
  SessionId,
} from "../../src/sync/schema.ts";

// ---------------------------------------------------------------------------
// Fake BroadcastChannel — in-process, synchronous, injectable for tests
// ---------------------------------------------------------------------------

type Listener = (event: MessageEvent) => void;

const channelsByName = new Map<string, Set<FakeBC>>();

class FakeBC {
  readonly name: string;
  readonly #listeners = new Set<Listener>();
  #closed = false;
  constructor(name: string) {
    this.name = name;
    let set = channelsByName.get(name);
    if (!set) {
      set = new Set();
      channelsByName.set(name, set);
    }
    set.add(this);
  }
  postMessage(data: unknown): void {
    if (this.#closed) return;
    const set = channelsByName.get(this.name);
    if (!set) return;
    for (const ch of set) {
      if (ch === this) continue;
      for (const fn of ch.#listeners) {
        fn(new MessageEvent("message", { data }));
      }
    }
  }
  addEventListener(_kind: "message", fn: Listener): void {
    this.#listeners.add(fn);
  }
  removeEventListener(_kind: "message", fn: Listener): void {
    this.#listeners.delete(fn);
  }
  close(): void {
    this.#closed = true;
    channelsByName.get(this.name)?.delete(this);
  }
}

const resetChannels = () => {
  for (const set of channelsByName.values()) {
    for (const ch of set) ch.close();
  }
  channelsByName.clear();
};

// ---------------------------------------------------------------------------
// Minimal SyncClient stub for unit tests — exposes only `live` and `connect`
// ---------------------------------------------------------------------------

const makeClientStub = (): {
  live: Stream.Stream<Event, never, never>;
  connect: () => void;
} => {
  const pubSub = Effect.runSync(
    PubSub.bounded<Event>({ capacity: 1024 }),
  );
  return {
    live: Stream.fromPubSub(pubSub),
    connect: () => {},
  };
};

const sampleEvent = (i: number): Event => ({
  id: `e${i}` as EventId,
  seq: i as Seq,
  branch: "main" as Branch,
  parent: null,
  payload: { n: i },
  author: "tester" as SessionId,
  createdAt: Date.now(),
});

// Deterministic id generator and clock for tests.
const makeIdSeq = (prefix: string) => {
  let i = 0;
  return () => `${prefix}-${++i}`;
};

const makeClock = () => {
  let t = 0;
  return () => {
    t += 100;
    return t;
  };
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("MultiTab coordinator", () => {
  it("elects a single leader across two tabs", () =>
    Effect.sync(() => {
      resetChannels();
      const c1 = MultiTab.create({
        channel: "test-1",
        client: makeClientStub() as never,
        BroadcastChannel: FakeBC as unknown as typeof BroadcastChannel,
        randomUUID: makeIdSeq("tab"),
        now: makeClock(),
        heartbeatMs: 1_000,
        leaseMs: 5_000,
      });
      const c2 = MultiTab.create({
        channel: "test-1",
        client: makeClientStub() as never,
        BroadcastChannel: FakeBC as unknown as typeof BroadcastChannel,
        randomUUID: makeIdSeq("tab"),
        now: makeClock(),
        heartbeatMs: 1_000,
        leaseMs: 5_000,
      });

      // Exactly one of them should be leader.
      expect(c1.isLeader() !== c2.isLeader()).toBe(true);

      c1.stop();
      c2.stop();
    }));

  it("stop() closes the channel and removes it from the registry", () =>
    Effect.sync(() => {
      resetChannels();
      const c = MultiTab.create({
        channel: "test-stop",
        client: makeClientStub() as never,
        BroadcastChannel: FakeBC as unknown as typeof BroadcastChannel,
      });
      c.stop();
      expect(channelsByName.get("test-stop")?.size ?? 0).toBe(0);
    }));

  it("head() returns a number", () =>
    Effect.sync(() => {
      resetChannels();
      const c = MultiTab.create({
        channel: "test-head",
        client: makeClientStub() as never,
        BroadcastChannel: FakeBC as unknown as typeof BroadcastChannel,
      });
      expect(typeof c.head()).toBe("number");
      c.stop();
    }));

  it("exposes a reactive `live` stream", () =>
    Effect.sync(() => {
      resetChannels();
      const c = MultiTab.create({
        channel: "test-live",
        client: makeClientStub() as never,
        BroadcastChannel: FakeBC as unknown as typeof BroadcastChannel,
      });
      // Stream is exposed; verify it has the right shape.
      expect(c.live).toBeDefined();
      c.stop();
    }));

  it("isLeader returns false after stop()", () =>
    Effect.sync(() => {
      resetChannels();
      const c = MultiTab.create({
        channel: "test-isleader",
        client: makeClientStub() as never,
        BroadcastChannel: FakeBC as unknown as typeof BroadcastChannel,
      });
      c.stop();
      expect(c.isLeader()).toBe(false);
    }));
});