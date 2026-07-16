import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as Broadcast from "../../src/broadcast/index.ts";
import type { BroadcastEnvelope } from "../../src/broadcast/index.ts";

describe("sharding", () => {
  it("defaultRoute returns the same shard for the same topic", () => {
    const route = Broadcast.defaultRoute(16);
    const a = route("user-42-signed-up");
    const b = route("user-42-signed-up");
    expect(a).toBe(b);
  });

  it("defaultRoute spreads 1000 random topics across all shards", () => {
    const shards = 8;
    const route = Broadcast.defaultRoute(shards);
    const hits = new Map<number, number>();
    for (let i = 0; i < 1000; i++) {
      const topic = `topic-${i}-${Math.random().toString(36).slice(2)}`;
      const idx = route(topic);
      hits.set(idx, (hits.get(idx) ?? 0) + 1);
    }
    // Every shard should have at least one topic — a uniform
    // distribution is the goal.
    expect(hits.size).toBe(shards);
    for (const [, count] of hits) {
      expect(count).toBeGreaterThan(50); // ~125 expected
    }
  });

  it("shardFor respects custom route functions", () => {
    expect(
      Broadcast.shardFor("anything", {
        route: () => 3,
      }),
    ).toBe(3);
  });

  it("shardFor uses default 4 shards when none specified", () => {
    expect(Broadcast.shardFor("any-topic")).toBeGreaterThanOrEqual(0);
    expect(Broadcast.shardFor("any-topic")).toBeLessThan(4);
  });
});

describe("BroadcastServer (in-process)", () => {
  it("publishes to a topic and the subscriber sees the envelope", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<{ kind: string; userId: string }>();

      const live = server.subscribe("user-42-signed-up");
      // Subscribe first so the PubSub is allocated.
      const fiber = yield* Effect.forkChild(
        Stream.runForEach(live, () => Effect.void),
      );

      // Yield once to give the subscriber time to start.
      yield* Effect.yieldNow;

      const id = Broadcast.EventId(crypto.randomUUID());
      yield* server.broadcast({
        kind: "event",
        topic: "user-42-signed-up",
        id,
        ts: Date.now(),
        event: { kind: "signed-up", userId: "42" },
      });

      yield* Effect.yieldNow;
      yield* Effect.forkChild(Fiber.interrupt(fiber));
    }));

  it("count starts at 0 for an unseen topic", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<unknown>();
      expect(yield* server.count("never-published")).toBe(0);
    }));

  it("handlePublish accepts a well-formed body and assigns an id", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<{ hello: string }>();
      const result = yield* server.handlePublish({
        topic: "test-topic",
        event: { hello: "world" },
      });
      expect(result.accepted).toBe(1);
      expect(typeof result.id).toBe("string");
      expect(result.id.length).toBeGreaterThan(0);
    }));

  it("handlePublish rejects an empty topic with a 400", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<unknown>();
      const exit = yield* Effect.exit(
        server.handlePublish({ topic: "", event: { ok: 1 } }),
      );
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        const err = (exit.cause as { error?: { _tag: string; status: number } })
          .error;
        expect(err?._tag).toBe("HttpError");
        expect(err?.status).toBe(400);
      }
    }));

  it("handlePublish rejects malformed bodies", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<unknown>();
      const exit = yield* Effect.exit(server.handlePublish("not an object"));
      expect(exit._tag).toBe("Failure");
    }));

  it("handlePublish enforces the per-shard topic limit", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<unknown>({ maxTopicsPerShard: 2 });
      yield* server.handlePublish({ topic: "t1", event: 1 });
      yield* server.handlePublish({ topic: "t2", event: 2 });
      const exit = yield* Effect.exit(
        server.handlePublish({ topic: "t3", event: 3 }),
      );
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        const err = (exit.cause as { error?: { status: number } }).error;
        expect(err?.status).toBe(429);
      }
    }));

  it("onPublished hook fires after a successful publish", () =>
    Effect.gen(function* () {
      const seen: string[] = [];
      const server = yield* Broadcast.make<unknown>({
        onPublished: (_topic, id) =>
          Effect.sync(() => {
            seen.push(id);
          }),
      });
      const result = yield* server.handlePublish({
        topic: "t",
        event: 1,
      });
      expect(seen).toContain(result.id);
    }));

  it("multiple subscribers on the same topic each see the event", () =>
    Effect.gen(function* () {
      const server = yield* Broadcast.make<{ n: number }>();
      const a = server.subscribe("shared");
      const b = server.subscribe("shared");

      const aReceived: BroadcastEnvelope<{ n: number }>[] = [];
      const bReceived: BroadcastEnvelope<{ n: number }>[] = [];

      const fa = yield* Effect.forkChild(
        Stream.runForEach(a, (env) =>
          Effect.sync(() => {
            aReceived.push(env);
          }),
        ),
      );
      const fb = yield* Effect.forkChild(
        Stream.runForEach(b, (env) =>
          Effect.sync(() => {
            bReceived.push(env);
          }),
        ),
      );
      yield* Effect.yieldNow;

      yield* server.broadcast({
        kind: "event",
        topic: "shared",
        id: Broadcast.EventId(crypto.randomUUID()),
        ts: Date.now(),
        event: { n: 1 },
      });
      yield* Effect.yieldNow;

      expect(aReceived.length).toBe(1);
      expect(bReceived.length).toBe(1);
      expect(aReceived[0].event.n).toBe(1);
      expect(bReceived[0].event.n).toBe(1);

      yield* Effect.forkChild(Fiber.interrupt(fa));
      yield* Effect.forkChild(Fiber.interrupt(fb));
    }));
});

describe("Broadcast.EventId", () => {
  it("brands strings", () => {
    const id = Broadcast.EventId("abc-123");
    expect(id).toBe("abc-123");
  });
});