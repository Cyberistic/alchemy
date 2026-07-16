import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeInMemoryStore } from "../../src/sync/store.ts";
import {
  Branch,
  EventId,
  Seq,
  SessionId,
} from "../../src/sync/schema.ts";

const sample = (id: string, parent: EventId | null = null, payload: unknown = {}) => ({
  branch: "main" as Branch,
  id: id as EventId,
  parent,
  payload,
  author: "sess-1" as SessionId,
  createdAt: Date.now(),
});

describe("InMemoryStore", () => {
  it.effect("append assigns seq starting at 1", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      yield* store.append(sample("a"));
      yield* store.append(sample("b", "a" as EventId));
      yield* store.append(sample("c", "b" as EventId));
      const events = yield* store.readSince({
        branch: "main" as Branch,
        since: 0 as Seq,
      });
      expect(events.map((e) => e.seq)).toEqual([1, 2, 3]);
      expect(events.map((e) => e.id)).toEqual(["a", "b", "c"]);
    }),
  );

  it.effect("readSince(since=N) returns only events with seq > N", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      yield* store.append(sample("a"));
      yield* store.append(sample("b", "a" as EventId));
      yield* store.append(sample("c", "b" as EventId));
      const after2 = yield* store.readSince({
        branch: "main" as Branch,
        since: 2 as Seq,
      });
      expect(after2.map((e) => e.id)).toEqual(["c"]);
    }),
  );

  it.effect("head() returns the highest seq + total count", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      yield* store.append(sample("a"));
      yield* store.append(sample("b", "a" as EventId));
      const head = yield* store.head("main" as Branch);
      expect(head.head).toBe(2);
      expect(head.count).toBe(2);
    }),
  );

  it.effect("head() on an empty branch returns seq=0, count=0", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      const head = yield* store.head("main" as Branch);
      expect(head.head).toBe(0);
      expect(head.count).toBe(0);
    }),
  );

  it.effect("readSince respects limit", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      yield* store.append(sample("a"));
      yield* store.append(sample("b", "a" as EventId));
      yield* store.append(sample("c", "b" as EventId));
      yield* store.append(sample("d", "c" as EventId));
      const events = yield* store.readSince({
        branch: "main" as Branch,
        since: 0 as Seq,
        limit: 2,
      });
      expect(events.length).toBe(2);
      expect(events.map((e) => e.id)).toEqual(["a", "b"]);
    }),
  );

  it.effect("isolates branches", () =>
    Effect.gen(function* () {
      const store = makeInMemoryStore();
      yield* store.append({ ...sample("a"), branch: "main" as Branch });
      yield* store.append({ ...sample("x"), branch: "feature" as Branch });
      const main = yield* store.readSince({
        branch: "main" as Branch,
        since: 0 as Seq,
      });
      const feature = yield* store.readSince({
        branch: "feature" as Branch,
        since: 0 as Seq,
      });
      expect(main.map((e) => e.id)).toEqual(["a"]);
      expect(feature.map((e) => e.id)).toEqual(["x"]);
    }),
  );
});