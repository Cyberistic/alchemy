import { describe, expect, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type {
  PresenceClientEnvelope,
  PresenceServerEnvelope,
} from "../../src/core/protocol.ts";
import {
  isStale,
  resolveClientHeartbeat,
  resolveServerHeartbeat,
} from "../../src/core/purr.ts";

describe("Presence wire protocol", () => {
  describe("client envelopes", () => {
    it.effect("serialises a state envelope", () =>
      Effect.sync(() => {
        const env: PresenceClientEnvelope<{ x: number }, unknown> = {
          kind: "state",
          state: { x: 1 },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "state",
          state: { x: 1 },
        });
      }),
    );

    it.effect("serialises a message envelope", () =>
      Effect.sync(() => {
        const env: PresenceClientEnvelope<unknown, { text: string }> = {
          kind: "message",
          data: { text: "hi" },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "message",
          data: { text: "hi" },
        });
      }),
    );

    it.effect("serialises a private (DM) envelope", () =>
      Effect.sync(() => {
        const env: PresenceClientEnvelope<unknown, { kind: "typing" }> = {
          kind: "private",
          to: "user-42",
          data: { kind: "typing" },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "private",
          to: "user-42",
          data: { kind: "typing" },
        });
      }),
    );

    it.effect("serialises a purr envelope (heartbeat)", () =>
      Effect.sync(() => {
        const env: PresenceClientEnvelope<unknown, unknown> = { kind: "purr" };
        expect(JSON.parse(JSON.stringify(env))).toEqual({ kind: "purr" });
      }),
    );
  });

  describe("server envelopes", () => {
    it.effect("serialises an init envelope with peers", () =>
      Effect.sync(() => {
        const env: PresenceServerEnvelope<{ name: string }, unknown> = {
          kind: "init",
          self: "a",
          peers: { b: { name: "Bob" } },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "init",
          self: "a",
          peers: { b: { name: "Bob" } },
        });
      }),
    );

    it.effect("serialises a presence envelope", () =>
      Effect.sync(() => {
        const env: PresenceServerEnvelope<{ cursor: unknown }, unknown> = {
          kind: "presence",
          id: "x",
          state: { cursor: { x: 1, y: 2 } },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "presence",
          id: "x",
          state: { cursor: { x: 1, y: 2 } },
        });
      }),
    );

    it.effect("serialises an absence envelope", () =>
      Effect.sync(() => {
        const env: PresenceServerEnvelope<unknown, unknown> = {
          kind: "absence",
          id: "gone",
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "absence",
          id: "gone",
        });
      }),
    );

    it.effect("serialises a message envelope", () =>
      Effect.sync(() => {
        const env: PresenceServerEnvelope<unknown, { text: string }> = {
          kind: "message",
          from: "alice",
          data: { text: "hello" },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "message",
          from: "alice",
          data: { text: "hello" },
        });
      }),
    );

    it.effect("serialises a private (DM) envelope from server to client", () =>
      Effect.sync(() => {
        const env: PresenceServerEnvelope<unknown, { kind: "typing" }> = {
          kind: "private",
          from: "alice",
          data: { kind: "typing" },
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({
          kind: "private",
          from: "alice",
          data: { kind: "typing" },
        });
      }),
    );

    it.effect("serialises a purr envelope (server heartbeat reply)", () =>
      Effect.sync(() => {
        const env: PresenceServerEnvelope<unknown, unknown> = {
          kind: "purr",
        };
        expect(JSON.parse(JSON.stringify(env))).toEqual({ kind: "purr" });
      }),
    );
  });
});

describe("purr (heartbeat) configuration", () => {
  it.effect("server defaults: 30s interval, 45s timeout", () =>
    Effect.sync(() => {
      const c = resolveServerHeartbeat();
      expect(Duration.toMillis(c.interval)).toBe(30_000);
      expect(Duration.toMillis(c.timeout)).toBe(45_000);
    }),
  );

  it.effect("client defaults: 25s interval, 35s timeout", () =>
    Effect.sync(() => {
      const c = resolveClientHeartbeat();
      expect(Duration.toMillis(c.interval)).toBe(25_000);
      expect(Duration.toMillis(c.timeout)).toBe(35_000);
    }),
  );

  it.effect("custom options override defaults", () =>
    Effect.sync(() => {
      const c = resolveServerHeartbeat({
        interval: "10 seconds",
        timeout: "20 seconds",
      });
      expect(Duration.toMillis(c.interval)).toBe(10_000);
      expect(Duration.toMillis(c.timeout)).toBe(20_000);
    }),
  );
});

describe("isStale", () => {
  it.effect("undefined lastSeen is stale", () =>
    Effect.sync(() => {
      expect(isStale(undefined, 10_000, 1000)).toBe(true);
    }),
  );

  it.effect("recent lastSeen is not stale", () =>
    Effect.sync(() => {
      const now = 100_000;
      expect(isStale(now - 5_000, 10_000, now)).toBe(false);
    }),
  );

  it.effect("old lastSeen is stale", () =>
    Effect.sync(() => {
      const now = 100_000;
      expect(isStale(now - 20_000, 10_000, now)).toBe(true);
    }),
  );
});