import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  allow,
  defaultRateLimit,
  deny,
  isAuthError,
  reject,
  type PresenceLogEvent,
  type RateLimitCounters,
  type RateLimitDecision,
} from "../../src/core/hooks.ts";

const zeroCounters: RateLimitCounters = {
  inboundLastSecond: 0,
  inboundLastMinute: 0,
  broadcastsLastMinute: 0,
  privatesLastMinute: 0,
};

const hotCounters = (overrides: Partial<RateLimitCounters> = {}): RateLimitCounters => ({
  inboundLastSecond: 100,
  inboundLastMinute: 100,
  broadcastsLastMinute: 100,
  privatesLastMinute: 100,
  ...overrides,
});

describe("allow / deny helpers", () => {
  it.effect("allow is `{ allow: true }`", () =>
    Effect.sync(() => {
      expect(allow.allow).toBe(true);
      expect(allow.retryAfterMs).toBeUndefined();
    }),
  );

  it.effect("deny(n) is `{ allow: false, retryAfterMs: n }`", () =>
    Effect.sync(() => {
      expect(deny(1000)).toEqual({ allow: false, retryAfterMs: 1000 });
      expect(deny()).toEqual({ allow: false });
    }),
  );
});

describe("defaultRateLimit policy", () => {
  it.effect("purr is always allowed", () =>
    Effect.sync(() => {
      expect(defaultRateLimit({ kind: "purr", counters: hotCounters() })).toEqual(
        allow,
      );
    }),
  );

  it.effect("state allows under 30/sec", () =>
    Effect.sync(() => {
      expect(
        defaultRateLimit({
          kind: "state",
          counters: { ...zeroCounters, inboundLastSecond: 29 },
        }),
      ).toEqual(allow);
    }),
  );

  it.effect("state denies at 30/sec with 1s backoff", () =>
    Effect.sync(() => {
      expect(
        defaultRateLimit({
          kind: "state",
          counters: { ...zeroCounters, inboundLastSecond: 30 },
        }),
      ).toEqual({ allow: false, retryAfterMs: 1_000 });
    }),
  );

  it.effect("message allows under 10/sec, denies at 10 with 1s backoff", () =>
    Effect.sync(() => {
      expect(
        defaultRateLimit({
          kind: "message",
          counters: { ...zeroCounters, inboundLastSecond: 9 },
        }),
      ).toEqual(allow);
      expect(
        defaultRateLimit({
          kind: "message",
          counters: { ...zeroCounters, inboundLastSecond: 10 },
        }),
      ).toEqual({ allow: false, retryAfterMs: 1_000 });
    }),
  );

  it.effect("private allows under 5/sec, denies at 5 with 2s backoff", () =>
    Effect.sync(() => {
      expect(
        defaultRateLimit({
          kind: "private",
          counters: { ...zeroCounters, inboundLastSecond: 4 },
        }),
      ).toEqual(allow);
      expect(
        defaultRateLimit({
          kind: "private",
          counters: { ...zeroCounters, inboundLastSecond: 5 },
        }),
      ).toEqual({ allow: false, retryAfterMs: 2_000 });
    }),
  );
});

describe("isAuthError", () => {
  it.effect("rejects non-AuthError values", () =>
    Effect.sync(() => {
      expect(isAuthError(new Error("other"))).toBe(false);
      expect(isAuthError("string")).toBe(false);
      expect(isAuthError(undefined)).toBe(false);
    }),
  );

  it.effect("recognises errors with a name matching AuthError", () =>
    Effect.sync(() => {
      class FakeAuthError extends Error {
        constructor(msg: string) {
          super(msg);
          this.name = "AuthError";
        }
      }
      expect(isAuthError(new FakeAuthError("nope"))).toBe(true);
    }),
  );
});

describe("PresenceLogEvent exhaustiveness", () => {
  // Compile-time check: the tagged union is exhaustive enough that
  // `switch (event.kind)` covers every variant. We can't catch missing
  // cases at runtime, but documenting them here keeps the index
  // discoverable.
  it.effect("documents the variant set", () =>
    Effect.sync(() => {
      const kinds: PresenceLogEvent["kind"][] = [
        "connect",
        "auth-rejected",
        "disconnect",
        "heartbeat-timeout",
        "broadcast",
        "throttled",
        "error",
      ];
      expect(kinds.length).toBe(7);
    }),
  );
});

void ({} as RateLimitDecision);