import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { Schema } from "effect";
import {
  Branch,
  Event,
  EventId,
  Seq,
  SessionId,
  StoreId,
} from "../../src/sync/schema.ts";
import {
  PushRequest,
  PushResponse,
  PullRequest,
  PullResponse,
  HeadResponse,
  LiveEnvelope,
  ClientLiveEnvelope,
} from "../../src/sync/protocol.ts";

describe("SyncSchema", () => {
  it.effect("Event round-trips through JSON", () =>
    Effect.sync(() => {
      const event: Event = {
        id: "ev-1" as EventId,
        seq: 1 as Seq,
        branch: "main" as Branch,
        parent: null,
        payload: { kind: "TodoCreated", title: "write README" },
        author: "sess-1" as SessionId,
        createdAt: 1700000000000,
      };
      const round = Schema.decodeUnknownSync(Event)(
        JSON.parse(JSON.stringify(event)),
      );
      expect(round).toEqual(event);
    }),
  );

  it.effect("EventId is branded", () =>
    Effect.sync(() => {
      const branded = "ev-1" as EventId;
      expect(Schema.is(EventId)(branded)).toBe(true);
      expect(Schema.is(EventId)("not-an-id")).toBe(true); // brands don't validate at runtime
    }),
  );
});

describe("PushRequest / PushResponse", () => {
  it.effect("decodes a valid push", () =>
    Effect.sync(() => {
      const req: PushRequest = {
        storeId: "default" as StoreId,
        branch: "main" as Branch,
        sessionId: "sess-1" as SessionId,
        events: [
          {
            id: "ev-1" as EventId,
            parent: null,
            payload: { kind: "Test" },
            createdAt: 1,
          },
        ],
      };
      const decoded = Schema.decodeUnknownSync(PushRequest)(req);
      expect(decoded.branch).toBe("main");
    }),
  );

  it.effect("rejects an event with missing id", () =>
    Effect.sync(() => {
      try {
        Schema.decodeUnknownSync(PushRequest)({
          storeId: "default",
          branch: "main",
          sessionId: "sess-1",
          events: [{ parent: null, payload: {}, createdAt: 1 }],
        });
        expect.unreachable("should have thrown");
      } catch (e) {
        expect(e instanceof Error).toBe(true);
      }
    }),
  );
});

describe("PullRequest", () => {
  it.effect("accepts since as optional", () =>
    Effect.sync(() => {
      const r = Schema.decodeUnknownSync(PullRequest)({
        storeId: "default",
        branch: "main",
      });
      expect(r.since).toBeUndefined();
      expect(r.limit).toBeUndefined();
    }),
  );

  it.effect("parses `since` as number", () =>
    Effect.sync(() => {
      const r = Schema.decodeUnknownSync(PullRequest)({
        storeId: "default",
        branch: "main",
        since: 42,
        limit: 10,
      });
      expect(r.since).toBe(42);
      expect(r.limit).toBe(10);
    }),
  );
});

describe("LiveEnvelope", () => {
  it.effect("hello envelope", () =>
    Effect.sync(() => {
      const env: LiveEnvelope = {
        kind: "hello",
        sessionId: "sess-1" as SessionId,
        head: 0 as Seq,
      };
      expect(Schema.decodeUnknownSync(LiveEnvelope)(env)).toEqual(env);
    }),
  );

  it.effect("event envelope", () =>
    Effect.sync(() => {
      const env: LiveEnvelope = {
        kind: "event",
        event: {
          id: "ev-1" as EventId,
          seq: 1 as Seq,
          branch: "main" as Branch,
          parent: null,
          payload: { hi: 1 },
          author: "sess-1" as SessionId,
          createdAt: 1,
        },
      };
      expect(Schema.decodeUnknownSync(LiveEnvelope)(env)).toEqual(env);
    }),
  );

  it.effect("ping/pong enums", () =>
    Effect.sync(() => {
      expect(Schema.decodeUnknownSync(LiveEnvelope)({ kind: "ping" })).toEqual({
        kind: "ping",
      });
      expect(Schema.decodeUnknownSync(LiveEnvelope)({ kind: "pong" })).toEqual({
        kind: "pong",
      });
    }),
  );
});

describe("ClientLiveEnvelope", () => {
  it.effect("subscribe", () =>
    Effect.sync(() => {
      const env: ClientLiveEnvelope = {
        kind: "subscribe",
        branch: "main" as Branch,
      };
      expect(Schema.decodeUnknownSync(ClientLiveEnvelope)(env)).toEqual(env);
    }),
  );
});