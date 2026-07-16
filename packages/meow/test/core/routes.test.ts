import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  type PrivateRouteTable,
  deregisterConnection,
  registerConnection,
  resolveAuthId,
  resolveConnection,
} from "../../src/core/routes.ts";
import type { AuthContext } from "../../src/core/hooks.ts";

const alice: AuthContext = { id: "alice" };
const bob: AuthContext = { id: "bob" };

const empty: PrivateRouteTable = {
  connectionsById: new Map(),
  connectionsByAuth: new Map(),
};

describe("PrivateRouteTable", () => {
  it.effect("registerConnection adds to both maps", () =>
    Effect.sync(() => {
      const t1 = registerConnection(empty, "c1", alice);
      expect(t1.connectionsById.get("c1")).toEqual(alice);
      expect(resolveAuthId(t1, "alice")).toEqual(["c1"]);
    }),
  );

  it.effect("registerConnection for an unauth'd connection leaves auths empty", () =>
    Effect.sync(() => {
      const t1 = registerConnection(empty, "c1", undefined);
      expect(t1.connectionsById.get("c1")).toBeUndefined();
      expect(resolveAuthId(t1, "anything")).toEqual([]);
    }),
  );

  it.effect("registerConnection accumulates multiple connections per auth", () =>
    Effect.sync(() => {
      const t1 = registerConnection(empty, "c1", alice);
      const t2 = registerConnection(t1, "c2", alice);
      const t3 = registerConnection(t2, "c3", bob);
      expect(resolveAuthId(t3, "alice")).toEqual(["c1", "c2"]);
      expect(resolveAuthId(t3, "bob")).toEqual(["c3"]);
      expect(resolveConnection(t3, "c2")).toEqual(alice);
    }),
  );

  it.effect("deregisterConnection removes from both maps", () =>
    Effect.sync(() => {
      const t1 = registerConnection(empty, "c1", alice);
      const t2 = registerConnection(t1, "c2", alice);
      const { table } = deregisterConnection(t2, "c1");
      expect(resolveAuthId(table, "alice")).toEqual(["c2"]);
      expect(resolveConnection(table, "c1")).toBeUndefined();
    }),
  );

  it.effect("deregisterConnection drops the auth entry when last tab leaves", () =>
    Effect.sync(() => {
      const t1 = registerConnection(empty, "c1", alice);
      const { table } = deregisterConnection(t1, "c1");
      expect(table.connectionsByAuth.has("alice")).toBe(false);
    }),
  );
});