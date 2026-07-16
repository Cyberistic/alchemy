import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  PresenceClient,
  presenceUrl,
} from "../../src/presence/Client.ts";

/**
 * Minimal mock WebSocket that records sends and lets the test drive state
 * transitions. Stands in for `globalThis.WebSocket` in Node tests.
 */
class MockWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: MockWebSocket[] = [];

  readonly sent: string[] = [];
  readyState: number = MockWebSocket.CONNECTING;
  onopen: ((ev: Event) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;

  addEventListener = (
    type: "open" | "close" | "message" | "error",
    listener: any,
  ) => {
    if (type === "open") this.onopen = listener;
    else if (type === "close") this.onclose = listener;
    else if (type === "message") this.onmessage = listener;
    else this.onerror = listener;
  };

  removeEventListener = () => {};

  send = (data: string) => {
    this.sent.push(data);
  };

  close = () => {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.({} as CloseEvent);
  };

  // Test helpers ----------------------------------------------------------

  emitOpen() {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.({} as Event);
  }

  emitMessage(data: string) {
    this.onmessage?.({ data } as MessageEvent);
  }

  emitClose() {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.({} as CloseEvent);
  }
}

const installMock = () => {
  MockWebSocket.instances = [];
  const MockWS = function (url: string) {
    const ws = new MockWebSocket();
    (ws as any).url = url;
    MockWebSocket.instances.push(ws);
    return ws as unknown as WebSocket;
  } as unknown as typeof WebSocket;
  (MockWS as any).CONNECTING = 0;
  (MockWS as any).OPEN = 1;
  (MockWS as any).CLOSING = 2;
  (MockWS as any).CLOSED = 3;
  (globalThis as any).WebSocket = MockWS;
};

describe("PresenceClient", () => {
  it.effect("connects, exposes peer updates, and serialises sends", () =>
    Effect.gen(function* () {
      installMock();
      const client = new PresenceClient<{ x: number }, { text: string }>({
        url: "wss://example.test/parties/Room/lobby",
      });
      client.connect();
      const ws = MockWebSocket.instances[0]!;
      ws.emitOpen();

      ws.emitMessage(
        JSON.stringify({
          kind: "init",
          self: "me",
          peers: { other: { x: 1 } },
        }),
      );
      expect(client.self).toBe("me");
      expect(client.peers.get("other")).toEqual({ x: 1 });

      ws.emitMessage(
        JSON.stringify({ kind: "presence", id: "third", state: { x: 2 } }),
      );
      expect(client.peers.get("third")).toEqual({ x: 2 });

      ws.emitMessage(JSON.stringify({ kind: "absence", id: "third" }));
      expect(client.peers.has("third")).toBe(false);

      client.update({ x: 99 });
      expect(ws.sent[ws.sent.length - 1]).toBe(
        JSON.stringify({ kind: "state", state: { x: 99 } }),
      );

      client.send({ text: "hi" });
      expect(ws.sent[ws.sent.length - 1]).toBe(
        JSON.stringify({ kind: "message", data: { text: "hi" } }),
      );

      client.close();
    }),
  );

  it.effect("ignores messages from a non-current socket after reconnect", () =>
    Effect.gen(function* () {
      installMock();
      const snapshots: number[] = [];
      const client = new PresenceClient<{ x: number }, unknown>({
        url: "wss://example.test/parties/Room/lobby",
        onPeers: (peers) => snapshots.push(peers.size),
      });
      client.connect();
      const ws = MockWebSocket.instances[0]!;
      ws.emitOpen();
      ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));
      const before = snapshots.length;

      ws.emitClose();

      // Late messages from the closed socket shouldn't update peers.
      ws.emitMessage(
        JSON.stringify({ kind: "presence", id: "ghost", state: { x: 1 } }),
      );
      expect(snapshots.length).toBe(before);
      expect(client.peers.size).toBe(0);

      // close() should still work and stop reconnects.
      client.close();
    }),
  );

  it.effect("subscribe() supports multiple listeners", () =>
    Effect.gen(function* () {
      installMock();
      const client = new PresenceClient<{ x: number }, unknown>({
        url: "wss://example.test/parties/Room/lobby",
      });
      client.connect();
      const ws = MockWebSocket.instances[0]!;
      ws.emitOpen();
      ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));

      const a: number[] = [];
      const b: number[] = [];
      const unsubA = client.subscribe((peers) => a.push(peers.size));
      const unsubB = client.subscribe((peers) => b.push(peers.size));

      ws.emitMessage(
        JSON.stringify({ kind: "presence", id: "x", state: { x: 1 } }),
      );
      expect(a).toEqual([0, 1]);
      expect(b).toEqual([0, 1]);

      unsubA();
      ws.emitMessage(
        JSON.stringify({ kind: "presence", id: "y", state: { x: 2 } }),
      );
      expect(a).toEqual([0, 1]);
      expect(b).toEqual([0, 1, 2]);

      client.close();
    }),
  );

  it.effect("sendPrivate() serialises a private envelope", () =>
    Effect.gen(function* () {
      installMock();
      const client = new PresenceClient<unknown, { kind: "typing" }>({
        url: "wss://example.test/parties/Room/lobby",
      });
      client.connect();
      const ws = MockWebSocket.instances[0]!;
      ws.emitOpen();

      client.sendPrivate("user-42", { kind: "typing" });
      expect(ws.sent[ws.sent.length - 1]).toBe(
        JSON.stringify({
          kind: "private",
          to: "user-42",
          data: { kind: "typing" },
        }),
      );

      client.close();
    }),
  );

  it.effect("onPrivate() receives private envelopes from the server", () =>
    Effect.gen(function* () {
      installMock();
      const received: Array<{ from: string; data: { kind: string } }> = [];
      const client = new PresenceClient<unknown, { kind: string }>({
        url: "wss://example.test/parties/Room/lobby",
        onPrivate: (from, data) => received.push({ from, data }),
      });
      client.connect();
      const ws = MockWebSocket.instances[0]!;
      ws.emitOpen();
      ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));

      ws.emitMessage(
        JSON.stringify({
          kind: "private",
          from: "alice",
          data: { kind: "typing" },
        }),
      );
      ws.emitMessage(
        JSON.stringify({
          kind: "private",
          from: "bob",
          data: { kind: "draft" },
        }),
      );

      expect(received).toEqual([
        { from: "alice", data: { kind: "typing" } },
        { from: "bob", data: { kind: "draft" } },
      ]);

      client.close();
    }),
  );

  it("purr: server reply clears the watchdog", async () => {
    installMock();
    const client = new PresenceClient<unknown, unknown>({
      url: "wss://example.test/parties/Room/lobby",
      heartbeat: { interval: "50 millis", timeout: "200 millis" },
    });
    client.connect();
    const ws = MockWebSocket.instances[0]!;
    ws.emitOpen();

    // Within ~120ms the client should have sent at least one purr.
    await new Promise((r) => setTimeout(r, 120));

    const purrs = ws.sent.filter(
      (s) => JSON.parse(s).kind === "purr",
    );
    expect(purrs.length).toBeGreaterThan(0);

    // Server replies with purr; the watchdog should be cleared (no
    // forced close within the timeout window).
    ws.emitMessage(JSON.stringify({ kind: "purr" }));
    await new Promise((r) => setTimeout(r, 100));
    expect(ws.readyState).toBe(MockWebSocket.OPEN);

    client.close();
  }, 10_000);
});

describe("presenceUrl", () => {
  it.effect("builds the canonical /meow/:class/:room URL", () =>
    Effect.sync(() => {
      expect(
        presenceUrl({ host: "app.example.com", class: "Room", room: "lobby" }),
      ).toBe("wss://app.example.com/meow/Room/lobby");
      expect(
        presenceUrl({
          host: "app.example.com",
          class: "Room",
          room: "lobby",
          protocol: "ws",
        }),
      ).toBe("ws://app.example.com/meow/Room/lobby");
      expect(presenceUrl({ host: "app.example.com", room: "lobby" })).toBe(
        "wss://app.example.com/meow/main/lobby",
      );
    }),
  );
});