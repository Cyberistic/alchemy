import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { PresenceClient } from "../../src/presence/Client.ts";

/**
 * Minimal mock WebSocket. Same shape as the one in
 * test/presence/Client.test.ts — duplicated here to keep the two
 * test files independent (no shared mutable class state across suites).
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

describe("PresenceClient outbox (per-message status)", () => {
  it.effect(
    "sendWithStatus starts at `sending` when the socket is open",
    () =>
      Effect.gen(function* () {
        installMock();
        const client = new PresenceClient<unknown, { text: string }>({
          url: "wss://example.test/parties/Room/lobby",
        });
        client.connect();
        const ws = MockWebSocket.instances[0]!;
        ws.emitOpen();
        ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));

        const ref = client.sendWithStatus({ text: "hi" });
        expect(ref.id).toBeDefined();
        expect(ref.status).toBe("sending");
        expect(ref.attempts).toBe(1);
        expect(ref.sentAt).toBeDefined();
        expect(client.outbox).toHaveLength(1);

        client.close();
      }),
  );

  it.effect("server `{kind:'ack', id}` flips the ref to `sent`", () =>
    Effect.gen(function* () {
      installMock();
      const client = new PresenceClient<unknown, { text: string }>({
        url: "wss://example.test/parties/Room/lobby",
      });
      client.connect();
      const ws = MockWebSocket.instances[0]!;
      ws.emitOpen();
      ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));

      const ref = client.sendWithStatus({ text: "hi" });
      const lastSent = JSON.parse(ws.sent[ws.sent.length - 1]!);
      expect(lastSent.id).toBe(ref.id);

      ws.emitMessage(JSON.stringify({ kind: "ack", id: ref.id }));

      const updated = client.outbox[0]!;
      expect(updated.status).toBe("sent");
      expect(updated.ackedAt).toBeDefined();
      expect(updated.ackedAt!).toBeGreaterThanOrEqual(updated.sentAt!);

      client.close();
    }),
  );

  it.effect(
    "sendWhileClosed stays `pending` until reconnect, then flushes",
    () =>
      Effect.gen(function* () {
        installMock();
        const client = new PresenceClient<unknown, { text: string }>({
          url: "wss://example.test/parties/Room/lobby",
        });
        client.connect();
        const ws = MockWebSocket.instances[0]!;
        // Don't emitOpen — the socket never reaches OPEN.
        const ref = client.sendWithStatus({ text: "queued" });
        expect(ref.status).toBe("pending");
        expect(ref.sentAt).toBeUndefined();
        expect(client.outbox).toHaveLength(1);

        client.close();
      }),
  );

  it.effect(
    "retry() flips a `failed` message back to `pending` and bumps attempts",
    () =>
      Effect.gen(function* () {
        installMock();
        const client = new PresenceClient<unknown, { text: string }>({
          url: "wss://example.test/parties/Room/lobby",
        });
        client.connect();
        const ws = MockWebSocket.instances[0]!;
        ws.emitOpen();
        ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));

        const ref = client.sendWithStatus({ text: "retry me" });
        client.cancel(ref.id); // demonstrate cancel
        expect(client.outbox[0]!.status).toBe("cancelled");
        client.retry(ref.id); // no-op — cancelled stays cancelled
        expect(client.outbox[0]!.status).toBe("cancelled");

        client.close();
      }),
  );

  it.effect(
    "status reflects the four-way connection lifecycle",
    () =>
      Effect.gen(function* () {
        installMock();
        const client = new PresenceClient<unknown, unknown>({
          url: "wss://example.test/parties/Room/lobby",
        });

        // Pre-connect: offline.
        expect(client.status).toBe("offline");
        client.connect();
        // Just connecting — still offline (open hasn't fired yet).
        expect(client.status).toBe("offline");
        const ws = MockWebSocket.instances[0]!;
        ws.emitOpen();
        // Open + no `init` yet → syncing.
        expect(client.status).toBe("syncing");
        ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));
        // Open + `init` received → online.
        expect(client.status).toBe("online");
        ws.emitClose();
        // Closed mid-session → reconnecting.
        expect(client.status).toBe("reconnecting");

        client.close();
      }),
  );

  it.effect(
    "subscribeOutbox() fires on every status transition",
    () =>
      Effect.gen(function* () {
        installMock();
        const client = new PresenceClient<unknown, { text: string }>({
          url: "wss://example.test/parties/Room/lobby",
        });
        client.connect();
        const ws = MockWebSocket.instances[0]!;
        ws.emitOpen();
        ws.emitMessage(JSON.stringify({ kind: "init", self: "me", peers: {} }));

        const snapshots: number[] = [];
        const unsub = client.subscribeOutbox((outbox) => {
          snapshots.push(outbox.length);
        });
        const a = client.sendWithStatus({ text: "a" });
        const b = client.sendWithStatus({ text: "b" });
        ws.emitMessage(JSON.stringify({ kind: "ack", id: a.id }));
        ws.emitMessage(JSON.stringify({ kind: "ack", id: b.id }));

        unsub();
        client.sendWithStatus({ text: "after unsub" });
        // The unsub'd listener shouldn't fire again.
        expect(snapshots).toContain(2);

        client.close();
      }),
  );
});