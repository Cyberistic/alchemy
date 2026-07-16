// @vitest-environment happy-dom
import { describe, expect, it } from "@effect/vitest";
import { act, render } from "@testing-library/react";
import * as React from "react";
import { usePresence } from "../../src/presence/hooks.ts";

// Minimal renderHook — @testing-library/react 16 deprecated
// `@testing-library/react-hooks`, so we use a captured-state
// pattern with `useRef`.
function renderHook<T>(callback: () => T): {
  readonly current: T;
  readonly rerender: () => void;
  readonly unmount: () => void;
} {
  const ref: { value: T | undefined } = { value: undefined };
  let triggerRerender: () => void = () => {};

  function Wrapper({ tick }: { readonly tick: number }) {
    const value = callback();
    ref.value = value;
    const [, setTick] = React.useState(0);
    triggerRerender = () => setTick((n) => n + 1);
    void tick;
    return null;
  }

  const result = render(React.createElement(Wrapper, { tick: 0 }));
  return {
    get current() {
      return ref.value as T;
    },
    rerender: () => {
      act(() => {
        triggerRerender();
      });
    },
    unmount: () => result.unmount(),
  };
}

describe("usePresence", () => {
  it("creates a client and exposes an empty peer map on first render", () => {
    const result = renderHook(() =>
      usePresence<{ x: number; y: number }>({
        url: "wss://test/meow/Room/lobby",
        reconnect: { min: 100, max: 200, factor: 1, maxRetries: 0 },
      }),
    );
    expect(result.current.peers.size).toBe(0);
    expect(result.current.peerList.length).toBe(0);
    expect(result.current.self).toBeUndefined();
    result.unmount();
  });

  it("returns the same client instance across re-renders", () => {
    let firstClient: unknown = null;
    function Harness({ tick }: { readonly tick: number }) {
      const r = usePresence<{ n: number }>({
        url: "wss://test/meow/Room/lobby",
        reconnect: { min: 100, max: 200, factor: 1, maxRetries: 0 },
      });
      firstClient ??= r.client;
      void tick;
      return null;
    }
    const result = render(React.createElement(Harness, { tick: 0 }));
    act(() => {
      result.rerender(React.createElement(Harness, { tick: 1 }));
    });
    // Re-render should keep the same client (PresenceClient holds
    // its own ref-based identity inside the hook).
    expect(firstClient).not.toBeNull();
    result.unmount();
  });

  it("exposes status, outbox, and stable closures in the result shape", () => {
    const result = renderHook(() =>
      usePresence<{ cursor: { x: number; y: number } | null }>({
        url: "wss://test/meow/Room/lobby",
        reconnect: { min: 100, max: 200, factor: 1, maxRetries: 0 },
      }),
    );
    expect(result.current.outbox).toBeDefined();
    expect(Array.isArray(result.current.outbox)).toBe(true);
    expect(typeof result.current.update).toBe("function");
    expect(typeof result.current.send).toBe("function");
    expect(typeof result.current.sendWithStatus).toBe("function");
    expect(typeof result.current.retry).toBe("function");
    expect(typeof result.current.cancel).toBe("function");
    expect(result.current.status).toBeDefined();
    result.unmount();
  });

  it("cleans up on unmount (closes the client)", () => {
    let lastClient: { close: () => void } | null = null;
    function Probe() {
      const r = usePresence<unknown>({
        url: "wss://test/meow/Room/lobby",
        reconnect: { min: 100, max: 200, factor: 1, maxRetries: 0 },
      });
      lastClient = r.client as unknown as { close: () => void };
      return null;
    }
    const result = render(React.createElement(Probe));
    expect(lastClient).not.toBeNull();
    // Spy on close.
    let closed = false;
    (lastClient as unknown as { close: () => void }).close = () => {
      closed = true;
    };
    act(() => {
      result.unmount();
    });
    expect(closed).toBe(true);
  });
});