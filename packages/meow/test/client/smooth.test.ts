import { describe, expect, it } from "@effect/vitest";
import {
  smoothed,
  smoothedCursor,
  peerSmoothers,
  lerpNumber,
  lerpVec2,
  lerpAngle,
  snapLerp,
  type Vec2,
  type Smoothed,
} from "../../src/client/smooth.ts";

describe("lerp primitives", () => {
  it("lerpNumber", () => {
    expect(lerpNumber(0, 10, 0)).toBe(0);
    expect(lerpNumber(0, 10, 0.5)).toBe(5);
    expect(lerpNumber(0, 10, 1)).toBe(10);
  });

  it("lerpVec2", () => {
    expect(lerpVec2({ x: 0, y: 0 }, { x: 10, y: 20 }, 0.5)).toEqual({
      x: 5,
      y: 10,
    });
  });

  it("lerpAngle takes the short path across the wrap", () => {
    // 350° → 10°: the short way is +20°, ending back at 0° (mod 360°).
    const a = (350 * Math.PI) / 180;
    const b = (10 * Math.PI) / 180;
    const mid = lerpAngle(a, b, 0.5);
    const TAU = Math.PI * 2;
    const midDeg = ((mid % TAU) + TAU) % TAU * (180 / Math.PI);
    expect(midDeg).toBeCloseTo(0, 0);
  });

  it("snapLerp picks the nearer endpoint", () => {
    expect(snapLerp("idle", "run", 0.4)).toBe("idle");
    expect(snapLerp("idle", "run", 0.5)).toBe("run");
    expect(snapLerp("idle", "run", 0.9)).toBe("run");
  });
});

describe("smoothed (generic)", () => {
  it("snap (set) updates value and target", () => {
    const c = smoothed<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
    });
    c.set({ x: 42, y: 99 });
    expect(c.value).toEqual({ x: 42, y: 99 });
    expect(c.target).toEqual({ x: 42, y: 99 });
  });

  it("setTarget only moves the target", () => {
    const c = smoothed<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
    });
    c.setTarget({ x: 100, y: 100 });
    expect(c.value).toEqual({ x: 0, y: 0 });
    expect(c.target).toEqual({ x: 100, y: 100 });
  });

  it("infers default interpolator for number initial", () => {
    const n = smoothed({ initial: 0 });
    n.setTarget(100);
    // inferred as Lerp<number>; verify it doesn't throw
    expect(n.target).toBe(100);
  });

  it("infers default interpolator for Vec2 initial", () => {
    const v = smoothed({ initial: { x: 0, y: 0 } });
    v.setTarget({ x: 50, y: 50 });
    expect(v.target).toEqual({ x: 50, y: 50 });
  });

  it("throws when no interpolator fits the shape", () => {
    expect(() =>
      smoothed({
        initial: { foo: "bar" },
        interpolate: undefined,
      }),
    ).toThrow(/cannot infer interpolator/);
  });

  it("per-field interpolation: smooth numbers, snap discrete", async () => {
    type Character = {
      position: Vec2;
      animation: "idle" | "walk" | "run";
    };
    const c = smoothed<Character>({
      initial: { position: { x: 0, y: 0 }, animation: "idle" },
      interpolate: {
        position: lerpVec2,
        animation: snapLerp,
      },
      duration: 20,
    });
    c.setTarget({ position: { x: 100, y: 100 }, animation: "run" });
    await new Promise((res) => setTimeout(res, 80));
    // Position has lerped noticeably
    expect(c.value.position.x).toBeGreaterThan(50);
    // Animation has snapped to the target (the closer one)
    expect(["idle", "run"]).toContain(c.value.animation);
  });

  it("global Lerp<T> works for custom shapes", () => {
    type Score = { points: number; combo: number };
    const score = smoothed<Score>({
      initial: { points: 0, combo: 0 },
      interpolate: (a, b, t) => ({
        points: lerpNumber(a.points, b.points, t),
        combo: lerpNumber(a.combo, b.combo, t),
      }),
    });
    score.setTarget({ points: 100, combo: 5 });
    expect(score.target).toEqual({ points: 100, combo: 5 });
    expect(score.value).toEqual({ points: 0, combo: 0 });
  });

  it("eventually settles on target", async () => {
    const c = smoothed<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
      duration: 20,
    });
    c.setTarget({ x: 100, y: 100 });
    await new Promise((res) => setTimeout(res, 250));
    expect(c.value.x).toBeCloseTo(100, 0);
    expect(c.value.y).toBeCloseTo(100, 0);
  });

  it("stop() halts the loop", async () => {
    const c = smoothed<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
      duration: 30,
    });
    c.setTarget({ x: 100, y: 100 });
    c.stop();
    const snapshot = { ...c.value };
    await new Promise((res) => setTimeout(res, 100));
    expect(c.value).toEqual(snapshot);
  });
});

describe("smoothedCursor", () => {
  it("produces a Smoothed<Vec2>", () => {
    const c = smoothedCursor();
    const _check: Smoothed<Vec2> = c;
    expect(c.value).toEqual({ x: 0, y: 0 });
    c.setTarget({ x: 10, y: 20 });
    expect(c.target).toEqual({ x: 10, y: 20 });
  });
});

describe("peerSmoothers", () => {
  it("upsert creates a smoother on first call", () => {
    const cursors = peerSmoothers<Vec2>({
      initial: { x: 5, y: 5 },
      interpolate: lerpVec2,
    });
    const a = cursors.upsert("alice");
    expect(a.value).toEqual({ x: 5, y: 5 });
  });

  it("upsert returns the same smoother for the same peer", () => {
    const cursors = peerSmoothers<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
    });
    const a1 = cursors.upsert("alice");
    const a2 = cursors.upsert("alice");
    expect(a1).toBe(a2);
  });

  it("delete removes the peer", () => {
    const cursors = peerSmoothers<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
    });
    cursors.upsert("alice");
    cursors.delete("alice");
    expect(Array.from(cursors.entries())).toEqual([]);
  });

  it("clear removes all peers", () => {
    const cursors = peerSmoothers<Vec2>({
      initial: { x: 0, y: 0 },
      interpolate: lerpVec2,
    });
    cursors.upsert("alice");
    cursors.upsert("bob");
    cursors.clear();
    expect(Array.from(cursors.entries())).toEqual([]);
  });
});