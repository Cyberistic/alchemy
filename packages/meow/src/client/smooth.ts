/**
 * Generic interpolation smoother.
 *
 * Smooth a `T` of any shape — `Vec2`, a game character's
 * `{ position, rotation, animationState }`, a stream's audio gain, a
 * pointer's pressure+twist — by providing an interpolator (one global
 * lerp, or per-field lerps for mixed smooth/discrete values).
 *
 * ```typescript
 * import { smoothed, lerpNumber, lerpVec2 } from "meow/client";
 *
 * // Cursor position
 * const cursor = smoothed({
 *   initial: { x: 0, y: 0 },
 *   interpolate: lerpVec2,
 *   duration: 80,
 * });
 * cursor.setTarget({ x: 100, y: 50 });
 *
 * // Game character: smooth position + rotation, snap animation state
 * type Character = {
 *   position: { x: number; y: number; z: number };
 *   rotation: number;
 *   animation: "idle" | "walk" | "run";
 * };
 * const character = smoothed<Character>({
 *   initial: { position: { x: 0, y: 0, z: 0 }, rotation: 0, animation: "idle" },
 *   interpolate: {
 *     position: (a, b, t) => ({
 *       x: lerpNumber(a.x, b.x, t),
 *       y: lerpNumber(a.y, b.y, t),
 *       z: lerpNumber(a.z, b.z, t),
 *     }),
 *     rotation: lerpAngle,
 *     animation: snapLerp, // discrete: pick the closer one
 *   },
 *   duration: 120,
 * });
 * ```
 *
 * The smoother owns a `requestAnimationFrame`-shaped loop (~60 Hz by
 * default) that lerps `value` toward `target` over `duration` ms.
 * Consumers bind `.value` to whatever rendering API they prefer —
 * CSS transform, Canvas2D draw, WebGL uniform, Vue/Solid/Svelte ref,
 * a plain `console.log`.
 */
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Schedule from "effect/Schedule";

/** Default easing — ease-out cubic. Feels snappy at the start, settles at the end. */
export const easeOutCubic = (t: number): number => 1 - Math.pow(1 - t, 3);

/** Linear easing. Useful when the data is already discrete (audio gain, opacity). */
export const linear = (t: number): number => t;

/**
 * Lerp function. Given two values of the same shape and a normalized
 * progress `t` in `[0, 1]`, returns the interpolated value. Use
 * `lerpNumber` for numbers, `lerpVec2` for 2D positions, and provide
 * your own for richer shapes.
 */
export type Lerp<T> = (a: T, b: T, t: number) => T;

/** Linear interpolation for numbers. */
export const lerpNumber: Lerp<number> = (a, b, t) => a + (b - a) * t;

/** Generic 2D vector. */
export interface Vec2 {
  readonly x: number;
  readonly y: number;
}

/** Linear interpolation for 2D positions. */
export const lerpVec2: Lerp<Vec2> = (a, b, t) => ({
  x: lerpNumber(a.x, b.x, t),
  y: lerpNumber(a.y, b.y, t),
});

/**
 * Shortest-path angle interpolation. Angles wrap modulo 2π; naively
 * lerping 350° → 10° spins the long way around (340° back through 0).
 * This picks the shorter arc.
 */
export const lerpAngle: Lerp<number> = (a, b, t) => {
  const TAU = Math.PI * 2;
  let diff = ((b - a) % TAU + TAU) % TAU;
  if (diff > Math.PI) diff -= TAU;
  return a + diff * t;
};

/** Normalise an angle to [0, 2π). */
const normaliseAngle = (x: number): number => {
  const TAU = Math.PI * 2;
  return ((x % TAU) + TAU) % TAU;
};

/**
 * Discrete lerp — picks the closer endpoint once `t` crosses 0.5.
 * Use for animation states, sprite indices, or any value where
 * interpolation doesn't make sense.
 */
export const snapLerp = <T>(a: T, _b: T, t: number): T => (t < 0.5 ? a : _b);

/**
 * Per-field interpolation map. Each key holds a `Lerp` for that
 * field; pass as `interpolate` to a per-shape `smoothed` call.
 */
export type FieldInterpolators<T> = { [K in keyof T]?: Lerp<T[K]> };

export interface SmoothedOptions<T> {
  readonly initial: T;
  /**
   * Either a single `Lerp<T>` that interpolates the whole value, or a
   * record of per-field lerps. If omitted, the smoother requires T to
   * be `number` or `Vec2` (a sensible default is provided).
   */
  readonly interpolate?: Lerp<T> | FieldInterpolators<T>;
  /** Easing duration in milliseconds. Default: 80. */
  readonly duration?: number;
  /**
   * Optional schedule to drive the lerp ticks. Default: `Schedule.spaced("16 millis")`.
   * Override for headless or low-power modes.
   */
  readonly schedule?: Schedule.Schedule<unknown, unknown>;
  /** Tweening function applied to the elapsed/duration ratio. Default: easeOutCubic. */
  readonly easing?: (t: number) => number;
}

const computeT = (elapsed: number, duration: number): number =>
  Math.max(0, Math.min(1, elapsed / duration));

const defaultInterpolateFor = <T>(opts: {
  initial: T;
  interpolate?: Lerp<T> | FieldInterpolators<T>;
}): Lerp<T> => {
  if (opts.interpolate) {
    if (typeof opts.interpolate === "function") {
      return opts.interpolate;
    }
    // Field-wise: build a composite lerp
    const fields = opts.interpolate as FieldInterpolators<T>;
    return (a: T, b: T, t: number): T => {
      const out: Record<string, unknown> = {};
      for (const k of Object.keys(fields)) {
        const lerp = fields[k as keyof T]!;
        out[k] = lerp(
          (a as Record<string, unknown>)[k] as never,
          (b as Record<string, unknown>)[k] as never,
          t,
        );
      }
      return out as T;
    };
  }
  // Sensible defaults for the common cases
  const init = opts.initial;
  if (typeof init === "number") {
    return ((a, b, t) =>
      lerpNumber(a as unknown as number, b as unknown as number, t)) as Lerp<T>;
  }
  if (
    typeof init === "object" &&
    init !== null &&
    "x" in (init as object) &&
    "y" in (init as object) &&
    typeof (init as Record<string, unknown>).x === "number" &&
    typeof (init as Record<string, unknown>).y === "number"
  ) {
    return ((a, b, t) =>
      lerpVec2(
        a as unknown as Vec2,
        b as unknown as Vec2,
        t,
      )) as Lerp<T>;
  }
  throw new Error(
    "smoothed(): cannot infer interpolator for shape — pass `interpolate` explicitly",
  );
};

/** Stateful smoother for any shape `T`. */
export interface Smoothed<T> {
  /** Current rendered value. Mutate-free — re-read each frame. */
  readonly value: T;
  /** Latest target value the smoother is chasing. */
  readonly target: T;
  /** Snap both `value` and `target`. Use for local-input echo. */
  set(value: T): void;
  /** Move the target. `value` will lerp toward it. */
  setTarget(value: T): void;
  /** Halt the loop. Idempotent. */
  stop(): void;
}

export const smoothed = <T>(options: SmoothedOptions<T>): Smoothed<T> => {
  const duration = options.duration ?? 80;
  const easing = options.easing ?? easeOutCubic;
  const schedule = options.schedule ?? Schedule.spaced("16 millis");
  const interpolate = defaultInterpolateFor({
    initial: options.initial,
    interpolate: options.interpolate,
  });

  let value: T = options.initial;
  let target: T = options.initial;
  let lastTick = Date.now();
  let fiber: import("effect/Fiber").Fiber<unknown, unknown> | null = null;

  const tick = Effect.gen(function* () {
    const now = Date.now();
    const elapsed = now - lastTick;
    lastTick = now;
    value = interpolate(value, target, easing(computeT(elapsed, duration)));
    yield* Effect.void;
  });

  fiber = Effect.runFork(tick.pipe(Effect.repeat({ schedule })));

  return {
    get value() {
      return value;
    },
    get target() {
      return target;
    },
    set(v: T) {
      target = v;
      value = v;
    },
    setTarget(v: T) {
      target = v;
    },
    stop() {
      if (fiber) {
        Effect.runFork(Fiber.interrupt(fiber));
        fiber = null;
      }
    },
  };
};

/** Shorthand for the common 2D cursor case. */
export const smoothedCursor = (options: {
  readonly initial?: Vec2;
  readonly duration?: number;
  readonly easing?: (t: number) => number;
} = {}): Smoothed<Vec2> =>
  smoothed<Vec2>({
    initial: options.initial ?? { x: 0, y: 0 },
    interpolate: lerpVec2,
    duration: options.duration,
    easing: options.easing,
  });

/**
 * Multi-peer registry of smoothers. Useful when a single render loop
 * needs to draw N peers at once.
 *
 * ```typescript
 * type Player = { position: Vec2; hp: number; name: string };
 * const players = peerSmoothers<Player>({
 *   initial: { position: { x: 0, y: 0 }, hp: 100, name: "" },
 *   interpolate: {
 *     position: lerpVec2,
 *     hp: lerpNumber,
 *     name: snapLerp, // discrete — names shouldn't interpolate
 *   },
 *   duration: 100,
 * });
 *
 * ws.addEventListener("message", (event) => {
 *   const msg = JSON.parse(event.data);
 *   if (msg.kind === "presence") players.upsert(msg.id).setTarget(msg.state);
 *   if (msg.kind === "absence") players.delete(msg.id);
 * });
 * ```
 */
export interface PeerSmoothers<T> {
  readonly entries: () => IterableIterator<readonly [string, Smoothed<T>]>;
  upsert(peerId: string): Smoothed<T>;
  delete(peerId: string): void;
  clear(): void;
}

export const peerSmoothers = <T>(
  options: SmoothedOptions<T>,
): PeerSmoothers<T> => {
  const map = new Map<string, Smoothed<T>>();
  return {
    entries: () => map.entries(),
    upsert(peerId: string) {
      let s = map.get(peerId);
      if (!s) {
        s = smoothed(options);
        map.set(peerId, s);
      }
      return s;
    },
    delete(peerId: string) {
      const s = map.get(peerId);
      if (s) {
        s.stop();
        map.delete(peerId);
      }
    },
    clear() {
      for (const s of map.values()) s.stop();
      map.clear();
    },
  };
};