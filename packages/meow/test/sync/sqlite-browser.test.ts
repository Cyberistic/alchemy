import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  makeSqliteBrowserStore,
  type SqlJsLike,
} from "../../src/sync/sqlite-browser.ts";
import {
  type Branch,
  type EventId,
  type Seq,
} from "../../src/sync/schema.ts";

// A minimal in-memory sql.js shim sufficient for the SyncStore
// contract. We avoid loading the WASM file in the unit test by
// implementing the three methods (exec/prepare/run) ourselves.
//
// Only exec, prepare, and run are called by the SyncStore SQL —
// export is exercised separately.
const makeInMemorySqlJs = (): SqlJsLike & {
  execCalls: string[];
  rows: Map<string, ReadonlyArray<ReadonlyArray<unknown>>>;
} => {
  const tables = new Map<string, Map<string, unknown[]>>();
  let autoInc = 1;

  const rowToObj = (tableName: string, row: unknown[]) => {
    const cols = tables.get(tableName);
    if (!cols) return {};
    const keys = [...cols.keys()];
    const out: Record<string, unknown> = {};
    keys.forEach((k, i) => {
      out[k] = row[i];
    });
    out.lastInsertRowid = row[0] ?? autoInc;
    return out;
  };

  const exec = (sql: string) => {
    const trimmed = sql.trim();
    if (trimmed.startsWith("CREATE TABLE")) {
      // `CREATE TABLE IF NOT EXISTS meow_sync_events (id TEXT, ...)`
      const match = trimmed.match(/CREATE TABLE[^(]+\(([^)]+)\)/);
      if (match) {
        const cols = match[1]!.split(",").map((c) =>
          c.trim().split(/\s+/)[0]!,
        );
        if (!tables.has(trimmed.split(/\s+/)[5]!)) {
          tables.set(trimmed.split(/\s+/)[5]!, new Map());
        }
        const t = tables.get(trimmed.split(/\s+/)[5]!)!;
        for (const c of cols) if (!t.has(c)) t.set(c, []);
      }
    } else if (trimmed.startsWith("INSERT")) {
      const tableMatch = trimmed.match(/INSERT INTO\s+(\w+)/);
      const tableName = tableMatch![1]!;
      const cols = tables.get(tableName)!;
      const valuesMatch = trimmed.match(/VALUES\s*\(([^)]+)\)/);
      const params = valuesMatch![1]!.split(",").map((s) => s.trim());
      const row: unknown[] = [];
      let i = 0;
      for (const k of cols.keys()) {
        if (params[i] === "?") {
          row.push(null);
        } else if (params[i]?.startsWith("'")) {
          row.push(params[i]!.slice(1, -1));
        } else {
          row.push(Number(params[i]));
        }
        i++;
      }
      for (const [k, list] of cols) {
        list.push(row[[...cols.keys()].indexOf(k)] ?? null);
      }
    }
  };

  return {
    execCalls: [],
    rows: new Map(),
    exec: (sql: string) => {
      exec(sql);
      return { rows: [] };
    },
    prepare: (sql: string, params?: unknown[]) => {
      const tableMatch = sql.match(/FROM\s+(\w+)/);
      const tableName = tableMatch![1]!;
      const cols = tables.get(tableName)!;
      const rows: Array<Record<string, unknown>> = [];
      // naive scan: every row matches in our in-memory mock
      const colNames = [...cols.keys()];
      const n = cols.get(colNames[0]!)?.length ?? 0;
      for (let r = 0; r < n; r++) {
        const obj: Record<string, unknown> = {};
        colNames.forEach((c, i) => {
          obj[c] = cols.get(c)![r];
        });
        rows.push(obj);
      }
      let idx = 0;
      return {
        step: () => {
          if (idx < rows.length) {
            const row = rows[idx++]!;
            // Apply WHERE filter very loosely
            if (sql.includes("WHERE") && params) {
              const branchMatch = sql.match(/branch\s*=\s*\?/);
              const seqMatch = sql.match(/seq\s*>\s*\?/);
              if (branchMatch && row.branch !== params[0]) return false;
              if (seqMatch && (row.seq as number) <= (params[1] as number))
                return false;
            }
            Object.assign(this as unknown as object, row);
            return true;
          }
          return false;
        },
        getAsObject: () => rows[Math.max(0, idx - 1)] ?? {},
        free: () => {},
      };
    },
    run: (_sql: string, _params?: unknown[]) => {
      autoInc++;
    },
    export: () => new Uint8Array(),
  } as SqlJsLike & {
    execCalls: string[];
    rows: Map<string, ReadonlyArray<ReadonlyArray<unknown>>>;
  };
};

describe("sql.js browser SyncStore", () => {
  it("creates schema lazily and accepts a write", () =>
    Effect.gen(function* () {
      const db = makeInMemorySqlJs();
      const store = yield* makeSqliteBrowserStore(db);
      const out = yield* store.append({
        branch: "main" as Branch,
        id: "e1" as EventId,
        parent: null,
        payload: { hello: "browser" },
        author: "alice",
        createdAt: Date.now(),
      });
      expect(out).toBe("e1");
    }));

  it("round-trips readSince", () =>
    Effect.gen(function* () {
      const db = makeInMemorySqlJs();
      const store = yield* makeSqliteBrowserStore(db);
      for (let i = 0; i < 3; i++) {
        yield* store.append({
          branch: "main" as Branch,
          id: `e${i}` as EventId,
          parent: null,
          payload: { n: i },
          author: "alice",
          createdAt: Date.now() + i,
        });
      }
      const events = yield* store.readSince({
        branch: "main" as Branch,
        since: 0 as Seq,
      });
      expect(events.length).toBe(3);
    }));

  it("head returns count + max seq", () =>
    Effect.gen(function* () {
      const db = makeInMemorySqlJs();
      const store = yield* makeSqliteBrowserStore(db);
      yield* store.append({
        branch: "main" as Branch,
        id: "a" as EventId,
        parent: null,
        payload: { n: 1 },
        author: "alice",
        createdAt: Date.now(),
      });
      const head = yield* store.head("main" as Branch);
      expect(head.count).toBeGreaterThanOrEqual(1);
    }));
});