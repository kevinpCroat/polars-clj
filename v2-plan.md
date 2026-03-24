# polars-clj v2 Plan: Multi-Backend Clojure Query DSL

## What Changed From v1

The v1 plan proposed a Polars-only Rust JNI bridge. The critique exposed:

1. **The user base for "Polars via JNI" is too narrow.** Clojure devs wanting fast DataFrames without Python can already use DuckDB + HoneySQL.
2. **The effort is 2x what v1 estimated** (1,800-2,500 LOC Rust, 7-9 weeks) for a single-engine backend.
3. **The strongest version of this project is engine-agnostic.** Our pipeline-vector DSL `[[:scan-csv ...] [:filter ...] [:collect]]` is already backend-independent in shape. The v1 plan accidentally designed a multi-backend DSL and then hardcoded it to one backend.

v2 reframes the project: **a Clojure query DSL (like Python's Ibis) that compiles to multiple backends** — starting with DuckDB (easiest, most value), then Polars, then DataFusion.

---

## Approach Evaluation — Rank Ordered

### 1. DuckDB via JDBC (Recommended First Backend)

**What:** Compile our pipeline DSL to SQL, execute via DuckDB's native JDBC driver.

| Dimension | Assessment |
|---|---|
| **Effort** | ~200-400 LOC Clojure. No Rust. No JNI. No native libs to manage. |
| **Performance** | DuckDB is comparable to Polars. Vectorized columnar engine, parallel execution. |
| **Distribution** | Single Maven dep (`org.duckdb/duckdb_jdbc`). Works on all platforms out of the box. |
| **Ecosystem** | HoneySQL already compiles Clojure data to SQL. We can extend or build on this pattern. |
| **Maintenance** | DuckDB has a stable SQL interface. SQL doesn't break between versions. |
| **User base** | Broad. Every Clojure dev who uses `next.jdbc` can adopt this immediately. |
| **Joins** | Free — SQL has `JOIN`. |
| **`from-maps`** | DuckDB can ingest JSON, Arrow, CSV. Type inference is DuckDB's problem. |

**Pros:**
- Lowest effort, highest immediate value
- No native compilation, no Python, no cross-platform headaches
- SQL is a stable interface — immune to Polars' breaking Rust API changes
- HoneySQL proves the "Clojure data -> SQL" pattern works
- DuckDB handles type inference, joins, window functions, CTEs — all for free
- JDBC is a first-class JVM citizen; Clojure has excellent JDBC tooling

**Cons:**
- Expression API is SQL-shaped, not DataFrame-shaped (some operations are awkward in SQL)
- DuckDB's lazy evaluation is implicit (query optimizer), not explicit (LazyFrame)
- Users who specifically want Polars' expression API won't get it
- SQL string generation has injection risks if not careful (parameterize everything)

**How it works:** Our `pipeline.clj` compiles `[:filter [:> :salary 50000]]` to `WHERE salary > 50000`. The `[:group-by [:dept] :agg [[:mean :salary]]]` becomes `SELECT dept, AVG(salary) FROM ... GROUP BY dept`. Execute via `next.jdbc` against an in-process DuckDB instance.

### 2. Wrap scala-polars (Fastest Path to Polars Without Python)

**What:** Use scala-polars' existing JNI bridge from Clojure via Java interop.

| Dimension | Assessment |
|---|---|
| **Effort** | ~200 LOC Clojure interop wrapper. Zero Rust. |
| **Performance** | Identical to custom JNI — same Polars Rust engine underneath. |
| **Distribution** | Depends on scala-polars' Maven artifacts. |
| **Maintenance** | We don't maintain the JNI bridge. But scala-polars may be unmaintained. |
| **Risk** | Single-maintainer project, pinned to Polars 0.35. If abandoned, we inherit a dead dependency. |

**Pros:**
- Get Polars on the JVM today with ~200 lines of Clojure
- JNI bridge is already built, tested, and handles the hard problems (memory, types, errors)
- Validates whether anyone actually wants Polars-on-JVM before investing in custom Rust

**Cons:**
- scala-polars appears unmaintained (last updated 2023, targets Polars 0.35)
- Polars 0.35 → 0.53 is a huge gap; many features missing
- Scala dependency adds JAR bloat and potential version conflicts
- If it's abandoned, wrapping it buys us nothing long-term
- No control over the native bridge — can't fix bugs or add features

**Verdict:** Worth a 1-day spike to test. If scala-polars is alive and on a recent Polars version, this is the fastest path. If it's dead (likely), skip to option 3 or 4.

### 3. Custom Polars JNI Bridge (v1 Plan, Revised)

**What:** The v1 plan, with corrections from the critique.

| Dimension | Assessment |
|---|---|
| **Effort** | ~1,800-2,500 LOC Rust + ~300 LOC Clojure. 7-9 weeks. |
| **Performance** | Best possible — direct Rust, no intermediary. |
| **Distribution** | Platform-specific JARs (4 targets). Cross-compilation is the hard part. |
| **Maintenance** | Must track Polars Rust API (breaks without deprecation). |

**Revised scope (from critique):**
- **Add joins** — without them the library is a toy
- **Handle lifecycle: clone-on-use** — Rust clones LazyFrame on each operation, JVM side manages handles via `Closeable` + `Cleaner` safety net
- **`from-maps`: require explicit schema** for MVP — type inference is a 500-line rabbit hole. Add auto-inference later.
- **Budget 7-9 weeks**, not 4-6
- **Prototype Panama alongside JNI** in week 1 (JDK 25 LTS has stable Panama)
- **Rust line estimate: 1,800-2,500**, not 950
- **Testing strategy:** mirror existing 22 Clojure tests + memory leak test (10K pipelines, check RSS)
- **REPL ergonomics:** `Handle` deftype with `toString` showing schema/shape via a JNI call

**Pros:**
- No Python dependency
- Best performance for Polars-specific workloads
- Full control over the bridge

**Cons:**
- Highest effort by far
- Cross-platform build is the riskiest milestone
- Few Clojure contributors know Rust
- Polars Rust API instability = ongoing maintenance tax

### 4. Polars via Arrow Flight Sidecar

**What:** Run Polars as a lightweight Rust binary. Communicate via Arrow Flight (gRPC + Arrow IPC).

| Dimension | Assessment |
|---|---|
| **Effort** | ~800 LOC Rust (server) + ~400 LOC Clojure (client). 5-7 weeks. |
| **Performance** | ~1-5ms per-query latency for gRPC round-trip. Zero-copy for results via Arrow IPC. |
| **Distribution** | Ship a Rust binary (same cross-platform challenge as JNI). |
| **Maintenance** | Polars Rust API tracking (same as option 3). |

**Pros:**
- Clean process isolation — Rust crash doesn't take down JVM
- No `unsafe` code, no handle lifecycle, no JNI complexity
- Arrow IPC gives efficient data transfer without JNI bulk-array gymnastics
- Easier to debug (separate processes, standard gRPC tooling)
- Could eventually support remote execution / connection pooling

**Cons:**
- Process lifecycle management (start/stop sidecar)
- Same cross-platform build challenge as JNI
- ~1-5ms latency per query (fine for analytics, bad for tight loops)
- Users must accept a background process
- More moving parts than in-process JNI

### 5. GraalPy (Zero Effort If It Works)

**What:** Run the existing polars-clj codebase unchanged on GraalVM's Python implementation.

| Dimension | Assessment |
|---|---|
| **Effort** | Near zero if it works. ~1 day to test. |
| **Performance** | Unknown — GraalPy's JIT may be slower than CPython for Polars' native extensions. |
| **Risk** | High — Polars uses Rust native extensions via PyO3. GraalPy's C extension compatibility is incomplete. |

**Pros:**
- If it works, we're done. No code changes.
- Stays on the Python API (stable, well-documented)

**Cons:**
- Very likely doesn't work today — Polars' PyO3 extensions require CPython ABI compatibility
- Requires GraalVM (not standard OpenJDK)
- Performance is unpredictable
- Ties us to GraalVM's release cycle

**Verdict:** Worth a 2-hour test. If `import polars` works on GraalPy, this is the answer. If not (almost certain), move on.

### 6. Substrait IR (Engine-Agnostic, Long-Term Play)

**What:** Compile DSL to Substrait — a standardized cross-language query plan format consumed by DuckDB, DataFusion, Velox, and others.

| Dimension | Assessment |
|---|---|
| **Effort** | ~600-1000 LOC Clojure (Substrait plan generation) + engine-specific execution adapters. |
| **Performance** | Depends on the engine. DuckDB/DataFusion are both fast. |
| **Ecosystem** | Substrait is backed by Voltron Data (Arrow/Ibis maintainers). Active development. |
| **Coverage** | Filter, select, sort, group-by, join, basic expressions — all covered. Window functions and advanced Polars-specific ops are not. |

**Pros:**
- True engine independence — same DSL targets DuckDB, DataFusion, Polars, Velox
- Substrait is a real standard with industry backing
- Eliminates Polars version tracking problem entirely
- Future-proof: new engines that support Substrait get automatic support

**Cons:**
- Substrait Java SDK maturity is unclear
- Polars' Substrait support is experimental/incomplete
- Adds an abstraction layer (DSL → Substrait → engine) with potential for impedance mismatch
- Some Polars-specific operations (rolling, over, melt) have no Substrait equivalent
- Debugging through two layers of plan transformation is harder

---

## Corrected Assumptions (from v1 critique)

| v1 Assumption | v2 Correction |
|---|---|
| ~950 LOC Rust | **1,800-2,500 LOC** — JNI boilerplate is ~15-25 lines per function |
| 4-6 weeks | **7-9 weeks** — cross-compilation is 2-3 weeks alone |
| Panama "too early" | **Re-evaluate** — JDK 25 LTS (stable since Sep 2025) has finalized Panama |
| scala-polars is copy-paste template | **Architectural reference only** — targets Polars 0.35, we need 0.53+ |
| 2-10x data transfer improvement | **1.5-3x numeric, 3-8x string-heavy** — column-by-column JNI still copies |
| `from-maps` is trivial | **300-500 LOC for type inference**, or require explicit schemas |
| Handle lifecycle handled by Cleaner | **Clone-on-use required** — users can reuse intermediate LazyFrames |
| No joins needed for MVP | **Joins are required** — without them it's a toy |
| No testing strategy needed | **Must include** memory leak tests, cross-platform CI, mirror existing 22 tests |

---

## Technical Gaps Now Addressed

### Handle Lifecycle: Clone-on-Use

Every bridge operation clones the input LazyFrame before transforming:

```rust
fn lazy_filter(lf_ptr: jlong, expr_ptr: jlong) -> jlong {
    let lf = unsafe { from_ptr::<LazyFrame>(lf_ptr) };
    let expr = unsafe { from_ptr::<Expr>(expr_ptr) };
    let new_lf = lf.clone().filter(expr.clone());  // clone, don't consume
    to_ptr(new_lf)
}
```

JVM side: handles are reference-counted or cleaned via `Cleaner`. Users can safely reuse intermediates:

```clojure
(let [base (pl/scan-csv "data.csv")
      a (pl/collect (pl/filter base [:> :salary 50000]))
      b (pl/collect (pl/select base [:name]))]  ;; safe — base was cloned
  ...)
```

### `from-maps`: Require Explicit Schema for MVP

```clojure
;; MVP: explicit schema
(pl/from-maps [{:name "Alice" :age 30}]
  :schema {:name :utf8 :age :int64})

;; Post-MVP: auto-inference (scan first N rows, infer types)
(pl/from-maps [{:name "Alice" :age 30}])
```

### Joins in MVP

```clojure
;; DSL extension
[:join :left other-lf :on [:= :id :other-id]]

;; Threading API
(pl/join lf other-lf :how :left :on [:= :id :other-id])
```

For JNI: `lazyJoin(long lf1, long lf2, long onExpr, String how) -> long`
For DuckDB: `LEFT JOIN other ON id = other_id`

### Error Messages: Structured Errors

Wrap `PolarsError` in a custom exception class with structured fields:

```rust
// Instead of RuntimeException("ComputeError: column 'salry' not found")
// Throw PolarsException with fields:
//   errorType: "ComputeError"
//   message: "column 'salry' not found"
//   context: "during filter step"
```

Clojure side catches and wraps in `ex-info` with full context:

```clojure
(catch PolarsException e
  (throw (ex-info (.getMessage e)
                  {:step :filter
                   :error-type (.getErrorType e)
                   :expr expr-form})))
```

### REPL Ergonomics

Handle's `toString` calls a JNI function to get schema/shape:

```clojure
;; Instead of: #object[Handle 0x... "ptr=140234567890"]
;; Show:       #<LazyFrame [salary: Int64, name: Utf8, ...] (lazy)>
```

For DuckDB backend: `DESCRIBE` query returns schema.

---

## Recommended Implementation Order

### Phase 1: DuckDB Backend (Weeks 1-3) — Highest Value, Lowest Risk

**Why first:** Broadest user base, zero native code, proves the multi-backend architecture.

1. **Week 1:** SQL compiler — translate pipeline vectors to SQL strings
   - `[:filter [:> :salary 50000]]` → `WHERE salary > 50000`
   - `[:select [:name :salary]]` → `SELECT name, salary`
   - `[:group-by [:dept] :agg [[:mean :salary]]]` → `SELECT dept, AVG(salary) ... GROUP BY dept`
   - `[:sort :salary {:descending true}]` → `ORDER BY salary DESC`
   - `[:limit 10]` → `LIMIT 10`
   - `[:join ...]` → `LEFT JOIN ... ON ...`

2. **Week 2:** DuckDB integration
   - In-process DuckDB via JDBC (`org.duckdb/duckdb_jdbc`)
   - `scan-csv` → `CREATE TABLE ... AS SELECT * FROM read_csv('...')`
   - `scan-parquet` → `SELECT * FROM read_parquet('...')`
   - `from-maps` → insert via prepared statements or Arrow ingestion
   - `collect` → execute SQL, convert ResultSet to keyword maps
   - `explain` → `EXPLAIN` prefix

3. **Week 3:** Backend switching + tests
   - `(pl/set-backend! :duckdb)` vs `(pl/set-backend! :python)`
   - Port all 22 existing tests to run against both backends
   - Add join tests

**Deliverable:** Same Clojure DSL, two backends (Python/Polars and DuckDB), all tests green on both.

### Phase 2: Validate scala-polars (1 Day Spike)

Before investing in custom Rust:

1. Add `scala-polars` Maven dep
2. Try calling its Java-facing API from Clojure
3. Check: what Polars version? What operations are supported? Is it maintained?

**If alive and recent:** Write a thin Clojure wrapper (~200 LOC), add as third backend.
**If dead (likely):** Document findings, move to Phase 3.

### Phase 3: Custom JNI/Panama Bridge (Weeks 4-12) — Only If Justified

Only pursue if:
- DuckDB backend proves the DSL has users
- Users specifically request Polars-native execution
- scala-polars is not viable

1. **Week 4:** Prototype both JNI (`jni-rs`) and Panama (`jextract`) against one operation (`scan-csv` + `collect`). Pick the winner.
2. **Weeks 5-6:** Expression construction + LazyFrame operations (1,800-2,500 LOC Rust)
3. **Week 7:** Data transfer (column-by-column bulk arrays)
4. **Weeks 8-9:** Handle lifecycle (clone-on-use, Cleaner safety net, REPL repr)
5. **Weeks 10-12:** Cross-platform build, CI, platform-specific JARs

### Phase 4 (Stretch): Substrait IR

If Phase 1-3 prove the multi-backend DSL has traction:

1. Compile DSL to Substrait plans instead of per-engine calls
2. Add DataFusion as a third native backend
3. Any future Substrait-compatible engine gets automatic support

---

## Architecture: Multi-Backend DSL

```
┌─────────────────────────────────────────────────┐
│  Clojure DSL Layer (UNCHANGED)                  │
│                                                 │
│  core.clj       ── Public API                   │
│  expr.clj       ── Expression data structures   │
│  pipeline.clj   ── Pipeline dispatch            │
└──────────┬──────────────────────────────────────┘
           │  (backend protocol)
           ▼
┌──────────────────┬──────────────────┬───────────────────┐
│  :python         │  :duckdb         │  :native          │
│  (current)       │  (Phase 1)       │  (Phase 3)        │
│                  │                  │                    │
│  libpython-clj   │  next.jdbc +     │  JNI/Panama +     │
│  → Polars Python │  DuckDB JDBC     │  Polars Rust      │
│                  │  → SQL engine    │  → direct engine   │
└──────────────────┴──────────────────┴───────────────────┘
```

The backend protocol is a Clojure protocol or multimethod:

```clojure
(defprotocol Backend
  (scan-csv* [backend path opts])
  (execute-expr* [backend expr-form])
  (filter* [backend lf expr])
  (select* [backend lf exprs])
  (collect* [backend lf opts])
  ;; ...
)
```

Each backend implements this protocol. `pipeline.clj` dispatches to the active backend. `core.clj` stays unchanged.

---

## Decision Matrix

| Criterion | DuckDB JDBC | Wrap scala-polars | Custom JNI | Arrow Flight | GraalPy | Substrait |
|---|---|---|---|---|---|---|
| **Effort** | Low (2-3 wk) | Very low (1 day) | High (7-9 wk) | Medium (5-7 wk) | Near zero | Medium (4-6 wk) |
| **Risk** | Low | High (abandoned?) | Medium | Medium | Very high | Medium |
| **Performance** | Excellent | Excellent | Best | Good (+1-5ms) | Unknown | Depends on engine |
| **No Python** | Yes | Yes | Yes | Yes | No (GraalVM) | Yes |
| **Joins** | Free (SQL) | If supported | Must build | Must build | Free (Python) | Free (Substrait) |
| **Maintenance** | Low (stable SQL) | None (not ours) | High (Rust API) | High (Rust API) | Low | Medium |
| **User base** | Broad | Narrow | Narrow | Narrow | Narrow | Broad (future) |
| **Distribution** | Maven JAR | Maven JAR | Platform JARs | Platform binary | GraalVM only | Maven JAR |

**Recommendation: DuckDB first, validate demand, then invest in native backends.**

---

## Summary

The v1 plan designed a good DSL but coupled it to a single expensive backend. v2 keeps the DSL, starts with the easiest backend (DuckDB), and only invests in custom Rust when demand justifies it.

The rank-ordered implementation path:
1. **DuckDB via JDBC** — 2-3 weeks, broadest value, zero native code
2. **Validate scala-polars** — 1 day spike, rules in or out the fast path to Polars
3. **Custom JNI/Panama bridge** — 7-9 weeks, only if users need native Polars
4. **Substrait IR** — engine-agnostic future, only if multi-backend proves its worth

This is the Ibis model: one DSL, many engines. The existing pipeline-vector format was already engine-agnostic — we just need to stop hardcoding the backend.
