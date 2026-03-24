# Critique: rust-direct.md — Polars JNI Bridge Plan

## 1. Assumptions That May Be Wrong

### Rust line estimate too low (~950 → ~1,800-2,500)

The plan allocates 150 lines for `jni/expr.rs`. The DSL supports 27 distinct expression forms. Each JNI function requires the `#[no_mangle] pub extern "system"` signature, JNIEnv parameter unpacking, `catch_unwind` wrapping, error-to-exception conversion, and the actual Polars call. The plan's own error handling example is 15 lines for a single function. At ~15-25 lines per JNI entry point, 27 expression functions alone need 400-675 lines, not 150.

**Realistic total: 1,800-2,500 lines of Rust for the MVP scope described.**

### Timeline optimistic (4-6 weeks → 7-9 weeks)

M0-M2 (weeks 1-3) are plausible for someone fluent in both Rust and Clojure. But M4 (cross-platform build and distribution) is where projects like this blow up. Cross-compiling Polars pulls in `zstd-sys`, `lz4-sys`, `brotli-sys` for compression — each a multi-day fight with linker errors across 4 target triples. Budget 2-3 weeks for M4 alone.

**Realistic total: 7-9 weeks.**

### Panama dismissal is outdated (as of March 2026)

The plan says Panama is "too early" and "requires JDK 22+." As of March 2026:
- JDK 25 LTS (Sep 2025) has Panama as a stable, non-preview feature and has been available for ~6 months.
- The Clojure ecosystem targets JDK 21+ as baseline for new projects.
- Panama via `jextract` would eliminate the `jni-rs` dependency, remove all `unsafe` Rust code on the JVM side, and produce a cleaner bridge.

**Should at minimum prototype both JNI and Panama in week 1 and compare.** The main remaining concern is that no mature Clojure-idiomatic Panama wrapper exists yet — a tooling gap, not a fundamental limitation.

### scala-polars as template needs validation

The plan references scala-polars repeatedly as direct prior art, but scala-polars targets Polars 0.35.x while the plan targets 0.53. The Polars Rust API has undergone significant breaking changes between versions (`polars-plan` restructured, `DslPlan` replaced older plan types, feature flags reorganized). scala-polars is a useful architectural reference but not a copy-paste template.

### Performance estimates

- **"2-10x for data transfer"** is too wide to be useful. Column-by-column JNI still copies every byte via `SetDoubleArrayRegion`. The real bottleneck in the Python bridge is string columns (UTF-8 → Python str → JVM String). More honest: **1.5-3x for numeric-heavy, 3-8x for string-heavy.**
- **"<5% for compute"** is well-grounded — Polars does the same Rust work either way.

---

## 2. Technical Gaps

### `from-maps` is the hardest problem and is essentially unaddressed

The current implementation delegates to `pl.DataFrame(data)` via Python, which leverages Polars' ~2000-line type inference engine handling mixed types, null inference, nested structures, date parsing, and type promotion.

In the JNI bridge, `from-maps` requires:
1. Receiving a Java `List<Map<String, Object>>` via JNI
2. Discovering the column key superset across all maps
3. Scanning all values per column to infer Polars dtypes (mixed int/float, nulls, boolean-vs-int ambiguity)
4. Constructing typed `Series` per column
5. Building a `DataFrame` from the Series vector

This is 300-500 lines of Rust on its own. The plan's 150-line estimate for `jni/series.rs` only covers "from primitive arrays" — assumes the caller already knows the types.

### Handle lifecycle is a correctness issue

The `reduce` pattern in `pipeline.clj` creates transient intermediate LazyFrame handles with no owner. In the Python bridge, intermediates are GC'd by Python's reference counter. In JNI, every intermediate handle is a heap allocation that must be freed.

Worse, the threading API allows users to reuse intermediate LazyFrames:

```clojure
(let [base (pl/scan-csv "data.csv")
      filtered (pl/filter base [:> :salary 50000])
      result-a (pl/collect filtered)
      result-b (pl/collect (pl/select base [:name]))]  ;; reuses `base`!
  ...)
```

If `pl/filter` consumed and freed `base` (ownership transfer), the second use is use-after-free. The plan mentions `Cleaner` but doesn't address this. Polars `LazyFrame` is `Clone`, so the bridge should `clone()` inputs — but this adds overhead and isn't mentioned.

**Needs explicit design: ownership transfer vs. reference counting vs. clone-on-use.**

### No joins in MVP

Without joins, the library is a toy for real-world use. The plan's native method surface assumes every operation takes handles in and returns a single handle out. `join` takes two LazyFrames and is critical for any non-trivial pipeline.

### Error message fidelity

The error chain `PolarsError::to_string()` → `RuntimeException` → Clojure `ex-info` loses structured information. A `PolarsError::ComputeError` with column name context becomes an opaque string. Users debugging a failed `group_by` see "ComputeError: column 'salry' not found" with no Clojure-level context pointing to which pipeline step failed. The current Python bridge at least gives a Python traceback.

Also: `JNIEnv` is not `UnwindSafe`, so capturing it inside `catch_unwind` is undefined behavior unless `AssertUnwindSafe`-wrapped.

### Testing strategy is completely absent

The plan mentions no testing approach. Needs:
- Integration tests from Clojure mirroring the existing 22 tests
- Memory leak tests (run 10K pipelines, verify RSS doesn't grow)
- Cross-platform CI tests
- Rust-side tests are limited since every function takes `JNIEnv`

### REPL ergonomics

Opaque `jlong` handles print as meaningless numbers. No plan for human-readable repr. Users expect to see DataFrame schema/shape at the REPL, not `#object[polars_clj.native.Handle 0x... "ptr=140234567890"]`.

---

## 3. Strategic Concerns

### DuckDB is the elephant in the room

DuckDB has:
- Native JVM bindings via JDBC (no Python, no Rust compilation needed)
- SQL interface (which HoneySQL already targets from Clojure)
- Comparable or superior performance to Polars for most analytical workloads
- A much larger and more active contributor base
- Apache Arrow integration for zero-copy interop

**For "fast DataFrames in Clojure without Python," `next.jdbc` + DuckDB achieves this today with zero custom native code.** The plan doesn't make the case for Polars over this.

### Why not wrap scala-polars?

scala-polars compiles to JVM bytecode. A Clojure wrapper around its Java-facing API would be ~200 lines of Clojure interop code, no Rust required, with the JNI bridge already built. The incremental value of building a new Rust bridge (~2000 lines) needs explicit justification. (Likely answer: scala-polars is unmaintained and pinned to old Polars — but the plan should state this.)

### Who is the user?

The target user is at the intersection of:
1. Wants DataFrame operations in Clojure
2. Does not want Python on their system
3. Needs Polars specifically (not SQL-based alternatives)
4. Is comfortable with platform-specific native JARs

This is narrow. Clojure devs wanting DataFrames without Python already have `tablecloth` (pure JVM). Those wanting speed have DuckDB via `next.jdbc`.

### The DSL abstraction leak

The plan claims `core.clj` stays "UNCHANGED." But `core.clj` currently requires `libpython-clj2.python`. The public function signatures stay the same, but the return types change from Python objects to opaque handles. The threading API returns handles that are now native pointers with manual lifecycle management — a fundamental change in contract.

---

## 4. Alternative Approaches Not Considered

### GraalPy

GraalVM's Python implementation runs Python packages on the JVM. If GraalPy can run Polars, the entire existing codebase works unchanged — same libpython-clj bridge, same Python API, zero Rust code. Not mentioned or ruled out in the plan.

### Substrait

A standardized cross-language query plan format. The Clojure DSL could compile to Substrait IR instead of Polars-specific calls. Multiple engines (DataFusion, DuckDB, Velox) consume Substrait. This would make the DSL engine-agnostic and eliminate Polars version tracking. For the operations in the current DSL (filter, select, sort, group-by, basic expressions), Substrait coverage is complete.

### Arrow Flight

Instead of JNI, run Polars in a lightweight Rust sidecar speaking Arrow Flight (gRPC + Arrow IPC). Eliminates all JNI complexity, handle lifecycle issues, and cross-compilation headaches. ~1-5ms per-query latency is acceptable for analytical workloads. The plan dismisses IPC as "over-engineered" but Arrow Flight is a well-defined, well-tooled protocol — not a custom IPC scheme.

### Multi-backend DSL (the strongest version)

The strongest version of this project might be a **multi-backend Clojure query DSL** (like Python's Ibis) that targets DuckDB, Polars, and DataFusion — not a Polars-only JNI bridge. The existing pipeline-vector representation is already engine-agnostic in shape.

---

## 5. What the Plan Gets Right

- **Opaque handle architecture** — Correct, matches DuckDB/scala-polars/DataFusion
- **Thin bridge design** — One JNI call per operation is the right granularity. Plan's reasoning is well-articulated.
- **Column-by-column data transfer for MVP** — Pragmatic. Arrow zero-copy should not be on the critical path.
- **Risk assessment is unusually honest** — Particularly the acknowledgment that contributor accessibility drops and compute performance gain is negligible.
- **Platform-classified Maven JARs** — Follows DuckDB/SQLite JDBC precedent. Correct distribution strategy.
- **Backend switching (keep Python fallback)** — Reduces risk, allows incremental migration. Good engineering judgment.

---

## Summary of Recommendations

1. **Re-evaluate Panama** as primary bridge technology given 2026 JDK landscape. Prototype both in week 1.
2. **Scope `from-maps` honestly** — needs its own milestone with 300-500 lines of type inference, or require explicit schemas.
3. **Design handle lifecycle explicitly** — clone-on-use is likely the right answer. This is a correctness issue.
4. **Add joins to MVP** — without them the library is a toy.
5. **Budget 7-9 weeks**, not 4-6, with cross-platform build as the high-risk item.
6. **Justify against `next.jdbc` + DuckDB** — this is the real competitive alternative.
7. **Investigate wrapping scala-polars** as lower-effort path before committing to new Rust bridge.
8. **Double the Rust line estimate** to ~1,800-2,500 lines.
9. **Consider the multi-backend DSL** — the existing pipeline-vector format is already engine-agnostic in shape. Targeting DuckDB + Polars + DataFusion via Substrait or per-engine backends would be a much stronger project.
