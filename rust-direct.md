# polars-clj Rust Direct: Eliminating the Python Middle Layer

## Executive Summary

Replace `Clojure -> JVM -> Python -> Polars (Rust)` with `Clojure -> JVM -> JNI -> Polars (Rust)`, cutting out the Python interpreter entirely.

**Honest assessment:** Feasible, with clear prior art in [scala-polars](https://github.com/chitralverma/scala-polars). An MVP is ~950 lines of Rust + ~300 lines of Clojure changes, achievable in 4-6 weeks. The biggest win is eliminating the Python dependency; the biggest risk is tracking Polars' Rust API which breaks without deprecation warnings.

---

## 1. Polars Rust API

Polars is a workspace of 20+ Rust sub-crates. The main `polars` crate (v0.53) is a facade re-exporting:

| Crate | Purpose |
|---|---|
| `polars-core` | DataFrame, Series, ChunkedArray |
| `polars-lazy` | LazyFrame, query planning |
| `polars-plan` | DslPlan, IR, optimization |
| `polars-expr` | Expr enum (~20 variants) |
| `polars-io` | CSV, Parquet, JSON I/O |
| `polars-arrow` | Arrow2 fork, columnar memory |

Key types map directly to what we already use:
- `LazyFrame` wraps a `DslPlan` — no data, just a logical plan
- `Expr` is an enum: `Column`, `Literal`, `BinaryExpr`, `Alias`, `Agg`, `Sort`, `Filter`, etc.
- `Expr` derives `Clone`, `Serialize`, `Deserialize`
- `collect()` triggers the optimizer and execution engine

**Stability warning:** The Rust API breaks without deprecation — changes are only listed in the changelog. The Python API has deprecation periods. This is the biggest maintenance difference.

---

## 2. JNI Bridge — Option Evaluation

### Option A: JNI via jni-rs (Recommended)

The `jni` crate (v0.21.1) provides Rust bindings to Java Native Interface. Write Rust functions with JNI naming conventions, compile to `cdylib`, JVM loads via `System.loadLibrary`.

- **Performance:** ~22ns per JNI crossing — negligible vs Polars computation
- **Prior art:** scala-polars, DuckDB Java (84.7% C++), DataFusion Java all use this
- **Complexity:** Medium — requires `unsafe` for pointer management, `catch_unwind` for panic safety

### Option B: JNA — Not Recommended

5-20x slower per call than JNI due to runtime reflection. Not worth it.

### Option C: Panama (Foreign Function & Memory API) — Too Early

Finalized in JDK 22 (March 2024). Competitive with JNI performance, less boilerplate. But requires JDK 22+ (cuts out JDK 17/21 LTS users), no Clojure tooling exists. Revisit in 2-3 years.

### Option D: GraalVM Native Interface — Too Niche

Requires GraalVM, limits user base. Standard JNI works on any JDK 17+.

### Option E: Sidecar/IPC — Over-engineered

Process management, protocol design, per-operation IPC latency. Unsuitable for a library.

**Verdict: Option A (JNI via jni-rs).** Three major data engines chose this path.

---

## 3. Data Transfer — The Hard Problem

### 3a. Opaque Handle Pattern (for lazy operations)

The proven pattern from scala-polars and DuckDB:

```rust
fn to_ptr<T>(obj: T) -> jlong {
    Box::into_raw(Box::new(obj)) as jlong
}

unsafe fn from_ptr<'a, T>(ptr: jlong) -> &'a mut T {
    &mut *(ptr as *mut T)
}

fn free_ptr<T>(ptr: jlong) {
    unsafe { drop(Box::from_raw(ptr as *mut T)) }
}
```

Rust objects (LazyFrame, Expr) are heap-allocated. Raw pointers cast to `jlong` (64-bit int) and passed to the JVM as opaque handles. Data never leaves Rust until `collect()`.

### 3b. Returning Results (on collect)

Three approaches, in order of complexity:

| Approach | Speed | Complexity | Notes |
|---|---|---|---|
| **Row-by-row JNI** | Slow | Low | 10M JNI calls for 1M×10 DataFrame. Good for MVP. |
| **Column-by-column bulk arrays** | Fast | Medium | One `SetDoubleArrayRegion` per column. DuckDB uses this. |
| **Arrow C Data Interface** | Zero-copy | High | Needs `arrow-java` dep. Long-term correct answer. |

**Plan:** Start with column-by-column (Approach 2) for MVP. Migrate to Arrow (Approach 3) later.

### 3c. Passing Expressions

**Option 1: Build incrementally via JNI calls** (recommended)

```
exprCol("salary")        -> handle₁
exprLitInt(50000)        -> handle₂
exprGt(handle₁, handle₂) -> handle₃
```

7 JNI calls for a complex expression = ~154ns total. Negligible vs query execution.

**Option 2: Serialize entire expression as JSON** — `Expr` supports serde, but the format is undocumented and changes across Polars versions. Not worth the coupling.

**Option 3: Serialize entire pipeline** — Same problem, plus loses fine-grained error reporting.

### 3d. String Encoding

JVM strings are UTF-16; Rust/Polars strings are UTF-8. The `jni` crate handles conversion automatically via `JNIEnv::new_string()` and `JNIEnv::get_string()`. Per-string cost is real for large string columns; Arrow zero-copy eliminates it.

---

## 4. Architecture

```
┌──────────────────────────────────────────────┐
│  Clojure (JVM)                               │
│                                              │
│  core.clj       ── Public API (UNCHANGED)    │
│  expr.clj       ── Compiles DSL to native    │
│                     JNI calls (not Python)    │
│  pipeline.clj   ── Orchestrates via handles  │
│  native.clj     ── Java native method decls, │
│                     System.loadLibrary,       │
│                     handle lifecycle          │
└──────────────┬───────────────────────────────┘
               │  JNI (opaque jlong handles)
               ▼
┌──────────────────────────────────────────────┐
│  polars-clj-native (Rust cdylib)             │
│                                              │
│  src/lib.rs         ── Module root           │
│  src/jni/                                    │
│    expr.rs          ── Expr construction     │
│    lazy_frame.rs    ── LazyFrame operations  │
│    data_frame.rs    ── DataFrame -> JVM      │
│    io.rs            ── scan_csv, scan_parq   │
│  src/convert.rs     ── Rust<->JVM type conv  │
│  src/error.rs       ── Error -> JException   │
│                                              │
│  Cargo.toml: polars 0.53, jni 0.21          │
└──────────────────────────────────────────────┘
```

### Key Design Decision: Thin Bridge

One JNI call per Polars operation (not one call for the whole pipeline).

**Why:** Per-operation JNI overhead (nanoseconds) is irrelevant vs Polars execution (milliseconds+). Thin bridge is simpler, more debuggable, and compatible with the existing threading API. Each operation is independently testable.

### Clojure Changes

- **New:** `polars-clj.native` (~100 LOC) — Java class with native methods + `System.loadLibrary`
- **Modified:** `interop.clj` — replace libpython-clj calls with native calls, same API surface
- **Modified:** `expr.clj` — replace `py/py.` with native handle construction
- **Modified:** `pipeline.clj` — pass opaque handles instead of Python objects
- **Unchanged:** `core.clj` — the public API stays identical

### Native Method Surface

```java
// Sources
static native long scanCsv(String path);
static native long scanParquet(String path);

// Expressions
static native long exprCol(String name);
static native long exprLitInt(long value);
static native long exprLitFloat(double value);
static native long exprLitStr(String value);
static native long exprLitBool(boolean value);
static native long exprGt(long left, long right);
static native long exprAdd(long left, long right);
// ... (one per operator)
static native long exprAlias(long expr, String name);
static native long exprSum(long expr);
// ... (one per aggregation)

// LazyFrame operations
static native long lazyFilter(long lf, long expr);
static native long lazySelect(long lf, long[] exprs);
static native long lazyWithColumns(long lf, long[] exprs);
static native long lazySort(long lf, String col, boolean desc);
static native long lazyLimit(long lf, int n);
static native long lazyGroupByAgg(long lf, String[] groups, long[] aggs);
static native long lazyCollect(long lf);
static native String lazyExplain(long lf);

// DataFrame -> JVM
static native int dfColumnCount(long df);
static native long dfRowCount(long df);
static native long[] dfColumnToLongArray(long df, int col);
static native double[] dfColumnToDoubleArray(long df, int col);
static native String[] dfColumnToStringArray(long df, int col);

// Cleanup
static native void freeLazyFrame(long ptr);
static native void freeExpr(long ptr);
static native void freeDataFrame(long ptr);
```

### Error Handling

```rust
// Every JNI function wraps in catch_unwind
#[no_mangle]
pub extern "system" fn Java_polars_clj_Native_scanCsv(
    mut env: JNIEnv, _class: JClass, path: JString
) -> jlong {
    let result = std::panic::catch_unwind(|| {
        let path: String = env.get_string(&path)?.into();
        let lf = LazyCsvReader::new(&path).finish()?;
        Ok(to_ptr(lf))
    });
    match result {
        Ok(Ok(ptr)) => ptr,
        Ok(Err(e)) => { env.throw_new("java/lang/RuntimeException", e.to_string()); 0 }
        Err(_) => { env.throw_new("java/lang/RuntimeException", "Rust panic"); 0 }
    }
}
```

### Handle Lifecycle

Handles are wrapped in a Clojure deftype implementing `Closeable`:

```clojure
(deftype Handle [^:volatile-mutable ptr free-fn]
  java.io.Closeable
  (close [_]
    (when (pos? ptr)
      (free-fn ptr)
      (set! ptr 0))))
```

Safety net via `java.lang.ref.Cleaner` for handles that escape without explicit close.

---

## 5. Prior Art

| Project | Approach | Relevance |
|---|---|---|
| **scala-polars** | JNI via jni-rs + opaque handles | Direct template — same language family, same problem |
| **DuckDB Java** | JNI, JDBC interface, 84.7% C++ | Gold standard for native data engine + JVM bindings |
| **DataFusion Java** | JNI via jni-rs, Arrow IPC transfer | Proves Arrow-based transfer from Rust works |
| **borkdude/clojure-rust-graalvm** | Clojure + Rust + JNI + GraalVM | Proves Clojure-to-Rust JNI pattern works |

scala-polars is the most directly relevant: it already does exactly what we want, for Scala instead of Clojure.

---

## 6. MVP Scope

### Feature Set (matches current polars-clj)

Same operations, same DSL, same public API. Only the backend changes.

### Minimum Rust Code

| File | Lines | Purpose |
|---|---|---|
| `lib.rs` | ~20 | Module declarations |
| `jni/expr.rs` | ~150 | col, lit variants, binary ops, unary aggs, alias |
| `jni/lazy_frame.rs` | ~200 | filter, select, with_columns, sort, limit, group_by, collect, explain |
| `jni/data_frame.rs` | ~100 | Column-to-array conversion, schema, free |
| `jni/io.rs` | ~80 | scan_csv, scan_parquet |
| `jni/series.rs` | ~150 | From primitive arrays, from string arrays |
| `convert.rs` | ~200 | Rust<->JVM type conversion |
| `error.rs` | ~50 | Error -> Java exception |
| **Total** | **~950** | |

---

## 7. Build & Distribution

### Cross-Platform Targets

| Target | Runner |
|---|---|
| `aarch64-apple-darwin` | macOS ARM (Apple Silicon) |
| `x86_64-apple-darwin` | macOS Intel |
| `x86_64-unknown-linux-gnu` | Linux x86_64 |
| `aarch64-unknown-linux-gnu` | Linux ARM64 (via cross-rs) |

### Distribution: Platform-Classified Maven JARs

Following DuckDB's pattern:

```
polars-clj-0.1.0.jar                           # Clojure source + Java class
polars-clj-native-0.1.0-macos-aarch64.jar      # libpolars_clj_native.dylib
polars-clj-native-0.1.0-macos-x86_64.jar
polars-clj-native-0.1.0-linux-x86_64.jar
polars-clj-native-0.1.0-linux-aarch64.jar
```

At runtime: detect platform, extract native lib from JAR resource to temp dir, `System.load`.

### User Experience (Goal)

```clojure
;; deps.edn — no Python, no pip, no virtualenv
{:deps {io.github.kevinpCroat/polars-clj {:mvn/version "0.1.0"}}}
```

Native library auto-detected and loaded. Just works.

---

## 8. Risks & Trade-offs

### Effort Comparison

| Aspect | Python Bridge (current) | Rust JNI Bridge |
|---|---|---|
| Initial build | Done (~400 LOC Clojure) | ~950 LOC Rust + ~300 LOC Clojure, 4-6 weeks |
| Adding an operation | ~5 lines Clojure | ~20-40 lines Rust + ~5 lines Clojure |
| Tracking Polars updates | Python API has deprecation periods | Rust API breaks without warning |
| Build complexity | None (pure JVM + pip) | Cross-platform Rust CI, native lib packaging |
| Contributor accessibility | Most Clojure devs know Python | Few Clojure devs know Rust |

### Performance Gain (Realistic)

| Workload | Improvement | Why |
|---|---|---|
| Heavy compute, small result | **< 5%** | Polars engine does the same work either way |
| Large result sets (100K+ rows) | **2-10x** | Column-by-column JNI vs Python object serialization |
| Interactive REPL (many small ops) | **3-5x latency** | No Python interpreter overhead per call |
| Startup | **~500ms-1s faster** | No Python initialization |

**The honest answer:** Not a 10x improvement for most workloads. The biggest win is **eliminating the Python dependency** — no pip, no virtualenv, no version conflicts. For a Clojure library, that's a major UX improvement.

### What We Gain

- No Python dependency (the biggest practical win)
- Faster startup (~1s saved)
- Simpler deployment (JAR with embedded native libs)
- Path to Arrow zero-copy transfer
- Better data transfer performance

### What We Lose

- Python ML ecosystem access (scikit-learn, PyTorch via libpython-clj)
- Rapid prototyping against well-documented Python API
- Simpler error messages (Python tracebacks vs JNI errors)
- Easy contributor onboarding (Rust is a harder requirement)

---

## 9. Implementation Milestones

### M0: Proof of Concept (Week 1)

Compile a Rust `cdylib` with one JNI function (`scanCsv`), call it from Clojure, get data back.

**Exit criteria:** Clojure REPL loads a CSV via Rust native library. No Python anywhere.

### M1: Expression Construction (Week 2)

Port all expression operators to JNI: col, lit (all types), arithmetic, comparison, logical, alias, aggregations. Port expression tests.

### M2: LazyFrame Operations (Week 3)

Port all pipeline operations: filter, select, with_columns, sort, limit, group_by+agg, collect, explain. Port integration tests.

### M3: Data Transfer (Week 4)

Column-by-column bulk array transfer. Benchmark vs Python bridge at 10K, 100K, 1M rows.

### M4: Build & Distribution (Week 5-6)

GitHub Actions cross-platform CI. Platform-specific JARs. Auto-detect and load native lib at runtime. Test on fresh machines (no Rust, no Python).

### M5: Backend Switching & Polish (Week 6)

```clojure
(polars-clj.config/set-backend! :native)  ;; or :python
```

Default to `:native` if available, fall back to `:python`. Handle lifecycle management with `Closeable` + `Cleaner` safety net.

### M6 (Stretch): Arrow Zero-Copy (Week 7-9)

Arrow C Data Interface for zero-copy numeric/temporal columns. Requires `arrow-java` dependency.

---

## 10. Open Questions

1. **Handle cleanup:** Explicit `close` + `Cleaner` safety net? Or rely entirely on `Cleaner`? Explicit close is more predictable; Cleaner prevents leaks from forgotten handles.

2. **Polars version pinning:** Pin in Cargo.toml. Each polars-clj release embeds a specific Polars version. Users who need a different version must wait for us to update.

3. **Native library size:** Polars with full features compiles to ~30-50MB. Minimum feature set may reduce this. Strip symbols + LTO helps.

4. **Thread safety:** `LazyFrame` is `Send + Sync`. `JNIEnv` is thread-local. Per-call pattern (no held references) keeps this safe.

5. **`from-maps` implementation:** Need to infer column dtypes from JVM data or require explicit schema. Currently the Python bridge delegates to `pl.DataFrame(data)` which auto-infers.

---

## References

- [scala-polars](https://github.com/chitralverma/scala-polars) — Direct template for JVM + Polars via JNI
- [DuckDB Java](https://github.com/duckdb/duckdb-java) — Gold standard for native engine + JVM bindings
- [DataFusion Java](https://github.com/datafusion-contrib/datafusion-java) — JNI + Arrow IPC approach
- [jni-rs](https://github.com/jni-rs/jni-rs) — Rust JNI bindings
- [Polars Rust docs](https://docs.rs/polars/latest/polars/)
- [Polars versioning policy](https://docs.pola.rs/development/versioning/)
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow Java C Data](https://arrow.apache.org/java/current/cdata.html)
- [JEP 454: Foreign Function & Memory API](https://openjdk.org/jeps/454)
