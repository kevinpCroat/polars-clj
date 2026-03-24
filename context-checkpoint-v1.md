# Context Checkpoint v1

## What Exists

### polars-clj — A Clojure DSL for Polars via libpython-clj
- **Repo:** https://github.com/kevinpCroat/polars-clj (public, pushed to main)
- **Status:** MVP complete, M1-M5 done, 22 tests passing

### Project Structure
```
polars-clj/
├── .gitignore
├── README.md                          # Full docs, expression reference, examples
├── deps.edn                           # Clojure 1.12 + libpython-clj 2.026
├── plan.md                            # Original implementation plan
├── rust-direct.md                     # Plan for Rust JNI bridge (no Python)
├── summary.md                         # Clojure Data Cookbook summary + Pandas/Polars comparison
├── src/polars_clj/
│   ├── interop.clj                    # Python bridge: thread-safe delay init, col/lit, scan, dataframe->clj (keywordized)
│   ├── expr.clj                       # Expression compiler: multimethod + macros (def-binary-op, def-variadic-op, def-unary-agg)
│   ├── pipeline.clj                   # Pipeline executor: multimethod execute-step, reduce over steps
│   └── core.clj                       # Public API: execute (data-driven) + threading wrappers, :refer-clojure excludes filter/sort/group-by
├── test/polars_clj/
│   ├── core_test.clj                  # 18 integration tests
│   └── validation_test.clj            # 4 error handling tests
└── test/resources/
    └── employees.csv                  # 10-row test dataset
```

### Key Design Decisions Made
1. **Pipeline vector** (not clause map) — operations are ordered/repeatable: `[[:scan-csv ...] [:filter ...] [:collect]]`
2. **Two APIs** — data-driven `(pl/execute [...])` and threading `(-> (pl/scan-csv ...) (pl/filter ...) pl/collect)`
3. **Multimethod dispatch** — both `compile-expr` (on expression type) and `execute-step` (on operation keyword)
4. **Macros for boilerplate** — `def-binary-op`, `def-variadic-op`, `def-unary-agg` eliminate repetition in expr.clj
5. **Keywordized results** — `dataframe->clj` returns `{:salary 95000}` not `{"salary" 95000}`
6. **Thread-safe init** — `delay` instead of atom + `when-not` (per code review)
7. **Public `kw->col-name`** — in interop.clj, used by pipeline.clj (deduplicated per review)
8. **Explicit type checking in expr dispatch** — numbers/strings/booleans/nil route to :literal, everything else to :unknown -> :default error

### Review Findings Applied
- Keywordized result maps (A1)
- Thread-safe init via delay (A2)
- Deduplicated kw->col-name (A3)
- :format option passed through in collect (M5)
- from-maps test uses keyword keys (M6)
- Reviewer's C1/C2/C3 (py/->py-list, kwargs) were false positives — all tests pass

### Environment
- macOS ARM (darwin), JDK 21 (Homebrew openjdk@21), Python 3.9.6, Polars 1.36.1
- JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
- Clojure 1.12.4 via brew

### Git State
- One commit on main: `c65ecb1 Initial implementation of polars-clj`
- Remote: origin -> https://github.com/kevinpCroat/polars-clj
- rust-direct.md not yet committed

### What's Next
- Commit and push rust-direct.md
- Potential M6 (stretch): when/then, string ops, cast
- Potential Rust JNI bridge implementation (see rust-direct.md for full plan)
