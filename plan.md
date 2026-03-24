# polars-clj — Implementation Plan

## Overview

A HoneySQL-style Clojure DSL that compiles to Polars Python expressions, executed via libpython-clj. Write Clojure data structures, get Polars LazyFrame execution.

## Design Decision: Pipeline Vector

Polars operations are ordered and repeatable, so the canonical representation is a vector of steps (not a clause map like HoneySQL):

```clojure
[[:scan-csv "data.csv"]
 [:filter [:> :age 25]]
 [:select [:name :salary]]
 [:sort :salary]]
```

## Architecture (4 namespaces)

| Namespace | Role |
|---|---|
| `polars_clj/interop.clj` | libpython-clj bridge — init Python, import polars, type conversion |
| `polars_clj/expr.clj` | Expression compiler — Clojure data -> Python Polars Expr objects (multimethod) |
| `polars_clj/pipeline.clj` | Pipeline executor — walks step vector, chains LazyFrame methods |
| `polars_clj/core.clj` | Public API — `execute` (data-driven) + `scan-csv`, `filter`, etc. (threading) |

## Two APIs

```clojure
;; Data-driven (primary)
(pl/execute
  [[:scan-csv "employees.csv"]
   [:filter [:and [:> :salary 50000] [:= :active true]]]
   [:group-by [:department] :agg [[:as [:mean :salary] "avg_salary"]]]
   [:sort "avg_salary" {:descending true}]
   [:collect]])

;; Threading (convenience)
(-> (pl/scan-csv "employees.csv")
    (pl/filter [:> :salary 50000])
    (pl/select [:name :department])
    (pl/collect))
```

## Expression DSL Reference

| DSL Form | Compiles To |
|----------|-------------|
| `:col-name` | `pl.col("col_name")` |
| `42` / `"str"` / `true` | `pl.lit(42)` / `pl.lit("str")` / `pl.lit(true)` |
| `[:+ a b]` | `a + b` |
| `[:- a b]` | `a - b` |
| `[:* a b]` | `a * b` |
| `[:/ a b]` | `a / b` |
| `[:> a b]` | `a > b` |
| `[:< a b]` | `a < b` |
| `[:>= a b]` | `a >= b` |
| `[:<= a b]` | `a <= b` |
| `[:= a b]` | `a == b` |
| `[:!= a b]` | `a != b` |
| `[:and a b ...]` | `a & b & ...` |
| `[:or a b ...]` | `a \| b \| ...` |
| `[:not a]` | `~a` |
| `[:as expr "name"]` | `expr.alias("name")` |
| `[:sum :col]` | `pl.col("col").sum()` |
| `[:mean :col]` | `pl.col("col").mean()` |
| `[:min :col]` | `pl.col("col").min()` |
| `[:max :col]` | `pl.col("col").max()` |
| `[:count :col]` | `pl.col("col").count()` |
| `[:is-null :col]` | `pl.col("col").is_null()` |

Keywords use `name` conversion: `:salary` -> `"salary"`. Hyphens map to underscores: `:first-name` -> `"first_name"`.

## MVP Scope

### In Scope (P0)

- `scan-csv`, `scan-parquet` — data sources
- `select`, `filter`, `with-columns` — core transforms
- `group-by` + `agg` — aggregation
- `sort`, `limit`, `collect` — ordering and execution
- `from-maps` — inline data
- Expression operators: arithmetic, comparison, logical, alias, core aggregations

### Out of Scope

- Joins, window functions, string/datetime ops, cast, when/then, streaming, reshaping

## Milestones

### M1: Skeleton + Python Bridge
- Project structure, deps.edn
- `interop.clj`: ensure-python!, pl-mod, scan-csv, dataframe->clj
- Smoke test: scan CSV from Clojure, get maps back

### M2: Expression Compiler
- `expr.clj` with multimethod `compile-expr`
- Column refs, literals, arithmetic, comparisons, logical, alias, aggregations
- Unit tests for each expression type

### M3: Pipeline Executor
- `pipeline.clj` with multimethod `execute-step`
- Steps: scan-csv, scan-parquet, filter, select, with-columns, group-by+agg, sort, limit, collect
- `execute-pipeline` as reduce over steps
- Integration tests: full pipelines

### M4: Public API
- `core.clj`: `execute` function + threading wrappers
- `from-maps` for inline data
- End-to-end tests with both APIs

### M5: Polish + README
- Input validation, error messages
- `explain` function (query plan)
- README with examples

### M6 (Stretch): Extended Expressions
- when/then/otherwise
- String operations
- Type casting

## Key Risks

1. **Data conversion overhead** — `.to_dicts()` copies through bridge; fine for MVP, Arrow zero-copy later
2. **Operator dispatch** — verify `(py. expr __gt__ 5)` works in libpython-clj
3. **group-by returns GroupBy, not LazyFrame** — step handles both calls internally
4. **Python/Polars not installed** — need clear error messages

## Dependencies

- `org.clojure/clojure 1.12.0`
- `clj-python/libpython-clj 2.026`
- User prerequisite: Python 3.9+ with `pip install polars`
