# polars-clj

A Clojure DSL that compiles to [Polars](https://pola.rs/) expressions, executed via [libpython-clj](https://github.com/clj-python/libpython-clj). HoneySQL for DataFrames.

## Motivation

Polars' expression-based API is a natural fit for Clojure's data-literal syntax. HoneySQL proved that representing a query language as Clojure data structures gives you composability, tooling, and metaprogramming for free. polars-clj applies the same pattern to DataFrames: write Clojure data, get Polars' Rust-native performance.

```clojure
;; HoneySQL turns this into SQL:
{:select [:name :salary] :from [:employees] :where [:> :salary 50000]}

;; polars-clj turns this into a Polars LazyFrame pipeline:
[[:scan-csv "employees.csv"]
 [:filter [:> :salary 50000]]
 [:select [:name :salary]]
 [:collect]]
```

## Quick Start

### Prerequisites

- JDK 17+
- Python 3.9+ with Polars installed: `pip install polars`

### deps.edn

```clojure
{:deps {org.clojure/clojure      {:mvn/version "1.12.0"}
        clj-python/libpython-clj {:mvn/version "2.026"}}}
```

Add polars-clj source to your `:paths` or reference it as a dependency once published.

### Hello World

```clojure
(require '[polars-clj.core :as pl])

(-> (pl/from-maps [{:name "Alice" :salary 90000 :dept "Eng"}
                    {:name "Bob"   :salary 55000 :dept "Sales"}
                    {:name "Carol" :salary 72000 :dept "Eng"}])
    (pl/filter [:> :salary 60000])
    (pl/select [:name :salary])
    pl/collect)
;; => [{"name" "Alice", "salary" 90000}
;;     {"name" "Carol", "salary" 72000}]
```

## Two APIs

### Data-driven

Describe a pipeline as a vector of steps and hand it to `execute`:

```clojure
(pl/execute
  [[:scan-csv "employees.csv"]
   [:filter [:> :salary 50000]]
   [:select [:name :salary]]
   [:collect]])
```

Pipelines are plain data. You can build them programmatically, store them, serialize them, or merge them.

### Threading

Compose individual functions with `->`:

```clojure
(-> (pl/scan-csv "employees.csv")
    (pl/filter [:> :salary 50000])
    (pl/select [:name :salary])
    pl/collect)
```

Both APIs produce identical results. Use whichever fits your context.

## Expression Reference

Expressions are Clojure data structures that compile to Python Polars `Expr` objects.

| DSL Form | Compiles To | Notes |
|---|---|---|
| `:salary` | `pl.col("salary")` | Keyword = column reference |
| `42`, `"hello"`, `true`, `nil` | `pl.lit(42)`, `pl.lit("hello")`, ... | Any non-keyword scalar = literal |
| `[:+ a b ...]` | `a + b + ...` | Variadic (left-fold) |
| `[:- a b ...]` | `a - b - ...` | Variadic |
| `[:* a b ...]` | `a * b * ...` | Variadic |
| `[:/ a b ...]` | `a / b / ...` | Variadic |
| `[:** a b]` | `a ** b` | Exponentiation |
| `[:> a b]` | `a > b` | |
| `[:< a b]` | `a < b` | |
| `[:>= a b]` | `a >= b` | |
| `[:<= a b]` | `a <= b` | |
| `[:= a b]` | `a == b` | |
| `[:!= a b]` | `a != b` | |
| `[:and a b ...]` | `a & b & ...` | Variadic |
| `[:or a b ...]` | `a \| b \| ...` | Variadic |
| `[:not a]` | `~a` | |
| `[:as expr "name"]` | `expr.alias("name")` | Alias / rename |
| `[:sum :col]` | `pl.col("col").sum()` | |
| `[:mean :col]` | `pl.col("col").mean()` | |
| `[:min :col]` | `pl.col("col").min()` | |
| `[:max :col]` | `pl.col("col").max()` | |
| `[:count :col]` | `pl.col("col").count()` | |
| `[:first :col]` | `pl.col("col").first()` | |
| `[:last :col]` | `pl.col("col").last()` | |
| `[:n-unique :col]` | `pl.col("col").n_unique()` | |
| `[:is-null :col]` | `pl.col("col").is_null()` | |
| `[:is-not-null :col]` | `pl.col("col").is_not_null()` | |

Expressions nest arbitrarily: `[:as [:* [:+ :price 10] :qty] "total"]`

Keyword column names convert hyphens to underscores: `:first-name` becomes `"first_name"`.

## Pipeline Operations Reference

| Operation | Type | Example |
|---|---|---|
| `scan-csv` | Source | `[:scan-csv "data.csv"]` or `[:scan-csv "data.csv" {:separator ";"}]` |
| `scan-parquet` | Source | `[:scan-parquet "data.parquet"]` |
| `from-maps` | Source | `[:from-maps [{:a 1} {:a 2}]]` |
| `filter` | Transform | `[:filter [:> :age 21]]` |
| `select` | Transform | `[:select [:name :salary]]` |
| `with-columns` | Transform | `[:with-columns [[:as [:* :price :qty] "total"]]]` |
| `sort` | Transform | `[:sort :salary]` or `[:sort [:a :b] {:descending true}]` |
| `limit` | Transform | `[:limit 10]` |
| `group-by` | Transform | `[:group-by [:dept] :agg [[:as [:mean :salary] "avg"]]]` |
| `collect` | Terminal | `[:collect]` — returns vector of maps |
| `explain` | Terminal | `[:explain]` — returns query plan string |
| `show` | Convenience | Threading API only: `(pl/show lf 10)` — prints and returns data |

## Examples

### Filter and select

```clojure
(-> (pl/scan-csv "employees.csv")
    (pl/filter [:and [:> :salary 50000]
                     [:= :active true]])
    (pl/select [:name :department :salary])
    (pl/sort :salary {:descending true})
    pl/collect)
```

### Computed columns

```clojure
(-> (pl/scan-csv "orders.csv")
    (pl/with-columns [[:as [:* :price :quantity] "total"]
                      [:as [:* [:* :price :quantity] 0.08] "tax"]])
    (pl/select [:order-id :total :tax])
    pl/collect)
```

### Group-by aggregation

```clojure
(-> (pl/scan-csv "employees.csv")
    (pl/group-by [:department]
      :agg [[:as [:mean :salary]    "avg_salary"]
            [:as [:max :salary]     "max_salary"]
            [:as [:count :salary]   "headcount"]])
    (pl/sort "avg_salary" {:descending true})
    pl/collect)
```

### Full pipeline (data-driven)

```clojure
(pl/execute
  [[:scan-csv "sales.csv"]
   [:filter [:and [:> :amount 0] [:is-not-null :region]]]
   [:with-columns [[:as [:* :amount :discount-pct] "discount"]]]
   [:group-by [:region]
    :agg [[:as [:sum :amount]   "total_sales"]
          [:as [:mean :amount]  "avg_sale"]
          [:as [:count :amount] "num_transactions"]]]
   [:sort "total_sales" {:descending true}]
   [:limit 10]
   [:collect]])
```

## How It Works

```
Clojure data         expr.clj              pipeline.clj           Polars (Python)
─────────────────────────────────────────────────────────────────────────────────
[:> :salary 50000]  ──> compile-expr  ──>  Python Expr object
                                            │
[:scan-csv "f.csv"] ──> execute-step ──>  LazyFrame
[:filter ...]       ──> execute-step ──>  LazyFrame.filter(expr)
[:collect]          ──> execute-step ──>  LazyFrame.collect() ──> Clojure maps
```

1. **interop.clj** — Initializes Python via libpython-clj, imports `polars`, provides `pl.col()`, `pl.lit()`, and data conversion.
2. **expr.clj** — A multimethod (`compile-expr`) that recursively compiles Clojure data structures into Polars `Expr` objects.
3. **pipeline.clj** — A multimethod (`execute-step`) that maps each operation keyword to the corresponding LazyFrame method call. `execute-pipeline` reduces over the step vector.
4. **core.clj** — Public API. `execute` for data-driven pipelines; `scan-csv`, `filter`, `select`, etc. for threading.

## Status

**Alpha.** The core expression and pipeline API is functional but the library is not yet published to Clojars.

### Supported

- CSV and Parquet lazy scanning
- Inline data via `from-maps`
- filter, select, with-columns, sort, limit, group-by + agg
- Arithmetic, comparison, logical, alias, and aggregation expressions
- Query plan inspection via `explain`

### Planned

- Joins (inner, left, outer, cross)
- Window functions (over, partition_by)
- `when` / `then` / `otherwise` conditional expressions
- String and datetime operations
- Type casting
- Arrow-based zero-copy data conversion

## Running Tests

Requires `JAVA_HOME` set to JDK 17+.

```bash
clojure -M:test
```
