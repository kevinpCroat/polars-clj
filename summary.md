# Clojure Data Cookbook — Summary & Comparison

## Overview

The [Clojure Data Cookbook](https://github.com/scicloj/clojure-data-cookbook) is an in-development (alpha) resource from the [SciCloj](https://scicloj.github.io/) community that provides practical recipes for common data science tasks in Clojure. Inspired by Kira McLean's talk "Clojure for Data Science in the Real World," it aims to document the community-recommended technology stack and lower the barrier to entry for data work in Clojure.

Licensed under CC BY-NC-ND 4.0.

## Key Libraries in the Ecosystem

The cookbook centers on **Noj** (`org.scicloj/noj`), an umbrella meta-package that bundles tested, interoperable libraries:

| Library | Role |
|---|---|
| **tech.ml.dataset** | Core columnar dataset abstraction (the "DataFrame" of Clojure) |
| **Tablecloth** | High-level, ergonomic API over tech.ml.dataset |
| **dtype-next** | High-performance typed array/tensor system underlying the dataset stack |
| **Fastmath** | Mathematics & statistics |
| **tech.ml** | Machine learning pipelines |
| **Kindly** | Notation system for visualization across renderers |
| **Clay** | Notebook/publishing environment (like Jupyter) |
| **Clerk** | Alternative notebook environment (Nextjournal) |
| **Hanami** | Vega/Vega-Lite wrapper for declarative visualization |
| **libpython-clj** | Python interop (call NumPy, scikit-learn, etc. from Clojure) |

Supporting libraries include: charred (CSV/JSON parsing), next.jdbc + HoneySQL (SQL), Apache Arrow/jarrow (columnar interchange), Apache POI/fastexcel (Excel), Portal (data inspector), Specter & Meander (data transformation), and SPARQL/RDF tools.

## Cookbook Structure (Planned)

1. **Introduction** — "whole game" end-to-end walkthrough
2. **Data Import/Export** — CSV, JSON, EDN, Excel, SPSS, databases (SQL/SPARQL), URLs, random/dummy data, messy data handling, export to CSV/HTML/PDF
3. **Data Transformation** — sorting, selecting, filtering, deduplication, computed columns, renaming, conditional transforms, aggregation, grouping, joins, wide/long pivoting, moving averages
4. **Data Tidying** — reshaping and cleaning
5. **Data Visualization** — bar/line/scatter charts, histograms, box plots, Q-Q plots, heatmaps, network graphs, choropleths, 3D plots, faceting, layering, theming, export (SVG/PDF)
6. **Statistics** — regression, hypothesis tests, ANOVA, bootstrapping, chi-square, inter-rater reliability
7. **Workflow** — IDE setup (CIDER/Calva/Cursive), publishing, code style, performance tips

## Comparison with Pandas and Polars

### At a Glance

| Dimension | Clojure (Tablecloth / tech.ml.dataset) | Pandas | Polars |
|---|---|---|---|
| **Language** | Clojure (JVM) | Python | Rust core, Python/Node/R bindings |
| **Paradigm** | Functional, immutable | Object-oriented, mutable | Functional expression API, immutable |
| **Eager/Lazy** | Eager | Eager | Lazy by default, eager optional |
| **Concurrency** | JVM threads + immutable data = safe parallelism | GIL-limited; workarounds via multiprocessing | Rust-level parallelism, no GIL |
| **Type System** | Statically typed columns (dtype-next) | Dynamically typed (object columns common) | Strictly typed columns |
| **Memory Model** | Off-heap columnar buffers (dtype-next), Arrow support | NumPy-backed, Arrow optional via PyArrow | Apache Arrow native |
| **Missing Values** | First-class support in typed columns | NaN / None (inconsistent across dtypes) | Null bitmask (Arrow-native) |
| **REPL / Notebook** | Clay, Clerk, Portal (REPL-first workflow) | Jupyter (dominant) | Jupyter, Marimo |
| **Ecosystem Size** | Small but integrated (SciCloj) | Massive (PyData) | Growing rapidly |
| **Maturity** | Alpha/beta — evolving APIs | Stable, 15+ years | Stable, ~4 years |

### Strengths of the Clojure Stack

- **Immutability by default.** Datasets are persistent/immutable, eliminating an entire class of bugs common in Pandas (`SettingWithCopyWarning`, accidental mutation).
- **Composability.** Tablecloth operations are plain functions — they compose with `->`, `comp`, `reduce`, and all standard Clojure higher-order functions. No method-chaining DSL.
- **REPL-driven development.** Evaluate any expression instantly. Clay/Clerk render rich output inline. The feedback loop is tighter than Jupyter's cell model.
- **JVM interop.** Direct access to Java's ecosystem — JDBC databases, Apache Arrow, Spark, Kafka — without serialization overhead.
- **Python interop.** `libpython-clj` lets you call pandas, scikit-learn, or PyTorch directly from Clojure when needed, bridging ecosystem gaps.
- **Typed columns.** dtype-next enforces column types from the start, catching errors that Pandas silently coerces into `object` dtype.

### Strengths of Pandas

- **Ecosystem dominance.** The largest library ecosystem in data science — virtually every tool, tutorial, and StackOverflow answer assumes Pandas.
- **Mature and battle-tested.** 15+ years of production use across every industry.
- **Low barrier to entry.** Familiar OOP-style API; enormous learning resources.
- **Integration breadth.** First-class support in scikit-learn, matplotlib, seaborn, Jupyter, and hundreds of other libraries.

### Strengths of Polars

- **Performance.** Rust-native, multithreaded, vectorized. Often 5–50x faster than Pandas on medium-to-large datasets.
- **Lazy evaluation.** Query optimizer rewrites and fuses operations before execution, minimizing memory and compute.
- **Strict typing and Arrow-native.** No silent type coercion; zero-copy interchange with other Arrow-based tools.
- **Expressive API.** Expression-based API is more composable than Pandas, closer in spirit to Tablecloth's functional approach.
- **Growing fast.** Rapidly becoming the default recommendation for new Python data projects.

### Where Each Falls Short

| Limitation | Clojure Stack | Pandas | Polars |
|---|---|---|---|
| **Ecosystem size** | Small community; fewer tutorials, fewer ready-made integrations | — | Smaller than Pandas, some libraries lack Polars support |
| **Learning curve** | Clojure itself is unfamiliar to most data scientists | — | New API to learn even for Pandas users |
| **Maturity** | APIs still in alpha/beta; breaking changes possible | — | Stable but younger; some edge cases |
| **Visualization** | Vega-Lite via Hanami is powerful but less polished than matplotlib/seaborn/plotly | Matplotlib is verbose and low-level | No built-in viz; relies on external libraries |
| **Big data** | No built-in distributed computing (though JVM enables Spark interop) | Single-machine, memory-bound | Single-machine (streaming mode helps) |
| **Mutability** | Immutable-only can feel verbose for exploratory one-liners | Mutation bugs are pervasive | Immutable (same trade-off as Clojure) |

### Who Should Consider What

- **Pandas** remains the pragmatic default for most data scientists — especially for exploratory work, quick scripts, and projects that depend heavily on the Python ML ecosystem.
- **Polars** is the right choice when performance matters, datasets are medium-to-large, or you want a cleaner API than Pandas while staying in Python.
- **Clojure (Tablecloth + Noj)** appeals to developers who value functional programming, immutability, and REPL-driven workflows. It's strongest when the project already lives on the JVM, when correctness matters more than ecosystem breadth, or when you want a single language for both application code and data analysis. The ecosystem is small but thoughtfully designed — and the Clojure Data Cookbook is the community's effort to make it accessible.
