(ns polars-clj.core
  "Public API for the polars-clj project.

   Two styles of usage:

   **Data-driven** — describe a pipeline as a vector of steps and hand it to
   `execute`:

       (pl/execute
         [[:scan-csv \"data.csv\"]
          [:filter [:> :salary 50000]]
          [:select [:name :salary]]
          [:collect]])

   **Threading-friendly** — compose individual functions with `->`:

       (-> (pl/scan-csv \"data.csv\")
           (pl/filter [:> :salary 50000])
           (pl/select [:name :salary])
           pl/collect)

   All transform functions accept a LazyFrame as their first argument and
   return a LazyFrame so they compose naturally with `->`.  Source functions
   (`scan-csv`, `scan-parquet`, `from-maps`) produce LazyFrames; terminal
   functions (`collect`, `explain`) consume them."
  (:refer-clojure :exclude [filter sort group-by])
  (:require [polars-clj.pipeline :as pipeline]
            [polars-clj.interop :as interop]
            [polars-clj.expr :as expr]
            [libpython-clj2.python :as py]
            [clojure.pprint :as pprint]))

;; ---------------------------------------------------------------------------
;; Data-driven API
;; ---------------------------------------------------------------------------

(defn execute
  "Executes a pipeline of operations defined as a vector of steps.
   Each step is a vector like [:op ...args].

   Example:
     (execute
       [[:scan-csv \"data.csv\"]
        [:filter [:> :salary 50000]]
        [:select [:name :salary]]
        [:collect]])"
  [pipeline]
  (pipeline/execute-pipeline pipeline))

;; ---------------------------------------------------------------------------
;; Source functions — no LazyFrame argument, return a LazyFrame
;; ---------------------------------------------------------------------------

(defn scan-csv
  "Lazily scan a CSV file, returning a Polars LazyFrame.
   `opts` is an optional map of keyword options forwarded to Polars
   (e.g. `{:separator \",\" :has-header true}`).

   Examples:
     ;; standalone
     (def lf (pl/scan-csv \"data.csv\"))
     (def lf (pl/scan-csv \"data.csv\" {:separator \";\"}))

     ;; threaded
     (-> (pl/scan-csv \"data.csv\")
         (pl/select [:name :age])
         pl/collect)"
  ([path]
   (pipeline/execute-step nil [:scan-csv path]))
  ([path opts]
   (pipeline/execute-step nil [:scan-csv path opts])))

(defn scan-parquet
  "Lazily scan a Parquet file, returning a Polars LazyFrame.
   `opts` is an optional map of keyword options forwarded to Polars.

   Examples:
     (def lf (pl/scan-parquet \"data.parquet\"))

     (-> (pl/scan-parquet \"data.parquet\")
         (pl/limit 100)
         pl/collect)"
  ([path]
   (pipeline/execute-step nil [:scan-parquet path]))
  ([path opts]
   (pipeline/execute-step nil [:scan-parquet path opts])))

(defn from-maps
  "Build a Polars LazyFrame from a vector of Clojure maps.

   Example:
     (-> (pl/from-maps [{:name \"Alice\" :age 30}
                        {:name \"Bob\"   :age 25}])
         (pl/filter [:> :age 27])
         pl/collect)"
  [data]
  (pipeline/execute-step nil [:from-maps data]))

;; ---------------------------------------------------------------------------
;; Transform functions — take a LazyFrame, return a LazyFrame
;; ---------------------------------------------------------------------------

(defn filter
  "Filter rows of `lf` that match `expr-data`, a polars-clj expression.

   Examples:
     (pl/filter lf [:> :salary 50000])
     (pl/filter lf [:and [:> :age 18] [:= :active true]])

     ;; threaded
     (-> lf
         (pl/filter [:> :salary 50000])
         pl/collect)"
  [lf expr-data]
  (pipeline/execute-step lf [:filter expr-data]))

(defn select
  "Select columns from `lf`. `col-exprs` is a vector of column references
   (keywords) or expression vectors.

   Examples:
     (pl/select lf [:name :salary])
     (pl/select lf [:name [:as [:* :price :qty] \"total\"]])

     ;; threaded
     (-> lf
         (pl/select [:name :salary])
         pl/collect)"
  [lf col-exprs]
  (pipeline/execute-step lf [:select col-exprs]))

(defn with-columns
  "Add or overwrite columns in `lf`. `col-exprs` is a vector of expression
   vectors, each typically aliased with `:as`.

   Examples:
     (pl/with-columns lf [[:as [:* :price :qty] \"total\"]])

     ;; threaded
     (-> lf
         (pl/with-columns [[:as [:+ :a :b] \"sum\"]])
         pl/collect)"
  [lf col-exprs]
  (pipeline/execute-step lf [:with-columns col-exprs]))

(defn sort
  "Sort `lf` by one or more columns. `col-or-cols` is a keyword (single
   column) or a vector of keywords. `opts` is an optional map, e.g.
   `{:descending true}`.

   Examples:
     (pl/sort lf :salary)
     (pl/sort lf [:dept :salary] {:descending true})

     ;; threaded
     (-> lf
         (pl/sort :salary {:descending true})
         pl/collect)"
  ([lf col-or-cols]
   (pipeline/execute-step lf [:sort col-or-cols]))
  ([lf col-or-cols opts]
   (pipeline/execute-step lf [:sort col-or-cols opts])))

(defn limit
  "Take the first `n` rows of `lf`.

   Examples:
     (pl/limit lf 5)

     ;; threaded
     (-> lf (pl/limit 10) pl/collect)"
  [lf n]
  (pipeline/execute-step lf [:limit n]))

(defn group-by
  "Group `lf` by the given columns and apply aggregations.
   `groups` is a keyword or vector of keywords identifying the grouping
   columns.  `:agg` is a vector of aggregation expressions.

   Examples:
     (pl/group-by lf [:dept] :agg [[:as [:mean :salary] \"avg_salary\"]])

     ;; threaded
     (-> lf
         (pl/group-by [:dept]
           :agg [[:as [:sum :revenue] \"total_revenue\"]])
         pl/collect)"
  [lf groups & {:keys [agg]}]
  (pipeline/execute-step lf [:group-by groups :agg agg]))

;; ---------------------------------------------------------------------------
;; Terminal functions — take a LazyFrame, return Clojure data
;; ---------------------------------------------------------------------------

(defn collect
  "Materialise the LazyFrame into Clojure data (a vector of maps by default).
   `opts` is an optional map passed through to the pipeline step, e.g.
   `{:format :col-maps}`.

   Examples:
     (pl/collect lf)
     (pl/collect lf {:format :col-maps})

     ;; threaded
     (-> lf (pl/filter [:> :age 21]) pl/collect)"
  ([lf]
   (pipeline/execute-step lf [:collect]))
  ([lf opts]
   (pipeline/execute-step lf [:collect opts])))

(defn explain
  "Return the optimised query plan for `lf` as a string.  Useful for
   understanding what Polars will actually execute.

   Example:
     (println (pl/explain lf))"
  [lf]
  (pipeline/execute-step lf [:explain]))

;; ---------------------------------------------------------------------------
;; Convenience helpers
;; ---------------------------------------------------------------------------

(defn show
  "Collects the LazyFrame and prints it. Returns the Clojure data.
   Useful for REPL exploration.

   Examples:
     (pl/show lf)       ;; prints first 10 rows
     (pl/show lf 25)    ;; prints first 25 rows"
  ([lf] (show lf 10))
  ([lf n]
   (let [result (-> lf (limit n) collect)]
     (pprint/pprint result)
     result)))
