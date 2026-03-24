(ns polars-clj.pipeline
  "Pipeline executor for polars-clj.
   Walks a vector of operation steps and executes each by calling the
   appropriate Polars LazyFrame method via libpython-clj."
  (:require [polars-clj.interop :as interop]
            [polars-clj.expr :as expr]
            [libpython-clj2.python :as py]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- assert-lazyframe
  "Throw if `lf` is nil, indicating a missing source step."
  [lf step-kw]
  (when (nil? lf)
    (throw (ex-info (str "Step " step-kw " requires a LazyFrame, but none was provided. "
                         "Did you forget a source step (:scan-csv, :scan-parquet, :from-maps)?")
                    {:step step-kw}))))

;; ---------------------------------------------------------------------------
;; Multimethod
;; ---------------------------------------------------------------------------

(defmulti execute-step
  "Execute a single pipeline step against the current LazyFrame.
   Dispatches on the operation keyword (first element of the step vector)."
  (fn [_lf step] (first step)))

;; ---------------------------------------------------------------------------
;; Source operations (lf is nil)
;; ---------------------------------------------------------------------------

(defmethod execute-step :scan-csv
  [_ step]
  (let [[_ path opts] step]
    (interop/scan-csv path opts)))

(defmethod execute-step :scan-parquet
  [_ step]
  (let [[_ path opts] step]
    (interop/scan-parquet path opts)))

(defmethod execute-step :from-maps
  [_ step]
  (let [[_ data] step]
    (interop/from-maps data)))

;; ---------------------------------------------------------------------------
;; Transform operations (lf must be a LazyFrame)
;; ---------------------------------------------------------------------------

(defmethod execute-step :filter
  [lf step]
  (assert-lazyframe lf :filter)
  (let [[_ expr-form] step
        compiled (expr/compile-expr expr-form)]
    (py/py. lf filter compiled)))

(defmethod execute-step :select
  [lf step]
  (assert-lazyframe lf :select)
  (let [[_ exprs] step
        compiled (mapv expr/compile-expr exprs)]
    (py/py. lf select (py/->py-list compiled))))

(defmethod execute-step :with-columns
  [lf step]
  (assert-lazyframe lf :with-columns)
  (let [[_ exprs] step
        compiled (mapv expr/compile-expr exprs)]
    (py/py. lf with_columns (py/->py-list compiled))))

(defmethod execute-step :sort
  [lf step]
  (assert-lazyframe lf :sort)
  (let [[_ col-spec opts] step
        by (if (vector? col-spec)
             (py/->py-list (mapv interop/kw->col-name col-spec))
             (interop/kw->col-name col-spec))]
    (if-let [desc (:descending opts)]
      (py/py. lf sort by :descending desc)
      (py/py. lf sort by))))

(defmethod execute-step :limit
  [lf step]
  (assert-lazyframe lf :limit)
  (let [[_ n] step]
    (py/py. lf limit n)))

(defmethod execute-step :group-by
  [lf step]
  (assert-lazyframe lf :group-by)
  (let [[_ groups & {:keys [agg]}] step
        group-strs   (py/->py-list (mapv interop/kw->col-name groups))
        compiled-aggs (py/->py-list (mapv expr/compile-expr agg))
        grouped      (py/py. lf group_by group-strs)]
    (py/py. grouped agg compiled-aggs)))

;; ---------------------------------------------------------------------------
;; Terminal operations
;; ---------------------------------------------------------------------------

(defmethod execute-step :collect
  [lf step]
  (assert-lazyframe lf :collect)
  (let [[_ opts] step
        df (if (:streaming opts)
             (py/py. lf collect :streaming true)
             (py/py. lf collect))
        fmt (get opts :format :row-maps)]
    (interop/dataframe->clj df fmt)))

(defmethod execute-step :explain
  [lf step]
  (assert-lazyframe lf :explain)
  (py/py. lf explain))

;; ---------------------------------------------------------------------------
;; Default — unknown step
;; ---------------------------------------------------------------------------

(defmethod execute-step :default
  [_ step]
  (throw (ex-info (str "Unknown pipeline step: " (first step)
                       ". Supported steps: :scan-csv, :scan-parquet, :from-maps, "
                       ":filter, :select, :with-columns, :sort, :limit, :group-by, "
                       ":collect, :explain")
                  {:step step})))

;; ---------------------------------------------------------------------------
;; Entry point
;; ---------------------------------------------------------------------------

(defn execute-pipeline
  "Execute a pipeline of operations. Each step is a vector like `[:op & args]`.
   Steps are reduced left-to-right: a source step produces a LazyFrame,
   transform steps thread it through, and a terminal step (:collect, :explain)
   returns the final result."
  [steps]
  (when-not (and (sequential? steps) (seq steps))
    (throw (ex-info "Pipeline must be a non-empty sequence of steps." {:steps steps})))
  (reduce execute-step nil steps))
