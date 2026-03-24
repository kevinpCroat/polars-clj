(ns polars-clj.interop
  "Low-level libpython-clj bridge to the Python polars library.
   Every other namespace in polars-clj depends on this one."
  (:require [libpython-clj2.python :as py]
            [libpython-clj2.require :refer [require-python]]
            [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn kw->col-name
  "Convert a keyword or string column name to a Python-friendly string,
   replacing hyphens with underscores."
  [k]
  (if (keyword? k)
    (str/replace (name k) "-" "_")
    (str k)))

(defn- opts->python-kwargs
  "Convert a Clojure map of options to a map whose keys are
   underscore-style strings suitable for Python kwargs."
  [m]
  (when m
    (into {}
          (map (fn [[k v]]
                 [(kw->col-name k) v]))
          m)))

;; ---------------------------------------------------------------------------
;; Python / Polars initialisation
;; ---------------------------------------------------------------------------

(def ^:private polars-init
  "Thread-safe lazy initialisation of Python + Polars."
  (delay
    (try
      (py/initialize!)
      (require-python 'polars)
      (py/import-module "polars")
      (catch Exception e
        (throw (ex-info
                (str "Failed to initialise Python/Polars. "
                     "Make sure Python is installed and the `polars` "
                     "package is available on the Python path.")
                {:cause e}))))))

(defn pl-mod
  "Return the polars Python module reference, initialising if necessary."
  []
  @polars-init)

;; ---------------------------------------------------------------------------
;; Expression constructors
;; ---------------------------------------------------------------------------

(defn col
  "Create a Polars column expression (`pl.col(col_name)`)."
  [col-name]
  (py/call-attr (pl-mod) "col" (kw->col-name col-name)))

(defn lit
  "Create a Polars literal expression (`pl.lit(value)`)."
  [value]
  (py/call-attr (pl-mod) "lit" value))

;; ---------------------------------------------------------------------------
;; I/O — lazy scanners
;; ---------------------------------------------------------------------------

(defn scan-csv
  "Lazily scan a CSV file, returning a Polars LazyFrame.
   `opts` is an optional map of keyword options forwarded as Python kwargs
   (e.g. `{:separator \",\" :has-header true}`)."
  ([path]
   (scan-csv path nil))
  ([path opts]
   (if opts
     (py/call-attr-kw (pl-mod) "scan_csv" [path] (opts->python-kwargs opts))
     (py/call-attr (pl-mod) "scan_csv" path))))

(defn scan-parquet
  "Lazily scan a Parquet file, returning a Polars LazyFrame.
   `opts` is an optional map of keyword options forwarded as Python kwargs."
  ([path]
   (scan-parquet path nil))
  ([path opts]
   (if opts
     (py/call-attr-kw (pl-mod) "scan_parquet" [path] (opts->python-kwargs opts))
     (py/call-attr (pl-mod) "scan_parquet" path))))

;; ---------------------------------------------------------------------------
;; DataFrame construction
;; ---------------------------------------------------------------------------

(defn from-maps
  "Build a Polars LazyFrame from a vector of Clojure maps.
   Creates an eager DataFrame via `pl.DataFrame(data)` then calls `.lazy()`."
  [data]
  (let [df (py/call-attr (pl-mod) "DataFrame" (py/->python data))]
    (py/call-attr df "lazy")))

;; ---------------------------------------------------------------------------
;; DataFrame -> Clojure conversion
;; ---------------------------------------------------------------------------

(defn dataframe->clj
  "Convert a Polars DataFrame to Clojure data structures.

   `format` controls the output shape:
     :row-maps  (default) — vector of maps, one per row  (`df.to_dicts()`)
     :col-maps  — map of column-name to vector of values  (`df.to_dict()`)
     :raw       — return the Python DataFrame unchanged"
  ([df]
   (dataframe->clj df :row-maps))
  ([df format]
   (letfn [(keywordize-map [m]
             (into {} (map (fn [[k v]] [(keyword k) v])) m))]
     (case format
       :row-maps (mapv keywordize-map (py/->jvm (py/call-attr df "to_dicts")))
       :col-maps (keywordize-map (py/->jvm (py/call-attr df "to_dict")))
       :raw      df
       (throw (ex-info (str "Unknown format: " format
                            ". Expected :row-maps, :col-maps, or :raw.")
                       {:format format}))))))
