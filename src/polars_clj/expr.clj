(ns polars-clj.expr
  "Expression compiler for the polars-clj DSL.
   Transforms Clojure data structures (keywords, vectors, literals)
   into live Python Polars Expr objects via libpython-clj."
  (:require [polars-clj.interop :as interop]
            [libpython-clj2.python :as py]))

;; ---------------------------------------------------------------------------
;; Dispatch
;; ---------------------------------------------------------------------------

(defn- expr-dispatch
  "Dispatch function for `compile-expr`.
   - keyword?  -> :column
   - vector?   -> the first element (an operator keyword like :+, :sum, etc.)
   - otherwise -> :literal (numbers, strings, booleans, nil)"
  [expr]
  (cond
    (keyword? expr)                          :column
    (vector? expr)                           (first expr)
    (or (number? expr) (string? expr)
        (boolean? expr) (nil? expr))         :literal
    :else                                    :unknown))

(defmulti compile-expr
  "Compile a Clojure expression form into a Polars Expr object.

   Supported forms:
     :col-name                    — column reference
     42, \"hello\", true, nil     — literal value
     [:+ expr expr ...]          — arithmetic / comparison / logical
     [:sum expr]                 — aggregation
     [:as expr \"alias\"]         — alias"
  #'expr-dispatch)

;; ---------------------------------------------------------------------------
;; Column & Literal
;; ---------------------------------------------------------------------------

(defmethod compile-expr :column
  [kw]
  (interop/col kw))

(defmethod compile-expr :literal
  [v]
  (interop/lit v))

;; ---------------------------------------------------------------------------
;; Helpers — macro to reduce operator boilerplate
;; ---------------------------------------------------------------------------

(defmacro ^:private def-binary-op
  "Define a compile-expr method for a binary comparison operator.
   `dispatch-kw` is the DSL keyword (e.g. :>), `py-method` is the
   Python dunder name (e.g. __gt__)."
  [dispatch-kw py-method]
  `(defmethod compile-expr ~dispatch-kw
     [[_# left# right#]]
     (py/py. (compile-expr left#) ~py-method (compile-expr right#))))

(defmacro ^:private def-variadic-op
  "Define a compile-expr method for a variadic (left-fold) operator.
   `dispatch-kw` is the DSL keyword (e.g. :+), `py-method` is the
   Python dunder name (e.g. __add__)."
  [dispatch-kw py-method]
  `(defmethod compile-expr ~dispatch-kw
     [[_# first-arg# & rest-args#]]
     (reduce (fn [a# b#]
               (py/py. a# ~py-method (compile-expr b#)))
             (compile-expr first-arg#)
             rest-args#)))

(defmacro ^:private def-unary-agg
  "Define a compile-expr method for a unary aggregation/transform.
   `dispatch-kw` is the DSL keyword (e.g. :sum), `py-method` is the
   Polars method name (e.g. sum)."
  [dispatch-kw py-method]
  `(defmethod compile-expr ~dispatch-kw
     [[_# inner#]]
     (py/py. (compile-expr inner#) ~py-method)))

;; ---------------------------------------------------------------------------
;; Arithmetic (variadic, left-fold)
;; ---------------------------------------------------------------------------

(def-variadic-op :+  __add__)
(def-variadic-op :-  __sub__)
(def-variadic-op :*  __mul__)
(def-variadic-op :/  __truediv__)
(def-variadic-op :** __pow__)

;; ---------------------------------------------------------------------------
;; Comparison (binary)
;; ---------------------------------------------------------------------------

(def-binary-op :>  __gt__)
(def-binary-op :<  __lt__)
(def-binary-op :>= __ge__)
(def-binary-op :<= __le__)
(def-binary-op :=  __eq__)
(def-binary-op :!= __ne__)

;; ---------------------------------------------------------------------------
;; Logical (variadic for and/or, unary for not)
;; ---------------------------------------------------------------------------

(def-variadic-op :and __and__)
(def-variadic-op :or  __or__)

(defmethod compile-expr :not
  [[_ inner]]
  (py/py. (compile-expr inner) __invert__))

;; ---------------------------------------------------------------------------
;; Alias
;; ---------------------------------------------------------------------------

(defmethod compile-expr :as
  [[_ inner alias-name]]
  (py/py. (compile-expr inner) alias alias-name))

;; ---------------------------------------------------------------------------
;; Aggregations / Transforms (unary)
;; ---------------------------------------------------------------------------

(def-unary-agg :sum        sum)
(def-unary-agg :mean       mean)
(def-unary-agg :min        min)
(def-unary-agg :max        max)
(def-unary-agg :count      count)
(def-unary-agg :first      first)
(def-unary-agg :last       last)
(def-unary-agg :n-unique   n_unique)
(def-unary-agg :is-null    is_null)
(def-unary-agg :is-not-null is_not_null)
(def-unary-agg :abs        abs)
(def-unary-agg :sqrt       sqrt)

;; ---------------------------------------------------------------------------
;; When / Then / Otherwise (conditional)
;; ---------------------------------------------------------------------------

(defmethod compile-expr :when
  [[_ pred then-val else-val]]
  (let [pl-mod (interop/pl-mod)
        when-obj (py/call-attr pl-mod "when" (compile-expr pred))
        then-obj (py/py. when-obj then (compile-expr then-val))]
    (if else-val
      (py/py. then-obj otherwise (compile-expr else-val))
      then-obj)))

;; ---------------------------------------------------------------------------
;; String operations — :str/* namespace
;; ---------------------------------------------------------------------------

(defmacro ^:private def-str-unary
  "Define a compile-expr method for a unary string accessor operation.
   `dispatch-kw` is the DSL keyword (e.g. :str/to-lowercase), `py-method`
   is the Python method name on the .str accessor (e.g. to_lowercase)."
  [dispatch-kw py-method]
  `(defmethod compile-expr ~dispatch-kw
     [[_# inner#]]
     (let [str-acc# (py/get-attr (compile-expr inner#) "str")]
       (py/py. str-acc# ~py-method))))

(defmacro ^:private def-str-binary
  "Define a compile-expr method for a binary string accessor operation
   (inner expression + one argument).
   `dispatch-kw` is the DSL keyword (e.g. :str/contains), `py-method`
   is the Python method name on the .str accessor (e.g. contains)."
  [dispatch-kw py-method]
  `(defmethod compile-expr ~dispatch-kw
     [[_# inner# arg#]]
     (let [str-acc# (py/get-attr (compile-expr inner#) "str")]
       (py/py. str-acc# ~py-method (compile-expr arg#)))))

(def-str-unary :str/to-lowercase to_lowercase)
(def-str-unary :str/to-uppercase to_uppercase)
(def-str-unary :str/len          len_chars)
(def-str-unary :str/strip-chars  strip_chars)

(def-str-binary :str/contains    contains)
(def-str-binary :str/starts-with starts_with)
(def-str-binary :str/ends-with   ends_with)

(defmethod compile-expr :str/replace
  [[_ inner old-val new-val]]
  (let [str-acc (py/get-attr (compile-expr inner) "str")]
    (py/py. str-acc replace (compile-expr old-val) (compile-expr new-val))))

;; ---------------------------------------------------------------------------
;; Cast — type conversion
;; ---------------------------------------------------------------------------

(def ^:private dtype-map
  "Map of DSL dtype keywords to Polars dtype attribute names."
  {:int8     "Int8"     :int16    "Int16"    :int32    "Int32"    :int64    "Int64"
   :uint8    "UInt8"    :uint16   "UInt16"   :uint32   "UInt32"   :uint64   "UInt64"
   :float32  "Float32"  :float64  "Float64"
   :utf8     "Utf8"     :string   "String"
   :bool     "Boolean"  :boolean  "Boolean"
   :date     "Date"     :datetime "Datetime"})

(defmethod compile-expr :cast
  [[_ inner dtype-kw]]
  (let [dtype-name (or (get dtype-map dtype-kw)
                       (throw (ex-info (str "Unknown dtype: " dtype-kw)
                                       {:dtype dtype-kw})))
        dtype-obj  (py/get-attr (interop/pl-mod) dtype-name)]
    (py/py. (compile-expr inner) cast dtype-obj)))

;; ---------------------------------------------------------------------------
;; Default — unknown expression form
;; ---------------------------------------------------------------------------

(defmethod compile-expr :default
  [expr]
  (throw (ex-info (str "Unknown expression form: " (pr-str expr)
                       ". Expected a keyword (column), literal value, "
                       "or vector like [:op ...args].")
                  {:expr expr})))
