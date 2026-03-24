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

;; ---------------------------------------------------------------------------
;; Default — unknown expression form
;; ---------------------------------------------------------------------------

(defmethod compile-expr :default
  [expr]
  (throw (ex-info (str "Unknown expression form: " (pr-str expr)
                       ". Expected a keyword (column), literal value, "
                       "or vector like [:op ...args].")
                  {:expr expr})))
