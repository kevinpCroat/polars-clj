(ns polars-clj.validation-test
  "Tests for input validation and error messages."
  (:require [clojure.test :refer [deftest is testing]]
            [polars-clj.core :as pl]
            [polars-clj.pipeline :as pipeline]
            [polars-clj.expr :as expr]))

;; ---------------------------------------------------------------------------
;; Unknown pipeline step
;; ---------------------------------------------------------------------------

(deftest unknown-pipeline-step-test
  (testing "unknown step keyword throws ex-info with descriptive message"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown pipeline step: :bogus"
                          (pipeline/execute-step nil [:bogus "abc"])))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown pipeline step: :drop"
                          (pipeline/execute-step nil [:drop :col-a])))))

;; ---------------------------------------------------------------------------
;; Unknown expression form
;; ---------------------------------------------------------------------------

(deftest unknown-expression-form-test
  (testing "unknown operator in expression vector throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown expression form"
                          (expr/compile-expr [:nonexistent :a :b]))))
  (testing "unsupported type (e.g. a set) throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Unknown expression form"
                          (expr/compile-expr #{:a :b})))))

;; ---------------------------------------------------------------------------
;; Empty / invalid pipeline
;; ---------------------------------------------------------------------------

(deftest empty-pipeline-test
  (testing "empty vector pipeline throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Pipeline must be a non-empty sequence of steps"
                          (pl/execute []))))
  (testing "nil pipeline throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Pipeline must be a non-empty sequence of steps"
                          (pl/execute nil)))))

;; ---------------------------------------------------------------------------
;; Transform step without source LazyFrame
;; ---------------------------------------------------------------------------

(deftest transform-without-source-test
  (testing "filter without a source LazyFrame throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"requires a LazyFrame"
                          (pipeline/execute-step nil [:filter [:> :x 1]]))))
  (testing "select without a source LazyFrame throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"requires a LazyFrame"
                          (pipeline/execute-step nil [:select [:x :y]]))))
  (testing "collect without a source LazyFrame throws ex-info"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"requires a LazyFrame"
                          (pipeline/execute-step nil [:collect])))))
