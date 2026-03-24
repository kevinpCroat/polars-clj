(ns polars-clj.expr-test
  "Tests for extended expression types: when/then/otherwise,
   string operations, cast, abs, and sqrt."
  (:require [clojure.test :refer [deftest is testing]]
            [polars-clj.core :as pl]))

;; ---------------------------------------------------------------------------
;; When / Then / Otherwise
;; ---------------------------------------------------------------------------

(deftest when-then-otherwise-test
  (testing "categorize ages as adult or minor"
    (let [result (-> (pl/from-maps [{:name "Alice" :age 30}
                                    {:name "Bob"   :age 17}
                                    {:name "Carol" :age 21}
                                    {:name "Dave"  :age 15}])
                     (pl/with-columns
                       [[:as [:when [:>= :age 18] "adult" "minor"] "category"]])
                     pl/collect)]
      (is (= 4 (count result)))
      (is (= "adult" (:category (first result))))
      (is (= "minor" (:category (second result))))
      (is (= "adult" (:category (nth result 2))))
      (is (= "minor" (:category (nth result 3)))))))

(deftest when-then-without-otherwise-test
  (testing "when/then without otherwise returns null for non-matching rows"
    (let [result (-> (pl/from-maps [{:name "Alice" :age 30}
                                    {:name "Bob"   :age 17}])
                     (pl/with-columns
                       [[:as [:when [:>= :age 18] "adult"] "category"]])
                     pl/collect)]
      (is (= "adult" (:category (first result))))
      (is (nil? (:category (second result)))))))

;; ---------------------------------------------------------------------------
;; String operations
;; ---------------------------------------------------------------------------

(deftest str-to-lowercase-test
  (testing "str/to-lowercase converts strings to lower case"
    (let [result (-> (pl/from-maps [{:name "ALICE"} {:name "Bob"}])
                     (pl/with-columns
                       [[:as [:str/to-lowercase :name] "lower"]])
                     pl/collect)]
      (is (= "alice" (:lower (first result))))
      (is (= "bob"   (:lower (second result)))))))

(deftest str-to-uppercase-test
  (testing "str/to-uppercase converts strings to upper case"
    (let [result (-> (pl/from-maps [{:name "alice"} {:name "Bob"}])
                     (pl/with-columns
                       [[:as [:str/to-uppercase :name] "upper"]])
                     pl/collect)]
      (is (= "ALICE" (:upper (first result))))
      (is (= "BOB"   (:upper (second result)))))))

(deftest str-contains-test
  (testing "str/contains checks for substring presence"
    (let [result (-> (pl/from-maps [{:name "Alice"} {:name "Bob"} {:name "Alicia"}])
                     (pl/filter [:str/contains :name "Ali"])
                     pl/collect)]
      (is (= 2 (count result)))
      (is (every? #(clojure.string/includes? (:name %) "Ali") result)))))

(deftest str-starts-with-test
  (testing "str/starts-with filters by prefix"
    (let [result (-> (pl/from-maps [{:name "Alice"} {:name "Bob"} {:name "Alicia"}])
                     (pl/filter [:str/starts-with :name "Al"])
                     pl/collect)]
      (is (= 2 (count result)))
      (is (every? #(clojure.string/starts-with? (:name %) "Al") result)))))

(deftest str-ends-with-test
  (testing "str/ends-with filters by suffix"
    (let [result (-> (pl/from-maps [{:name "Alice"} {:name "Bob"} {:name "Grace"}])
                     (pl/filter [:str/ends-with :name "ce"])
                     pl/collect)]
      (is (= 2 (count result)))
      (is (every? #(clojure.string/ends-with? (:name %) "ce") result)))))

(deftest str-len-test
  (testing "str/len computes string length in characters"
    (let [result (-> (pl/from-maps [{:name "Al"} {:name "Bob"} {:name "Carol"}])
                     (pl/with-columns
                       [[:as [:str/len :name] "name_len"]])
                     pl/collect)]
      (is (= 2 (:name_len (first result))))
      (is (= 3 (:name_len (second result))))
      (is (= 5 (:name_len (nth result 2)))))))

(deftest str-replace-test
  (testing "str/replace substitutes first occurrence of a substring"
    (let [result (-> (pl/from-maps [{:greeting "hello world"}
                                    {:greeting "hello there"}])
                     (pl/with-columns
                       [[:as [:str/replace :greeting "hello" "hi"] "replaced"]])
                     pl/collect)]
      (is (= "hi world" (:replaced (first result))))
      (is (= "hi there" (:replaced (second result)))))))

;; ---------------------------------------------------------------------------
;; Cast
;; ---------------------------------------------------------------------------

(deftest cast-int-to-float-test
  (testing "cast converts integer column to float64"
    (let [result (-> (pl/from-maps [{:x 1} {:x 2} {:x 3}])
                     (pl/with-columns
                       [[:as [:cast :x :float64] "x_float"]])
                     pl/collect)]
      (is (= 3 (count result)))
      (is (every? #(float? (:x_float %)) result))
      (is (= 1.0 (:x_float (first result))))
      (is (= 2.0 (:x_float (second result)))))))

;; ---------------------------------------------------------------------------
;; Abs
;; ---------------------------------------------------------------------------

(deftest abs-test
  (testing "abs returns absolute value of negative numbers"
    (let [result (-> (pl/from-maps [{:val -5} {:val 3} {:val -1} {:val 0}])
                     (pl/with-columns
                       [[:as [:abs :val] "abs_val"]])
                     pl/collect)]
      (is (= 5 (:abs_val (first result))))
      (is (= 3 (:abs_val (second result))))
      (is (= 1 (:abs_val (nth result 2))))
      (is (= 0 (:abs_val (nth result 3)))))))

;; ---------------------------------------------------------------------------
;; Sqrt
;; ---------------------------------------------------------------------------

(deftest sqrt-test
  (testing "sqrt computes square root"
    (let [result (-> (pl/from-maps [{:val 4} {:val 9} {:val 16}])
                     (pl/with-columns
                       [[:as [:sqrt :val] "root"]])
                     pl/collect)]
      (is (= 2.0 (:root (first result))))
      (is (= 3.0 (:root (second result))))
      (is (= 4.0 (:root (nth result 2)))))))
