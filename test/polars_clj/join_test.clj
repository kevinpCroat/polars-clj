(ns polars-clj.join-test
  (:require [clojure.test :refer [deftest is testing]]
            [polars-clj.core :as pl]))

(def employees-csv "test/resources/employees.csv")
(def departments-csv "test/resources/departments.csv")

;; ---------------------------------------------------------------------------
;; Join tests
;; ---------------------------------------------------------------------------

(deftest inner-join-test
  (testing "inner join employees with departments using left-on / right-on"
    (let [result (-> (pl/scan-csv employees-csv)
                     (pl/join (pl/scan-csv departments-csv)
                              :how :inner
                              :left-on [:department]
                              :right-on [:dept-name])
                     pl/collect)]
      (is (= 10 (count result)))
      (is (every? #(contains? % :location) result))
      (is (every? #(contains? % :budget) result))
      (is (every? #(contains? % :name) result)))))

(deftest left-join-test
  (testing "left join preserves all left rows"
    (let [result (-> (pl/scan-csv employees-csv)
                     (pl/join (pl/scan-csv departments-csv)
                              :how :left
                              :left-on [:department]
                              :right-on [:dept-name])
                     pl/collect)]
      (is (= 10 (count result)))
      (is (every? #(contains? % :location) result)))))

(deftest join-on-shorthand-test
  (testing "join with :on shorthand when both sides have the same column name"
    (let [left  (pl/from-maps [{:id 1 :val "a"}
                               {:id 2 :val "b"}
                               {:id 3 :val "c"}])
          right (pl/from-maps [{:id 1 :score 100}
                               {:id 2 :score 200}])
          result (-> left
                     (pl/join right :how :inner :on [:id])
                     pl/collect)]
      (is (= 2 (count result)))
      (is (every? #(contains? % :score) result))
      (is (every? #(contains? % :val) result)))))

(deftest join-left-on-right-on-test
  (testing "join with explicit :left-on and :right-on (different column names)"
    (let [left  (pl/from-maps [{"emp_id" 1 "name" "Alice"}
                               {"emp_id" 2 "name" "Bob"}])
          right (pl/from-maps [{"person_id" 1 "dept" "Eng"}
                               {"person_id" 2 "dept" "Sales"}])
          result (-> left
                     (pl/join right
                              :how :inner
                              :left-on [:emp-id]
                              :right-on [:person-id])
                     pl/collect)]
      (is (= 2 (count result)))
      (is (= #{"Alice" "Bob"} (set (map :name result))))
      (is (every? #(contains? % :dept) result)))))

;; ---------------------------------------------------------------------------
;; Unique tests
;; ---------------------------------------------------------------------------

(deftest unique-test
  (testing "unique removes duplicate rows"
    (let [result (-> (pl/from-maps [{:x 1 :y "a"}
                                    {:x 1 :y "a"}
                                    {:x 2 :y "b"}])
                     pl/unique
                     pl/collect)]
      (is (= 2 (count result))))))

(deftest unique-subset-test
  (testing "unique with subset considers only specified columns"
    (let [result (-> (pl/from-maps [{:x 1 :y "a"}
                                    {:x 1 :y "b"}
                                    {:x 2 :y "c"}])
                     (pl/unique [:x])
                     pl/collect)]
      (is (= 2 (count result))))))

;; ---------------------------------------------------------------------------
;; Rename tests
;; ---------------------------------------------------------------------------

(deftest rename-test
  (testing "rename columns"
    (let [result (-> (pl/from-maps [{:x 1 :y 2}
                                    {:x 3 :y 4}])
                     (pl/rename {:x "a" :y "b"})
                     pl/collect)]
      (is (every? #(contains? % :a) result))
      (is (every? #(contains? % :b) result))
      (is (not (contains? (first result) :x)))
      (is (not (contains? (first result) :y))))))
