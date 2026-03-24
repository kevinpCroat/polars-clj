(ns polars-clj.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [polars-clj.core :as pl]))

(def test-csv "test/resources/employees.csv")

;; ---------------------------------------------------------------------------
;; Data-driven API
;; ---------------------------------------------------------------------------

(deftest execute-scan-and-collect-test
  (testing "scan-csv + collect returns vector of maps with keyword keys"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:collect]])]
      (is (vector? result))
      (is (= 10 (count result)))
      (is (contains? (first result) :name)))))

(deftest execute-filter-test
  (testing "filter rows by salary > 80000"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:filter [:> :salary 80000]]
                    [:collect]])]
      (is (every? #(> (:salary %) 80000) result))
      (is (= 4 (count result))))))

(deftest execute-select-test
  (testing "select specific columns"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:select [:name :salary]]
                    [:collect]])]
      (is (= #{:name :salary} (set (keys (first result))))))))

(deftest execute-with-columns-test
  (testing "add computed column"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:with-columns [[:as [:/ :salary 12] "monthly"]]]
                    [:limit 1]
                    [:collect]])]
      (is (contains? (first result) :monthly)))))

(deftest execute-sort-test
  (testing "sort by salary ascending"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:sort :salary]
                    [:collect]])
          salaries (mapv :salary result)]
      (is (= salaries (sort salaries))))))

(deftest execute-sort-descending-test
  (testing "sort by salary descending"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:sort :salary {:descending true}]
                    [:collect]])
          salaries (mapv :salary result)]
      (is (= salaries (reverse (sort salaries)))))))

(deftest execute-limit-test
  (testing "limit rows"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:limit 3]
                    [:collect]])]
      (is (= 3 (count result))))))

(deftest execute-group-by-test
  (testing "group by department with aggregations"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:group-by [:department]
                     :agg [[:as [:mean :salary] "avg_salary"]
                           [:as [:count :name] "headcount"]]]
                    [:sort :department]
                    [:collect]])]
      (is (= 3 (count result)))
      (is (every? #(contains? % :avg_salary) result))
      (is (every? #(contains? % :headcount) result)))))

(deftest execute-complex-pipeline-test
  (testing "full pipeline: filter + with-columns + group-by + sort"
    (let [result (pl/execute
                   [[:scan-csv test-csv]
                    [:filter [:= :active true]]
                    [:with-columns [[:as [:/ :salary 12] "monthly"]]]
                    [:group-by [:department]
                     :agg [[:as [:mean :salary] "avg_salary"]
                           [:as [:count :name] "headcount"]]]
                    [:sort "avg_salary" {:descending true}]
                    [:collect]])]
      (is (pos? (count result)))
      (is (every? #(contains? % :avg_salary) result)))))

;; ---------------------------------------------------------------------------
;; Threading API
;; ---------------------------------------------------------------------------

(deftest threading-scan-csv-test
  (testing "threading: scan-csv + collect"
    (let [result (-> (pl/scan-csv test-csv)
                     pl/collect)]
      (is (= 10 (count result))))))

(deftest threading-filter-test
  (testing "threading: filter"
    (let [result (-> (pl/scan-csv test-csv)
                     (pl/filter [:> :salary 80000])
                     pl/collect)]
      (is (= 4 (count result))))))

(deftest threading-select-test
  (testing "threading: select"
    (let [result (-> (pl/scan-csv test-csv)
                     (pl/select [:name :department])
                     pl/collect)]
      (is (= #{:name :department} (set (keys (first result))))))))

(deftest threading-full-pipeline-test
  (testing "threading: full pipeline"
    (let [result (-> (pl/scan-csv test-csv)
                     (pl/filter [:> :salary 50000])
                     (pl/select [:name :department :salary])
                     (pl/sort :salary {:descending true})
                     (pl/limit 5)
                     pl/collect)]
      (is (<= (count result) 5))
      (is (every? #(> (:salary %) 50000) result)))))

(deftest threading-group-by-test
  (testing "threading: group-by"
    (let [result (-> (pl/scan-csv test-csv)
                     (pl/group-by [:department]
                       :agg [[:as [:sum :salary] "total_salary"]])
                     (pl/sort :department)
                     pl/collect)]
      (is (= 3 (count result)))
      (is (every? #(contains? % :total_salary) result)))))

;; ---------------------------------------------------------------------------
;; from-maps (keyword keys)
;; ---------------------------------------------------------------------------

(deftest from-maps-test
  (testing "from-maps creates a LazyFrame from inline data"
    (let [result (-> (pl/from-maps [{:x 1 :y "a"}
                                    {:x 2 :y "b"}
                                    {:x 3 :y "c"}])
                     (pl/filter [:> :x 1])
                     pl/collect)]
      (is (= 2 (count result)))
      (is (every? #(> (:x %) 1) result)))))

;; ---------------------------------------------------------------------------
;; Logical expressions
;; ---------------------------------------------------------------------------

(deftest logical-and-test
  (testing "filter with :and"
    (let [result (-> (pl/scan-csv test-csv)
                     (pl/filter [:and [:> :salary 60000] [:= :active true]])
                     pl/collect)]
      (is (every? #(and (> (:salary %) 60000)
                        (:active %))
                  result)))))

(deftest logical-or-test
  (testing "filter with :or"
    (let [result (-> (pl/scan-csv test-csv)
                     (pl/filter [:or [:> :salary 100000] [:< :age 25]])
                     pl/collect)]
      (is (every? #(or (> (:salary %) 100000)
                       (< (:age %) 25))
                  result)))))

;; ---------------------------------------------------------------------------
;; Explain
;; ---------------------------------------------------------------------------

(deftest explain-test
  (testing "explain returns a query plan string"
    (let [plan (-> (pl/scan-csv test-csv)
                   (pl/filter [:> :salary 50000])
                   pl/explain)]
      (is (string? plan))
      (is (pos? (count plan))))))
