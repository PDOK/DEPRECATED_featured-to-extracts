(ns ^:regression pdok.featured-to-extracts.regression-test
  (:require [pdok.featured-to-extracts
             [core :as e]]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(defn- inserted-features [extracts dataset extract-type collection features]
  (let [new-extracts (map #(vector (:_version %) (:_valid_from %) (:_valid_to %) %) features)]
    (swap! extracts concat new-extracts)))

(defn- remove-first [pred coll]
  (keep-indexed #(if (not= %1 (.indexOf coll (first (filter pred coll)))) %2) coll))

(defn- remove-extract-record [extracts record-info]
  (let [[version valid-from] record-info]
    (if valid-from
      (remove-first #(and (= version (nth %1 0))
                          (= valid-from (nth %1 1))) extracts)
      (remove #(= version (nth % 0)) extracts))))

(defn- deleted-versions [extracts dataset collection extract-type records]
  (doseq [record records]
    (swap! extracts remove-extract-record record)))

(defn process-feature-permutation [changelog]
  (let [extracts (atom [])]
    (with-bindings
      {#'e/*process-insert-extract* (partial inserted-features extracts)
       #'e/*process-delete-extract* (partial deleted-versions extracts)
       #'e/*initialized-collection?* (constantly true)}
      (let [result (e/update-extracts "regression-set" ["gml"] changelog)
            _ (println result)]
        {:statistics result
         :extracts   extracts}))))

(defmethod clojure.test/report :begin-test-var [m]
  (with-test-out
    (println ">>" (-> m :var meta :name))))

(defmacro defregressiontest* [name file results-var & body]
  `(deftest ~name
     (let [changelog# (io/resource (str "regression/" ~file ".changelog"))]
       (let [~results-var (process-feature-permutation changelog#)]
         ~@body
         ))))

(defmacro defregressiontest [test-name results-var & body]
  `(defregressiontest* ~test-name ~(name test-name) ~results-var ~@body))

(defn- test-timeline->extract [expected extracts]
  (is (= (:n-extracts expected) (count @extracts)))
  (is (= (:n-extracts expected) (count (distinct (map first @extracts))))) ; extracts should have distinct versions
  (is (= (:n-valid-to expected) (count (filter #((complement nil?) (nth % 2)) @extracts)))))

(defregressiontest new results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new-with-child results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new-change results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}
                                           (:extracts results)))

(defregressiontest new-delete results
                   (test-timeline->extract {:n-extracts 0
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new-change-change results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}
                                           (:extracts results)))

(defregressiontest new-change-close results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 2}
                                           (:extracts results)))

(defregressiontest new-change-close_with_attributes results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 2}
                                           (:extracts results)))

(defregressiontest new-change-change-delete results
                   (test-timeline->extract {:n-extracts 0
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new-change-change-delete-new-change results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}
                                           (:extracts results)))

(defregressiontest new_with_nested_null_geom results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new_with_nested_crappy_geom results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new_nested-change_nested-change_nested results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}
                                           (:extracts results)))

(defregressiontest new_double_nested-delete-new_double_nested-change_double_nested results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}
                                           (:extracts results)))

(defregressiontest new_invalid_nested results
                   (test-timeline->extract {:n-extracts 0
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new_double_nested-change_invalid results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new-change-change-pand-test results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}
                                           (:extracts results)))

(defregressiontest same-double-new results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results)))

(defregressiontest new_nested-close_parent results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 1}
                                           (:extracts results)))

(defregressiontest new_or_change results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}
                                           (:extracts results)))

(defregressiontest new-change_with_array_attribute results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}
                                           (:extracts results)))
