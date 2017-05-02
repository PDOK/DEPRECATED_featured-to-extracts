(ns ^:regression pdok.featured-to-extracts.regression-test
  (:require [pdok.featured-to-extracts
             [core :as e]]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [pdok.featured-to-extracts.config :as config]
            [pdok.postgres :as pg]
            [clojure.java.jdbc :as j]))

(def test-db config/db)
(def schema (:schema test-db))
(def dataset "regression-set")
(def extract-type "gml")

(defn- get-or-add-extractset [tx dataset extract-type]
  (let [table (str dataset "_" extract-type)]
    (j/execute! test-db [(str "CREATE SCHEMA IF NOT EXISTS " schema)])
    (j/execute! test-db [(str "CREATE TABLE " schema ".\"" table "\" ("
                              "id BIGSERIAL PRIMARY KEY, "
                              "version UUID, "
                              "feature_type TEXT, "
                              "valid_from TIMESTAMP WITHOUT TIME ZONE, "
                              "valid_to TIMESTAMP WITHOUT TIME ZONE, "
                              "publication TIMESTAMP WITHOUT TIME ZONE, "
                              "tiles INTEGER[], "
                              "xml TEXT NOT NULL, "
                              "created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")])
    (j/execute! test-db [(str "CREATE INDEX \"" table "_version_idx\" ON " schema ".\"" table "\" "
                              "USING btree(version)")])))

(defn- query [select where]
  (let [table (str dataset "_" extract-type)]
    (when (pg/table-exists? test-db schema table)
      (j/query test-db [(str "SELECT " select " FROM " schema ".\"" table "\" WHERE " where)]))))

(defn process-feature-permutation [changelog]
  (with-bindings
    {#'e/*render-template* (constantly "test")
     #'e/*get-or-add-extractset* get-or-add-extractset
     #'e/*add-metadata-extract-records* (constantly nil)
     #'e/*initialized-collection?* (constantly true)}
    (let [result (e/update-extracts dataset [extract-type] changelog)
          _ (println result)]
      {:statistics result})))

(defn cleanup []
  (j/execute! test-db [(str "DROP TABLE IF EXISTS " schema ".\"" dataset "_" extract-type "\"")]))

(defmethod clojure.test/report :begin-test-var [m]
  (with-test-out
    (println ">>" (-> m :var meta :name))))

(defmacro defregressiontest* [name file results-var & body]
  `(deftest ~name
     (let [changelog# (io/resource (str "regression/" ~file ".changelog"))]
       (cleanup)
       (let [~results-var (process-feature-permutation changelog#)]
         ~@body
         ))))

(defmacro defregressiontest [test-name results-var & body]
  `(defregressiontest* ~test-name ~(name test-name) ~results-var ~@body))

(defn- test-timeline->extract [expected]
  (is (= (:n-extracts expected) (count (query "*" "TRUE"))))
  (is (= (:n-extracts expected) (count (query "DISTINCT version" "TRUE")))) ; extracts should have distinct versions
  (is (= (:n-valid-to expected) (count (query "*" "valid_to IS NOT NULL")))))

(defregressiontest new results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}))

(defregressiontest new-with-child results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}))

(defregressiontest new-change results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}))

(defregressiontest new-delete results
                   (test-timeline->extract {:n-extracts 0
                                            :n-valid-to 0}))

(defregressiontest new-change-change results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}))

(defregressiontest new-change-close results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 2}))

(defregressiontest new-change-close_with_attributes results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 2}))

(defregressiontest new-change-change-delete results
                   (test-timeline->extract {:n-extracts 0
                                            :n-valid-to 0}))

(defregressiontest new-change-change-delete-new-change results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}))

(defregressiontest new_with_nested_null_geom results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}))

(defregressiontest new_with_nested_crappy_geom results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}))

(defregressiontest new_nested-change_nested-change_nested results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}))

(defregressiontest new_double_nested-delete-new_double_nested-change_double_nested results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}))

(defregressiontest new_invalid_nested results
                   (test-timeline->extract {:n-extracts 0
                                            :n-valid-to 0}))

(defregressiontest new_double_nested-change_invalid results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}))

(defregressiontest new-change-change-pand-test results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}))

(defregressiontest same-double-new results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}))

(defregressiontest new_nested-close_parent results
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 1}))

(defregressiontest new_or_change results
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}))

(defregressiontest new-change_with_array_attribute results
                   (test-timeline->extract {:n-extracts 2
                                            :n-valid-to 1}))
