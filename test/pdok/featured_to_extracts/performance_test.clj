(ns ^:performance pdok.featured-to-extracts.performance-test
  (:require [pdok.featured-to-extracts
             [config :as config]
             [core :as core]]
            [clojure.java.jdbc :as j]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import [java.io ByteArrayInputStream])
  )

  (def test-db config/test-db)
  (def schema (:schema test-db))
  (def dataset "performance-set")
  (def extract-type "gml")


  (defn clean-db []
        (j/execute! test-db [(str "DROP TABLE  IF EXISTS " schema ".\"" dataset "_" extract-type "\" CASCADE")]))

  (defn vacuum-tables []
        (j/execute! test-db ["VACUUM ANALYZE \"featured_performance_performance-set\".feature"])
        (j/execute! test-db ["VACUUM ANALYZE \"featured_performance_performance-set\".feature_stream"])
        (j/execute! test-db ["VACUUM ANALYZE \"featured_performance_performance-set\".timeline_col1"]))


(defn- get-or-add-extractset [tx dataset extract-type]
  (let [table (str dataset "_" "gml")]
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



(defn run [file]
    (clean-db)
    (try
      (with-bindings
        {#'core/*render-template* (constantly "test")
         #'core/*get-or-add-extractset* get-or-add-extractset
         #'core/*add-metadata-extract-records* (constantly nil)
         #'core/*initialized-collection?* (constantly true)}
        (with-open [in (io/input-stream (.getFile (clojure.java.io/resource file)))]
          (let [result (core/update-extracts dataset '("gml") in)]
            (println result)
            )))) )



(comment
  (deftest only-new-test
    (run "./resources/performance/1501164506557-performance-set-col1.changelog"))
)

(deftest new-and_deleted-test
  (doseq [i (range 1 10)]
    (println "Run#: " i)
    (time (run "./resources/performance/1501164670044-performance-set-col1.changelog")))
  )






