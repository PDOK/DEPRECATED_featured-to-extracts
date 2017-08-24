(ns ^:delta pdok.featured-to-extracts.delta-test
  (:require [pdok.featured-to-extracts
             [config :as config]
             [core :as core]]
            [clojure.java.jdbc :as j]
            [clojure.java.io :as io]
            [pdok.featured-to-extracts.mustache :as m]
            [clojure.test :refer :all]
            [pdok.featured-to-extracts.template :as template])
  (:import [java.io ByteArrayInputStream])
  )

  (def test-db config/test-db)
  (def schema (:schema test-db))
  (def dataset "bgtv3")
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

(def delta-gml-pand-template (slurp (io/resource "templates/delta/delta-gml-pand.mustache")))
(def bgtv3-gml-pand-template (slurp (io/resource "templates/delta/bgtv3-gml-pand.mustache")))


(defn run [file]
    (clean-db)
  (let [_ (template/add-or-update-template {:dataset      "bgtv3_delta"
                                            :extract-type "gml"
                                            :name         "pand"
                                            :template     delta-gml-pand-template})
        _ (template/add-or-update-template {:dataset      "bgtv3"
                                            :extract-type "gml"
                                            :name         "pand"
                                            :template     bgtv3-gml-pand-template})

        ]

    (try
      (with-bindings
        {
         #'core/*get-or-add-extractset* get-or-add-extractset
         #'core/*add-metadata-extract-records* (constantly nil)
         #'core/*initialized-collection?* (constantly true)}
        (with-open [in (io/input-stream (.getFile (clojure.java.io/resource file)))]
          (let [result (core/update-extracts dataset '("gml") in)]
            (println result)
            ))))) )



(comment
  (deftest new-and_deleted-test
    (doseq [i (range 1 10)]
      (println "Run#: " i)
      (time (run "./resources/performance/1501164670044-performance-set-col1.changelog")))
    )
)

(comment
  (deftest delta-new
    (run "./resources/delta/1502975188495-bgt-Pand.changelog")
    )
  )

(deftest delta-new-delelete
  (run "./resources/delta/bgt_pand_new_delete_G0353_cf44d351d9da42c5b6e73bfaf09f5b68.changelog")
  )


(def delta-gml-start-partial (slurp (io/resource "templates/delta/partials/start.mustache")))
(def delta-gml-end-partial   (slurp (io/resource "templates/delta/partials/end.mustache")))

(defn- test-feature [id version]
  {"_id" id
   "_version" version
   })

(defn two-features [] (list (test-feature "ID101" "Version1")
                            (test-feature "ID201" "Version2")))


(comment

  (deftest test-two-rendered-features
    (let [_ (template/add-or-update-template {:dataset      "delta"
                                              :extract-type "gml"
                                              :name         "pand"
                                              :template     delta-gml-pand-template})

          [error features] (core/features-for-extract "delta" "pand" "gml" (two-features))
          rendered-feature (nth (first features) 1)]
      (is (= 2 (count features)))))

  )



