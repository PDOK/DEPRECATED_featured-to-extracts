(ns pdok.featured-to-extracts.core
  (:require [clojure.java.jdbc :as j]
            [pdok.transit :as t]
            [pdok.featured-to-extracts.config :as config]
            [pdok.featured-to-extracts.mustache :as m]
            [pdok.featured-to-extracts.template :as template]
            [pdok.postgres :as pg]
            [clojure.tools.logging :as log]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-time [coerce :as tc] [format :as tf]])
  (:gen-class)
  (:import (java.sql SQLException)
           (java.util UUID)))

(def ^{:private true} extract-schema
  (pg/quoted (:schema config/db)))

(defn- qualified-table [table]
  (str extract-schema "." (pg/quoted table)))

(def ^{:private true} extractset-table
  (qualified-table "extractset"))

(def ^{:private true} extractset-area-table
  (qualified-table "extractset_area"))

(def ^:dynamic *render-template* m/render)

(defn features-for-extract [dataset feature-type extract-type features]
  "Returns the rendered representation of the collection of features for a given feature-type inclusive tiles-set"
  (if (empty? features)
    [nil nil]
    (let [template-key (template/template-key dataset extract-type feature-type)]
      [nil (map #(vector feature-type (:_version %) (:_tiles %) (*render-template* template-key %)
                         (:_valid_from %) (:_valid_to %) (:lv-publicatiedatum %)) features)])))

(defn- jdbc-insert-extract [tx table entries]
  (when (seq entries)
    (try
      (pg/batch-insert tx (qualified-table table)
                       [:feature_type, :version, :valid_from, :valid_to, :publication, :tiles, :xml] entries)
      (catch SQLException e
        (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
        (throw e)))))

(defn get-or-add-extractset [tx dataset extract-type]
  "return id"
  (let [query (str "SELECT id FROM " extractset-table " WHERE name = ? AND extract_type = ?")
        result (j/query tx [query dataset extract-type])]
    (if (empty? result)
      (do
        (j/query tx [(str "SELECT " extract-schema ".add_extractset(?, ?)") dataset extract-type])
        (get-or-add-extractset tx dataset extract-type))
      (:id (first result)))))

(def ^:dynamic *get-or-add-extractset* get-or-add-extractset)

(defn add-extractset-area [tx extractset-id tiles]
  (let [query (str "SELECT * FROM " extractset-area-table " WHERE extractset_id = ? AND area_id = ?")]
    (doseq [area-id tiles]
      (if (empty? (j/query tx [query extractset-id area-id]))
        (pg/insert tx extractset-area-table [:extractset_id :area_id] [extractset-id area-id])))))

(defn- tiles-from-feature [[type version tiles & more]]
  tiles)

(defn add-metadata-extract-records [tx extractset-id rendered-features]
  (let [tiles (reduce clojure.set/union (map tiles-from-feature rendered-features))]
    (add-extractset-area tx extractset-id tiles)))

(def ^:dynamic *add-metadata-extract-records* add-metadata-extract-records)

(defn- tranform-feature-for-db [[feature-type version tiles xml-feature valid-from valid-to publication-date]]
  [feature-type version valid-from valid-to publication-date (vec tiles) xml-feature])

(defn add-extract-records [tx dataset extract-type rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [extractset-id (*get-or-add-extractset* tx dataset extract-type)]
    (do
      (jdbc-insert-extract tx (str dataset "_" extract-type) (map tranform-feature-for-db rendered-features))
      (*add-metadata-extract-records* tx extractset-id rendered-features))
    (count rendered-features)))

(defn transform-and-add-extract [tx dataset feature-type extract-type features]
  (let [[error features-for-extract] (features-for-extract dataset feature-type extract-type features)]
    (if (nil? error)
      (if (nil? features-for-extract)
        {:status "ok" :count 0}
        (let [n-inserted-records (add-extract-records tx dataset extract-type features-for-extract)]
          (log/debug "Extract records inserted: " n-inserted-records
                     (str/join "-" (list dataset feature-type extract-type)))
          {:status "ok" :count n-inserted-records}))
      (do
        (log/error "Error creating extracts" error)
        {:status "error" :msg error :count 0}))))

(defn- delete-version-with-valid-from-sql [table]
  (str "DELETE FROM " (qualified-table table)
       " WHERE version = ? AND valid_from = ? AND id IN (SELECT id FROM " (qualified-table table)
       " WHERE version = ? AND valid_from = ? ORDER BY id ASC LIMIT 1)"))



(defn- delete-by-version-sql [table versions]
  (let [query (str "DELETE FROM " (qualified-table table)
                   " WHERE VERSION IN ("
                   (clojure.string/join "," (repeat (count versions) "?" ))  ") ")]))

(defn- jdbc-delete-versions-new-style [tx table versions]
  (try
    (pg/execute-query tx (delete-by-version-sql table versions)  versions)
    (catch SQLException e
      (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
      (throw e))))


(defn- jdbc-delete-versions-old-style [tx table versions]
  "([version valid_from] ... )"
  (let [versions-only (map #(take 1 %) (filter (fn [[_ valid-from]] (not valid-from)) versions))
        with-valid-from (map (fn [[ov vf]] [ov vf ov vf]) (filter (fn [[_ valid-from]] valid-from) versions))]
    (when (seq versions-only)
      (try
        (pg/batch-delete tx (qualified-table table) [:version] versions-only)
        (catch SQLException e
          (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
          (throw e))))
    (when (seq with-valid-from)
      (try
        (pg/execute-batch-query tx (delete-version-with-valid-from-sql table) with-valid-from)
        (catch SQLException e
          (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
          (throw e))))))

(defn- delete-extracts-with-version [db dataset feature-type extract-type versioned-deletes versions]
  (let [table (str dataset "_" extract-type)]
    (if (= versioned-deletes "true" )
      (jdbc-delete-versions-new-style db table versions)
      (jdbc-delete-versions-old-style db table versions))))

(defn changelog->change-inserts [record]
  (condp = (:_action record)
    "new" record
    "change" record
    "close" record
    nil))

(defn changelog->deletes [record]
  (condp = (:_action record)
    "delete" [(:_previous_version record)]
    "change" [(:_previous_version record) (:_valid_from record)]
    "close" [(:_previous_version record) (:_valid_from record)]
    nil))

(def ^:dynamic *initialized-collection?* m/registered?)

(defn- process-changes [tx dataset collection extract-types changes versioned-deletes]
  (let [batch-size 10000
        parts (partition-all batch-size changes)]
    (log/info "Creating extracts for" dataset collection extract-types)
    (doseq [extract-type extract-types]
      (loop [i 1
             remaining parts]
        (let [records (first remaining)]
          (when records
            (transform-and-add-extract tx dataset collection extract-type
                                       (filter (complement nil?) (map changelog->change-inserts records)))
            (delete-extracts-with-version tx dataset collection extract-type versioned-deletes
                                          (filter (complement nil?) (map changelog->deletes records)))
            (if (= 0 (mod i 10))
              (log/info "Creating extracts, processed:" (* i batch-size)))
            (recur (inc i) (rest remaining))))))
    (log/info "Finished " dataset collection extract-types)))

(def date-time-formatter (tf/formatters :date-time-parser))
(defn parse-time
  "Parses an ISO8601 date timestring to local date time"
  [datetimestring]
  (when-not (clojure.string/blank? datetimestring)
    (tc/to-local-date-time (tf/parse date-time-formatter datetimestring))))

(defn make-change-record [transit-line]
  (let [feature (t/from-json transit-line)]
    (merge (:attributes feature)
           {:_action (:action feature)
            :_collection (.toLowerCase (:collection feature))
            :_id (:id feature)
            :_previous_version (:previous-version feature)
            :_version (:version feature)
            :_tiles (:tiles feature)
            :_valid_from (:valid-from feature)
            :_valid_to (:valid-to feature)})))

(defn parse-changelog [in-stream]
  "Returns [dataset collection change-record], Where every line is a map with
  keys: feature-id,action,version,tiles,valid-from,old-version,old-tiles,old-valid-from,feature"
  (let [lines (line-seq (io/reader in-stream))
        version (first lines)
        collection (.toLowerCase (:collection (t/from-json (second lines))))
        ;drop collection info + header
        lines (drop 2 lines)]
    [version collection (map make-change-record lines)]))

(defn update-extracts [dataset extract-types changelog-stream versioned-deletes]
  (let [[version collection changes] (parse-changelog changelog-stream)]
    (if-not (= version "pdok-featured-changelog-v2")
      {:status "error" :msg (str "unknown changelog version" version)}
      (if-not (every? *initialized-collection?* (map #(template/template-key dataset % collection) extract-types))
        {:status "error" :msg "missing template(s)" :collection collection :extract-types extract-types}
        (do
          (when (seq? changes)
            (let [tx (pg/begin-transaction config/db)]
              (process-changes tx dataset collection extract-types changes versioned-deletes)
              (pg/commit-transaction tx)))
          {:status "ok" :collection collection})))))

(defn -main [template-location dataset extract-type & args]
  (let [templates-with-metadata (template/templates-with-metadata dataset template-location)]
    (if-not (some false? (map template/add-or-update-template templates-with-metadata))
      (let [changelog-file (first args)]
        (println (update-extracts dataset [extract-type] changelog-file)))
      (println "could not load template(s)"))))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")]
;  (time (last (features-from-package-stream s))))
