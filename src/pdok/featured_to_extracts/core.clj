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

(def ^{:private true} delta-schema
  (pg/quoted (:schema config/dbdelta)))

(defn- qualified-table [table]
  (str extract-schema "." (pg/quoted table)))

(defn- qualified-delta-table [table]
  (str delta-schema "." (pg/quoted table)))

(def ^{:private true} deltaset-table
  (qualified-delta-table "extractset"))



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
      [nil (map #(hash-map :feature-type feature-type :feature % :xml (*render-template* template-key %)) features)])))


(defn features-for-delta [dataset feature-type extract-type delta-type-info delta-records]
  "Returns the rendered representation of the collection of features for a given feature-type inclusive tiles-set"
  (if (empty? delta-records)
    [nil nil]
    [nil (map #(hash-map :feature-type feature-type
                         :feature (-> % (dissoc :previous) (dissoc :current))
                         :xml (*render-template* "_delta" (merge delta-type-info %))) delta-records)]))

(defn- jdbc-insert-delta [tx table entries]
  (when (seq entries)
    (try
      (pg/batch-insert tx (qualified-delta-table table)
                       [:_delivery_id :feature_type, :version, :valid_from, :valid_to, :publication, :tiles, :xml] entries)
      (catch SQLException e
        (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
        (throw e)))))

(defn get-or-add-deltaset [tx dataset extract-type]
  "return id"
  (let [query (str "SELECT id FROM " deltaset-table " WHERE name = ? AND extract_type = ?")
        result (j/query tx [query dataset extract-type])]
    (if (empty? result)
      (do
        (j/query tx [(str "SELECT " delta-schema ".add_extractset(?, ?)") dataset extract-type])
        (get-or-add-deltaset tx dataset extract-type))
      (:id (first result)))))

(defn retrieve-previous-versions [tx dataset collection extract-type versions]
  (if (seq versions)
    (let [table (str dataset "_" extract-type)
          query (str "SELECT * FROM " (qualified-table table) "WHERE version "
                     " IN (" (->> versions (map (constantly "?")) (str/join ", ")) ")")
          result (j/query tx (cons query versions))]
      (apply
        hash-map
        (mapcat #(list (:version %) (select-keys % [:tiles :xml])) result)))
    {}))

(def ^:dynamic *get-or-add-deltaset* get-or-add-deltaset)

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

(defn add-metadata-extract-records [tx extractset-id tiles]
  (let [tiles (reduce clojure.set/union tiles)]
    (add-extractset-area tx extractset-id tiles)))

(def ^:dynamic *add-metadata-extract-records* add-metadata-extract-records)

(defn map-to-columns [features]
  (map
    #(vector
       (:feature-type %)
       (:_version (:feature %))
       (:_valid_from (:feature %))
       (:_valid_to (:feature %))
       (:LV-publicatiedatum (:feature %))
       (vec (:_tiles (:feature %)))
       (:xml %))
    features))

(defn add-delta-records [tx dataset delivery-id extract-type rendered-features]
  "Inserts the xml-features and tile-set in an delta schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created. Most"
  (let [deltaset-id (*get-or-add-deltaset* tx dataset extract-type)]
    (do
      (jdbc-insert-delta
        tx
        (str dataset "_" extract-type)
        (map
          (partial cons delivery-id)
          (map-to-columns rendered-features)))
      (*add-metadata-extract-records* tx deltaset-id (map #(:_tiles (:feature %)) rendered-features)))
    (count rendered-features)))

(defn transform-and-add-delta [tx dataset collection delivery-id extract-type delta-type-info delta-records]
  (let [[error features-for-delta] (features-for-delta dataset collection extract-type delta-type-info delta-records)]
    (if (nil? error)
      (if (nil? features-for-delta)
        {}
        (let [n-inserted-records (add-delta-records tx dataset delivery-id extract-type features-for-delta)]
          (log/debug "Delta records inserted: " n-inserted-records
                     (str/join "-" (list dataset collection extract-type)))
          features-for-delta))
      (do
        (log/error "Error creating extracts" error)
        nil))))

(defn add-extract-records [tx dataset extract-type rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [extractset-id (*get-or-add-extractset* tx dataset extract-type)]
    (do
      (jdbc-insert-extract tx (str dataset "_" extract-type) (map-to-columns rendered-features ))
      (*add-metadata-extract-records* tx extractset-id (map #(:_tiles (:feature %)) rendered-features)))
    (count rendered-features)))

(defn transform-and-add-extract [tx dataset feature-type extract-type features]
  (let [[error features-for-extract] (features-for-extract dataset feature-type extract-type features)]
    (if (nil? error)
      (if (nil? features-for-extract)
        {}
        (let [n-inserted-records (add-extract-records tx dataset extract-type features-for-extract)]
          (log/debug "Extract records inserted: " n-inserted-records
                     (str/join "-" (list dataset feature-type extract-type)))
         (let [ m1 (mapcat #(list (:_version (:feature %))   (:xml %)) features-for-extract)]
          (apply hash-map m1 ))))
      (do
        (log/error "Error creating extracts" error)
        nil))))

(defn- delete-version-with-valid-from-sql [table]
  (str "DELETE FROM " (qualified-table table)
       " WHERE version = ? AND valid_from = ? AND id IN (SELECT id FROM " (qualified-table table)
       " WHERE version = ? AND valid_from = ? ORDER BY id ASC LIMIT 1)"))



(defn delete-by-version-sql [table version-count]
  (str "DELETE FROM " (qualified-table table)
                   " WHERE VERSION IN ("
                   (clojure.string/join "," (repeat version-count "?" ))  ") "))

(defn- jdbc-delete-versions-new-style [tx table versions]
  (try
    (pg/execute tx (delete-by-version-sql table (count versions)) versions)
    (log/debug (delete-by-version-sql table (count versions)))
     (catch SQLException e
    (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
    (throw e))))

(defn- jdbc-delete-versions-old-style [tx table versions]
  "([version valid_from] ... )"
    (when (not= nil versions)
      (try
        (pg/batch-delete tx (qualified-table table) [:version] versions)
        (catch SQLException e
          (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
          (throw e))))
)

(defn- delete-extracts-with-version [db dataset feature-type extract-type versions unique-versions ]
  (let [table (str dataset "_" extract-type)]
    (if unique-versions
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
    "delete" (:_previous_version record)
    "change" (:_previous_version record)
    "close" (:_previous_version record)
    nil))

(def ^:dynamic *initialized-collection?* m/registered?)

(defn- process-changes [tx dataset collection delivery-info extract-types delta-types changes unique-versions]
  (let [batch-size 10000
        parts (partition-all batch-size changes)
        delta-type-names (->> delta-types (keys) (map name) (vector))]
    (log/info "Creating extracts for" dataset collection extract-types 
              "and deltas for" delta-type-names)
    (doseq [extract-type extract-types]
      (loop [i 1
             remaining parts]
        (let [records (first remaining)]
          (when records
            (let [added-features (remove nil? (map changelog->change-inserts records))
                  deleted-features (remove nil? (map changelog->deletes records))]
              (let [current-versions-xml (transform-and-add-extract tx dataset collection extract-type added-features)]
                (if-let [delta-type-info (-> extract-type keyword delta-types)]
                  (let [delivery-id (:id delivery-info)]
                    (if (first (pg/execute-query tx "select from deltamanagement._delivery where id = ?" [] [delivery-id]))
                      (log/info "Delta delivery info already present in db")
                      (do
                        (log/info "Inserting new delta delivery info:" delivery-id)
                        (pg/execute
                          tx
                          "insert into deltamanagement._delivery(id, file, from_date, to_date) values(?, ?, ?, ?)"
                          (cons delivery-id (mapv delivery-info [:file :from-date :to-date])))))
                    (let [previous-versions (retrieve-previous-versions tx dataset collection extract-type deleted-features)
                          delta-records (->> records
                                          (map
                                            #(let [previous-version (->> % :_previous_version (get previous-versions))
                                                   current-version-xml (->> % :_version (get current-versions-xml))]
                                               (merge
                                                 %
                                                 {:_tiles (clojure.set/union
                                                            (:_tiles %)
                                                            (:tiles previous-version))
                                                  :previous (:xml previous-version)
                                                  :current current-version-xml}))))]
                      (transform-and-add-delta tx dataset collection delivery-id extract-type delta-type-info delta-records))))
                (delete-extracts-with-version tx dataset collection extract-type deleted-features unique-versions)
                (if (zero? (mod i 10))
                  (log/info "Creating extracts, processed:" (* i batch-size)))))
            (recur (inc i) (rest remaining))))))
    (log/info "Finished" dataset collection extract-types delta-type-names)))

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
        meta-info (t/from-json (second lines))
        ; drop header
        lines (drop 2 lines)]
    [version meta-info (map make-change-record lines)]))

(defn update-extracts [dataset extract-types delta-types changelog-stream unique-versions]
  (let [[version meta-info changes] (parse-changelog changelog-stream)
        delivery-info (:delivery-info meta-info)
        collection (-> meta-info :collection .toLowerCase)]
    (if-not (= version "pdok-featured-changelog-v2")
      {:status "error" :msg (str "unknown changelog version" version)}
      (let [missing-templates (->> extract-types 
                                (map #(template/template-key dataset % collection))
                                (filter #(not (*initialized-collection?* %))))]
        (if (next missing-templates)
          (let [error-msg {:status "error" 
                           :msg "missing template(s)" 
                           :collection collection 
                           :extract-types extract-types 
                           :missing-templates missing-templates}]
            (log/error "Not all templates are found. Details: " error-msg)
            error-msg)
          (do
            (log/info "All templates found")
            (when (seq? changes)
              (let [tx (pg/begin-transaction config/db)]
                (process-changes tx dataset collection delivery-info extract-types delta-types changes unique-versions)
                (pg/commit-transaction tx)))
            {:status "ok" :collection collection}))))))

(defn -main [template-location dataset extract-type & args]
  (let [templates-with-metadata (template/templates-with-metadata dataset template-location)]
    (if-not (some false? (map template/add-or-update-template templates-with-metadata))
      (let [changelog-file (first args)]
        (println (update-extracts dataset [extract-type] changelog-file false)))
      (println "could not load template(s)"))))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")]
;  (time (last (features-from-package-stream s))))
