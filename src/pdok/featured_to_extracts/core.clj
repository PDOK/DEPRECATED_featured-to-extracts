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
            [clj-time [coerce :as tc] [format :as tf]]
            [pdok.featured.tiles :as tiles])
  (:gen-class)
  (:import (java.sql SQLException)
           (java.util UUID)
           (java.util.zip ZipInputStream)))

(def ^{:private true} deltaset-table
  (pg/qualified-table config/extract-schema "deltaset"))

(def ^{:private true} extractset-table
  (pg/qualified-table config/extract-schema "extractset"))

(def ^{:private true} extractset-area-table
  (pg/qualified-table config/extract-schema "extractset_area"))

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

(defn get-or-add-deltaset [tx dataset extract-type]
  "return id"
  (let [query (str "SELECT id FROM " deltaset-table " WHERE name = ? AND delta_type = ?")
        result (j/query tx [query dataset extract-type])]
    (if (empty? result)
      (do
        (j/query tx [(str "SELECT " config/extract-schema ".add_deltaset(?, ?)") dataset extract-type])
        (get-or-add-deltaset tx dataset extract-type))
      (:id (first result)))))

(defn retrieve-previous-versions [tx dataset collection extract-type versions]
  (if (seq versions)
    (let [table (str dataset "_" extract-type)
          query (str "SELECT * FROM " (pg/qualified-table config/extract-schema table) "WHERE version "
                     " IN (" (->> versions (map (constantly "?")) (str/join ", ")) ")")
          result (j/query tx (cons query versions))]
      (apply
        hash-map
        (mapcat #(list (:version %) (select-keys % [:tiles :xml])) result)))
    {}))

(def ^:dynamic *get-or-add-deltaset* get-or-add-deltaset)

(defn get-or-add-extractset [tx dataset extract-type]
  "return id"
    (let [query (str "SELECT id FROM " extractset-table " WHERE name = ? AND extract_type = ?")
        result (j/query tx [query dataset extract-type])]
    (if (empty? result)
      (do
        (j/query tx [(str "SELECT " config/extract-schema ".add_extractset(?, ?)") dataset extract-type])
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

(defn add-delta-records [tx dataset delivery-id extract-type rendered-features]
  "Inserts the xml-features and tile-set in an delta schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created. Most"
  (let [deltaset-id (*get-or-add-deltaset* tx dataset extract-type)]
    (try
      (pg/batch-insert
        tx
        (pg/qualified-table config/extract-schema (str "delta_" dataset "_" extract-type))
        [:delivery_id :feature_id :feature_type, :tiles, :xml]
        (->> rendered-features
          (map 
            #(let [feature (:feature %)]
               (vector
                 delivery-id
                 (:_id feature)
                 (:feature-type %)
                 (-> feature
                   (:_tiles)
                   (vec))
                 (:xml %))))))
      (catch SQLException e
        (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
        (throw e)))
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

(defn add-extract-records [tx dataset delivery-id extract-type rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [extractset-id (*get-or-add-extractset* tx dataset extract-type)]
    (try
      (do
        (pg/batch-insert
          tx
          (pg/qualified-table config/extract-schema (str dataset "_" extract-type))
          [:min_delivery_id :feature_id :feature_type, :version, :valid_from, :valid_to, :publication, :tiles, :xml]
          (->> rendered-features
            (map
              #(let [feature (:feature %)]
                 (vector
                   delivery-id
                   (:_id feature)
                   (:feature-type %)
                   (:_version feature)
                   (:_valid_from feature)
                   (:_valid_to feature)
                   (:LV-publicatiedatum feature)
                   (-> feature
                     (:_tiles)
                     (vec))
                   (:xml %))))))
        (*add-metadata-extract-records* tx extractset-id (map #(-> % :feature :_tiles) rendered-features)))
      (catch SQLException e
        (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
        (throw e)))
    (count rendered-features)))

(defn transform-and-add-extract [tx dataset feature-type delivery-id extract-type features]
  (let [[error features-for-extract] (features-for-extract dataset feature-type extract-type features)]
    (if (nil? error)
      (if (nil? features-for-extract)
        {}
        (let [n-inserted-records (add-extract-records tx dataset delivery-id extract-type features-for-extract)]
          (log/debug "Extract records inserted: " n-inserted-records
                     (str/join "-" (list dataset feature-type extract-type)))
         (let [ m1 (mapcat #(list (:_version (:feature %))   (:xml %)) features-for-extract)]
          (apply hash-map m1 ))))
      (do
        (log/error "Error creating extracts" error)
        nil))))

(defn- delete-extracts-with-version [tx dataset feature-type delivery-id extract-type versions]
  (let [table (str dataset "_" extract-type)]
    (try
      (doseq [versions (partition-all 100 versions)] 
        (pg/execute
          tx
          (str
            "update "
            (pg/qualified-table config/extract-schema (str dataset "_" extract-type))
            " set max_delivery_id = ? "
            "where version in ("
            (->> versions
              (map (constantly "?"))
              (str/join ", "))
            ")")
          (cons delivery-id versions)))
      (catch SQLException e
        (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
        (throw e)))
    (log/debug "Extract records deleted: " (count versions))))

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

(defn- get-or-allocate-delivery-id [tx dataset {from-date :from-date to-date :to-date}]
  (let [run (fn [{msg :msg q :q params :params}]
              (log/info msg)
              (log/debug "query:" q)
              (log/debug "params:" params)
              (let [result (-> 
                             (pg/execute-query
                               tx
                               q
                               [:delivery-id]
                               params)
                             (first)
                             (:delivery-id))]
                (log/debug "result:" result)
                result))
        fetch {:msg "Fetching delivery-id from database"
               :q (str
                    "select id from " config/extract-schema ".delivery "
                    "where dataset = ? "
                    (if from-date
                      "and from_date = ? "
                      "and from_date is null ")
                    "and to_date = ?")
               :params (filterv identity [dataset from-date to-date])}
        insert {:msg "Delivery not yet present in database -> Adding delivery info to database"
                :q (str
                     "insert into " config/extract-schema ".delivery(dataset, from_date, to_date, external_id) "
                     "values(?, ?, ?, ?) "
                     "returning id")
                :params [dataset from-date to-date (java.util.UUID/randomUUID)]}]
    (let [delivery-id (run fetch)]
      (if delivery-id
        delivery-id
        (run insert)))))

(defn- process-changes [tx dataset collection delivery-info extract-types delta-types changes]
  (let [batch-size 10000
        parts (partition-all batch-size changes)
        delta-type-names (->> delta-types (keys) (map name) (vector))
        delivery-id (get-or-allocate-delivery-id tx dataset delivery-info)]
    (log/info "Creating extracts for" dataset collection extract-types "and deltas for" delta-type-names)
    (doseq [extract-type extract-types]
      (loop [i 1
             remaining parts]
        (when-let [records (first remaining)]
          (let [added-features (remove nil? (map changelog->change-inserts records))
                deleted-features (remove nil? (map changelog->deletes records))]
            (let [current-versions-xml (transform-and-add-extract tx dataset collection delivery-id extract-type added-features)]
              (if-let [delta-type-info (-> extract-type keyword delta-types)]
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
            (delete-extracts-with-version tx dataset collection delivery-id extract-type deleted-features)
            (when (zero? (mod i 10))
              (log/info "Creating extracts, processed:" (* i batch-size))))
          (recur (inc i) (rest remaining)))))
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

(defn- all-values [x]
  (cond
    (map? x) (->> x 
               (vals)
               (all-values))
    (coll? x) (->> x 
                (map all-values)
                (flatten))
    :else x))

(defn- calculate-tiles [feature]
  (apply clojure.set/union
         (->> feature
           (all-values)
           (filter #(instance? pdok.featured.GeometryAttribute %))
           (map tiles/nl))))

(defn update-extracts [dataset extract-types delta-types changelog-stream]
  (let [[version meta-info changes] (parse-changelog changelog-stream)
        delivery-info (:delivery-info meta-info)
        collection (-> meta-info :collection .toLowerCase)]
    (if-not (or 
              (= version "pdok-featured-changelog-v2")
              (= version "pdok-featured-changelog-v3"))
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
                (process-changes
                  tx
                  dataset
                  collection
                  delivery-info
                  extract-types
                  delta-types
                  (if (= version "pdok-featured-changelog-v3")
                    (map #(assoc % :_tiles (calculate-tiles %)) changes)
                    changes))
                (pg/commit-transaction tx)))
            {:status "ok" :collection collection}))))))

(defn- do-update-extracts [template-location dataset extract-type delta-feature-root-tag changelog-file]
  (let [templates-with-metadata (template/templates-with-metadata dataset template-location)]
    (if-not (some false? (map template/add-or-update-template templates-with-metadata))
      (println (update-extracts
                 dataset
                 [extract-type]
                 (if delta-feature-root-tag
                   {(keyword extract-type) {:featureRootTag delta-feature-root-tag}}
                   {})
                 (doto 
                   (ZipInputStream. (io/input-stream changelog-file))
                   (.getNextEntry)))))
      (println "could not load template(s)")))

(defn -main [& args]
  (condp = (count args)
    4 (let [[template-location dataset extract-type changelog-file] args]
        (do-update-extracts template-location dataset extract-type {} changelog-file))
    5 (apply do-update-extracts args)
    (println "No arguments provided. Expected: template-location dataset extract-type [delta-feature-root-tag] changelog-file")))