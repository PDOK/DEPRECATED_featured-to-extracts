(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [clojure.tools.logging :as log]
            [pdok.transit :as transit]
            [pdok.featured.feature :as f])
  (:import [com.vividsolutions.jts.io WKTWriter]
           [java.util Calendar TimeZone UUID]
           [org.joda.time DateTimeZone LocalDate LocalDateTime DateTime]
           (pdok.featured GeometryAttribute NilAttribute)
           (java.sql Types PreparedStatement Date Timestamp Array)
           (clojure.lang Keyword IPersistentMap IMeta IPersistentVector PersistentVector IPersistentList IPersistentSet)
           (com.vividsolutions.jts.geom Geometry)))

(def wkt-writer (WKTWriter.))

(def utcCal (Calendar/getInstance (TimeZone/getTimeZone "UTC")))
(def nlZone (DateTimeZone/getDefault)) ;; used for reading datetimes, because postgres returns Z values

(extend-protocol j/ISQLValue
  DateTime
  (sql-value [v] (tc/to-sql-time v))
  LocalDateTime
  (sql-value [v] (tc/to-sql-time v))
  LocalDate
  (sql-value [v] (tc/to-sql-date v))
  GeometryAttribute
  (sql-value [v] (-> v f/as-jts j/sql-value))
  Geometry
  (sql-value [v] (str "SRID=" (.getSRID v) ";" (.write ^WKTWriter wkt-writer v)))
  Keyword
  (sql-value [v] (name v))
  IPersistentMap
  (sql-value [v] (transit/to-json v)))

(deftype NilType [clazz]
  j/ISQLValue
  (sql-value [_] nil)
  IMeta
  (meta [_] {:type clazz}))

(declare clj-to-pg-type)

(defn write-vector [v ^PreparedStatement s ^long i]
  (if (empty? v)
    (.setObject s i nil Types/ARRAY)
    (let [con (.getConnection s)
          pg-type (clj-to-pg-type (first v))
          postgres-array (.createArrayOf con pg-type (into-array v))]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  NilAttribute
  (set-parameter [^NilAttribute v ^PreparedStatement s ^long i]
    (j/set-parameter (NilType. (.-clazz v)) s i))
  LocalDateTime
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setTimestamp s i (j/sql-value v) utcCal))
  LocalDate
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setDate s i (j/sql-value v) utcCal))
  UUID
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  GeometryAttribute
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  Geometry
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  IPersistentMap
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/VARCHAR))
  IPersistentVector
  (set-parameter [v ^PreparedStatement s ^long i]
    (write-vector v s i))
  PersistentVector
  (set-parameter [v ^PreparedStatement s ^long i]
    (write-vector v s i))
  IPersistentList
  (set-parameter [v ^PreparedStatement s ^long i]
    (if (empty? v)
      (.setObject s i nil Types/OTHER)
      (j/set-parameter (into-array v) s i)))
  IPersistentSet
  (set-parameter [v ^PreparedStatement s ^long i]
    (j/set-parameter (into [] v) s i)))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.Long;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "integer" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.Integer;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "integer" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.util.UUID;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "uuid" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.String;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "text" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/IResultSetReadColumn
  Date
  (result-set-read-column [v _ _] (LocalDate. ^Date v ^DateTimeZone nlZone))
  Timestamp
  (result-set-read-column [v _ _] (LocalDateTime. ^Timestamp v ^DateTimeZone nlZone))
  Array
  (result-set-read-column [v _ _]
    (into [] (.getArray v))))

(def geometry-type "geometry")

(declare clj-to-pg-type)

(defn vector->pg-type [v]
  (let [e (first v)]
    (str (clj-to-pg-type e) "[]")))

(defn clj-to-pg-type [clj-value]
  (let [clj-type (type clj-value)]
    (condp = clj-type
      nil "text"
      NilAttribute (clj-to-pg-type (NilType. (.-clazz clj-value)))
      Keyword "text"
      IPersistentMap "text"
      IPersistentVector (vector->pg-type clj-value)
      PersistentVector (vector->pg-type clj-value)
      DateTime "timestamp with time zone"
      LocalDateTime "timestamp without time zone"
      LocalDate "date"
      Integer "integer"
      Double "double precision"
      Boolean "boolean"
      UUID "uuid"
      GeometryAttribute geometry-type
      "text")))

(def quoted (j/quoted \"))
