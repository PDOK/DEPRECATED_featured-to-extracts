(ns pdok.util
  (:import [com.fasterxml.uuid Generators]
           [com.fasterxml.uuid.impl TimeBasedGenerator]
           (java.io File)
           (java.util.zip ZipFile ZipEntry)))

(def ^:private UUIDGenerator (Generators/timeBasedGenerator))

(defn ordered-UUID [] (locking UUIDGenerator (.generate ^TimeBasedGenerator UUIDGenerator)))

(defn first-file-from-zip [^File zipped-file]
  (with-open [zip-file (ZipFile. zipped-file)]
    (let [zip-entries (filter (fn [^ZipEntry e] (not (.isDirectory e))) (enumeration-seq (.entries zip-file)))
          first-entry (first zip-entries)]
      (when first-entry
        (let [tmp (File/createTempFile "featured" ".json")
              in (.getInputStream zip-file first-entry)]
          (clojure.java.io/copy in tmp)
          (.close zip-file)
          tmp)))))