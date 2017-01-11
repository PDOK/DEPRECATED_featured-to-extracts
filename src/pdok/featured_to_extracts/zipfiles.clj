(ns pdok.featured-to-extracts.zipfiles
  (:require  [clojure.tools.logging :as log])
  (:import [java.util.zip ZipFile ZipEntry]
           (java.io File)))

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
