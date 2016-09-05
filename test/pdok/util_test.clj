(ns pdok.util-test
  (:require [clojure.test :refer :all]
            [pdok.util :refer :all]
            [clojure.java.io :as io])
  (:import (java.io File)))

(defmacro with-resource-as-file [resource file-var & body]
  `(let [~file-var (File/createTempFile "featured-to-extracts-test" ".zip")]
     (with-open [zip# (io/input-stream (io/resource ~resource))]
       (io/copy zip# ~file-var))
     ~@body
     (io/delete-file ~file-var)
     ))

(deftest empty-zip
  (with-resource-as-file "zipfiles/empty.zip" file
                         (is (= nil (first-file-from-zip file)))))

(deftest one-entry-zip
  (with-resource-as-file "zipfiles/one-file.zip" file
                         (let [entry (first-file-from-zip file)]
                           (is (boolean (re-find #"test-dataset," (slurp entry)))))))