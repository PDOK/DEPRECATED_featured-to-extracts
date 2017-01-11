(ns pdok.featured-to-extracts.config
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            clojure.stacktrace
            [environ.core :as environ])
  (:import (java.io Reader)
           (java.util Properties)))

(Thread/setDefaultUncaughtExceptionHandler
  (reify Thread$UncaughtExceptionHandler
    (uncaughtException [_ thread throwable]
      (log/error throwable "Stacktrace:"
                 (print-str (clojure.stacktrace/print-stack-trace throwable))))))

(defn- keywordize [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "." "-")
      (keyword)))

(defn load-props [resource-file]
  (with-open [^Reader reader (io/reader (io/resource resource-file))]
    (let [props (Properties.)
          _ (.load props reader)]
      (into {} (for [[k v] props
                     ;; no mustaches, for local use
                     :when (not (re-find #"^\{\{.*\}\}$" v))]
                 [(keywordize k) v])))))

(defonce env
         (merge environ/env
                (load-props "plp.properties")))

(def db {:subprotocol "postgresql"
                  :subname (or (env :database-url) "//localhost:5432/pdok")
                  :user (or (env :database-user) "postgres")
                  :password (or (env :database-password) "postgres")
                  :transaction? true
                  :schema (or (env :extracts-schema) "extractmanagement")})

(defn create-workers [factory-f]
  (let [n-workers (read-string (or (env :n-workers) "2"))]
    (dorun (for [i (range 0 n-workers)]
             (factory-f i)))))