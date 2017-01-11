(ns pdok.featured-to-extracts.template
  (:require [pdok.featured-to-extracts.mustache :as m]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log])
  (:import (java.io File)))


(defn- template-qualifier [dataset extract-type]
  (str dataset "-" extract-type "-" ))

(defn template-key [dataset extract-type name]
  (str (template-qualifier dataset extract-type) name))

(defn- template-with-metadata* [dataset ^File file]
  (let [file-name (.getName file)]
    {:dataset dataset
     :extract-type (cond-> file
                           (-> file .getParentFile .getName (= "partials")) .getParentFile
                           true .getParentFile
                           true .getName)
     :name (subs file-name 0 (.lastIndexOf file-name "."))
     :template (slurp (.getPath file))}))

(defn templates-with-metadata [dataset template-location]
  "Returns the template-contents with metadata included.
   The metadata is based on dataset and where template-file in directory structure is located."
  (let [template-files (filter (fn [^File f] (.isFile f)) (file-seq (io/file template-location)))]
    (map (partial template-with-metadata* dataset) template-files)))

(defn- normalize [source]
  (s/trim (reduce str (s/split-lines (s/trim-newline (s/replace source #"\t" ""))))))

(defn prefix-partials [template qualifier]
  (clojure.string/replace template "{{>" (str "{{>" qualifier)))

(defn add-or-update-template [{:keys [dataset extract-type name template]}]
  (let [template (normalize template)
        template (prefix-partials template (template-qualifier dataset extract-type))
        template-key (template-key dataset extract-type name)]
    (try
      (m/register template-key template)
      (m/render template-key {}) ;check template can be rendered with empty map
      {:template template-key :status "registered"}
      (catch Exception e
        (log/error "Template: " template-key " cannot be registered")
        (log/error e)
        false))))

