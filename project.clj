(def version (slurp "VERSION"))
(def artifact-name (str "featured-to-extracts-" version))
(def uberjar-name (str artifact-name "-standalone.jar"))
(def webjar-name (str artifact-name "-web.jar"))
(def uberwar-name (str artifact-name ".war"))
(def git-ref (clojure.string/replace (:out (clojure.java.shell/sh "git" "rev-parse" "HEAD"))#"\n" "" ))

(defproject featured-to-extracts version
  :min-lein-version "2.5.4"
  :uberjar-name ~uberjar-name
  :manifest {"Implementation-Version" ~(str version "(" git-ref ")")}
  :description "PDOK - csv based extract generation"
  :url "http://github.com/PDOK/featured-to-extracts"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.2.395"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.4"
                  :exclusions [[log4j]]]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.postgresql/postgresql "9.4.1212.jre7"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.trace "0.7.9"]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.22"]
                 [org.slf4j/slf4j-log4j12 "1.7.22"]
                 [stencil "0.5.0"]
                 [environ "1.1.0"]
                 [cheshire "5.6.3"]
                 [clj-time "0.13.0"]
                 [com.cognitect/transit-clj "0.8.297"]
                 [prismatic/schema "1.1.3"]
                 [compojure "1.5.2" :exclusions [[org.clojure/tools.reader]]]
                 [ring/ring-defaults "0.2.2"]
                 [ring/ring-json "0.4.0"]
                 [http-kit "2.2.0"]
                 [nl.pdok/featured-shared "1.1.3"]]
  :plugins [[lein-ring "0.10.0" ]
            [lein-filegen "0.1.0-SNAPSHOT"]]
  :ring {:port 5000
         :handler pdok.featured-to-extracts.api/app
         :init pdok.featured-to-extracts.api/init!
         :uberwar-name ~uberwar-name}
  :main ^:skip-aot pdok.featured-to-extracts.cli
  :resource-paths ["config" "resources"]
  :test-selectors {:default (fn [m] (not (:delta m)))
                   :delta :delta}
  :filegen [{:data ~(str version "(" git-ref ")")
             :template-fn #(str %1)
             :target "resources/version"}]
  :profiles {:uberjar {:aot :all}
             :cli {:uberjar-name ~uberjar-name
                   :aliases {"build" ["do" "uberjar"]}}
             :web-war {:aliases {"build" ["do" "filegen" ["ring" "uberwar"]]}}
             :web-jar {:uberjar-name ~webjar-name
                       :aliases {"build" ["do" "filegen" ["ring" "uberjar"]]}}
             :test {:resource-paths ["test/resources"]}
             :dev {:resource-paths ["test/resources"]}})
