(def version (slurp "VERSION"))
(def artifact-name (str "featured-" version))
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
                 [org.clojure/core.async "0.2.385"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.3"
                  :exclusions [[log4j]]]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.postgresql/postgresql "9.4.1209.jre7"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.trace "0.7.9"]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [stencil "0.4.0"]
                 [environ "1.0.1"]
                 [cheshire "5.5.0"]
                 [clj-time "0.11.0"]
                 [com.cognitect/transit-clj "0.8.288"]
                 [prismatic/schema "0.4.3"]
                 [compojure "1.5.1" :exclusions [[org.clojure/tools.reader]]]
                 [ring/ring-defaults "0.1.5"]
                 [ring/ring-json "0.3.1"]
                 [http-kit "2.2.0"]
                 [nl.pdok/featured-shared "1.0-rc3"]]
  :plugins [[lein-ring "0.9.6" ]
            [lein-filegen "0.1.0-SNAPSHOT"]]
  :ring {:port 5000
         :handler pdok.featured-to-extracts.api/app
         :init pdok.featured-to-extracts.api/init!
         :uberwar-name ~uberwar-name}
  :main ^:skip-aot pdok.featured-to-extracts.cli
  :resource-paths ["config"]
  :target-path "target/%s"
  :filegen [{:data ~(str version "(" git-ref ")")
             :template-fn #(str %1)
             :target "resources/version"}]
  :profiles {:uberjar {:aot :all}
             :cli {:uberjar-name ~uberjar-name
                   :aliases {"build" ["do" "uberjar"]}}
             :web-war {:aliases {"build" ["do" "filegen" ["ring" "uberwar"]]}}
             :web-jar {:uberjar-name ~webjar-name
                       :aliases {"build" ["do" "filegen" ["ring" "uberjar"]]}}
             :dev {:resource-paths ["test/resources"]}})
