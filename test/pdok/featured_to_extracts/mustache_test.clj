(ns pdok.featured-to-extracts.mustache-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [pdok.featured-to-extracts.mustache :as m]))


(deftest test-resolve-as-function
  (is (= nil (m/resolve-as-function "clojure.string" "no-function")))
  (is (= #'clojure.string/split (m/resolve-as-function "clojure.string" "split"))))

(def ^{:private true} inverted-template "templates/gml2extract/inverted.mustache")

(deftest test-inverted-with-value
  (let [render-result (m/render-resource inverted-template
                                         {"lokaalid" "123456789" "inonderzoek" "vandaag"})]
    (is (boolean (re-find #"<inonderzoekA>vandaag<inonderzoekA>" render-result)))
    (is (= nil (re-find #"inonderzoekB" render-result)))))

(deftest test-inverted-with-false
  (let [render-result (m/render-resource inverted-template
                                         {"lokaalid" "123456789" "inonderzoek" false})]
    (is (boolean (re-find #"<inonderzoekA>false<inonderzoekA>" render-result)))
    (is (= nil (re-find #"inonderzoekB" render-result)))))

(deftest test-inverted-without-value
  (let [render-result (m/render-resource inverted-template
                                         {"lokaalid" "123456789"})]
    (is (boolean (re-find #"<inonderzoekB>no-fill<inonderzoekB>" render-result)))
    (is (= nil (re-find #"inonderzoekA" render-result)))))

(deftest test-inverted-without-value
  (let [render-result (m/render-resource inverted-template
                                         {"lokaalid" "123456789" "inonderzoek" nil})]
    (is (boolean (re-find #"<inonderzoekB>no-fill<inonderzoekB>" render-result)))
    (is (= nil (re-find #"inonderzoekA" render-result)))))

(def example-gml-bgt-wegdeel
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?><gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\" count=\"5\">172307.599 509279.740 172307.349 509280.920 172306.379 509280.670 172306.699 509279.490 172307.599 509279.740</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"
  )

(def example-geometry-bgt-wegdeel {"gml" example-gml-bgt-wegdeel, "type" "gml"})

(def example-attributes-bgt-wegdeel
  { "_traffic-area-gml-id" "abcdef"
   "_creation-data" "2014-12-05"
   "LV-publicatiedatum" "2015-02-05T17:22:59.000"
   "tijdstipRegistratie" "2014-12-05T15:32:38.000"
   "inOnderzoek" false
   "relatieveHoogteligging" 0
   :bronhouder "G4"
   "bronhouder_leeg" "BGT"
   "lokaalID" "G0303.0979f33001fd319ae05332a1e90a5e0b"
   :kruinlijn {:geo example-geometry-bgt-wegdeel}})

(def example-feature-bgt-wegdeel
  (merge
    {"_action" "new"}
    {:geometry example-geometry-bgt-wegdeel}
    example-attributes-bgt-wegdeel
    {:label ["een" "twee" "drie"]})
  )

(defn example-bgt-features [n] (repeat n example-feature-bgt-wegdeel))

(defn render-wegdeel-with-bgt-example [n]
  (map (partial m/render-resource "templates/gml2extract/wegdeel.mustache")
       (example-bgt-features n)))

(deftest test-features-mapping
  (is (= 5 (count(filter #(re-find #"G0303.0979f33001fd319ae05332a1e90a5e0b" %) (render-wegdeel-with-bgt-example 5)))))
  (is (= 5 (count(filter #(re-find #"<imgeo:inOnderzoek>false</imgeo:inOnderzoek>" %) (render-wegdeel-with-bgt-example 5))))))


(deftest test-features-gml-mapping
  (let [rendered (render-wegdeel-with-bgt-example 5)]
    (is (= 5 (count(filter #(re-find #"<gml:posList" %) rendered))))
    (testing ".gml should remove <?xml tags"
      (is (= 0 (count (filter #(re-find #"<\?" %) rendered)))))))


(defn render-wegdeel-with-sets-with-bgt-example [n]
  (let [rendered (map (partial m/render-resource "templates/gml2extract/dummy-set.mustache")
                      (example-bgt-features n))]
    rendered))

(deftest test-features-mapping-with-sets
  (is (= 2 (count (filter #(re-find #"<val>een</val>" %) (render-wegdeel-with-sets-with-bgt-example 2)))))
  (is (= 2 (count (filter #(re-find #"<val>drie</val>" %) (render-wegdeel-with-sets-with-bgt-example 2)))))
  )

(defn write-gml-files [n]
  (time (with-open [w (clojure.java.io/writer "target/features.gml.json")]
          (json/generate-stream {:features (render-wegdeel-with-bgt-example n)} w))))


(deftest collections-are-proxied-too []
                                     (let [feature {:nested (into [] (map (fn [g] {:_geometry g}) (repeat 3 example-geometry-bgt-wegdeel)))}
                                           rendered (m/render-resource "templates/gml2extract/nested-function.mustache" feature)]
                                       (is (= 3 (count (re-seq #"<gml:posList" rendered))))
                                       (is (= 0 (count (re-seq #"<\?" rendered))))))
