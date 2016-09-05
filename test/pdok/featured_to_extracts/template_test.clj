(ns pdok.featured-to-extracts.template-test
  (:require [clojure.test :refer :all]
            [pdok.featured-to-extracts.template :as t]))

(def test-template "{{>model-start}}<imgeo:Bak>{{>start-feature-type}}<imgeo:geometrie2dBak>{{{_geometry.gml}}}</imgeo:geometrie2dBak></imgeo:Bak>{{>model-eind}}")
(def expected-template "{{>bgt-city-model-start}}<imgeo:Bak>{{>bgt-city-start-feature-type}}<imgeo:geometrie2dBak>{{{_geometry.gml}}}</imgeo:geometrie2dBak></imgeo:Bak>{{>bgt-city-model-eind}}")

(deftest test-prefix-partials-in-template
         (is (= expected-template (t/prefix-partials test-template "bgt-city-"))))
