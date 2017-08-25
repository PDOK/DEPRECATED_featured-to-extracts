(ns pdok.featured-to-extracts.mustache-functions
  (:require [pdok.featured.feature :as feature]
            pdok.util))

(defn gml [arg] (feature/as-gml arg))

(defn simple-gml [arg] (feature/as-simple-gml arg))

(defn stufgeo-field [arg] (feature/as-stufgeo-field arg))
(defn stufgeo-gml [arg] (feature/as-stufgeo-gml arg))
(defn stufgeo-gml-lc [arg] (feature/as-stufgeo-gml-lc arg))


(defn delta-type [arg]
  (condp = arg
    "new" "toevoeging"
    "delete" "verwijdering"
    "wijziging")
  )
