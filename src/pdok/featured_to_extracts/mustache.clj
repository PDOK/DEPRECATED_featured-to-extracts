(ns pdok.featured-to-extracts.mustache
  (:require [stencil.core :as stencil]
            [stencil.loader :as loader]
            [pdok.featured-to-extracts.mustache-functions]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn resolve-as-function [namespace function]
  (ns-resolve *ns* (symbol (str namespace "/" (name function)))))

(declare collection-proxy lookup-proxy)

(defn mustache-proxy [k value]
  (if (sequential? value)
    (collection-proxy k value)
    (lookup-proxy value)))

(defn val-at[k obj]
  (if-let [f (resolve-as-function "pdok.featured-to-extracts.mustache-functions" k)]
    (mustache-proxy k (f obj))
    (if (and (map? obj) (or (contains? obj k) (contains? obj (name k))))
      (let [value (get obj k (get obj (name k)))]
        (if (or (= (class value) pdok.featured.NilAttribute) (= value nil))
          nil
          (mustache-proxy k value))))))

(defn lookup-proxy [obj]
  (reify
    clojure.lang.ILookup
    (valAt [_ k] (val-at k nil))
    (valAt [_ k not-found] (if-let [v (val-at k obj)] v not-found))
    clojure.lang.IPersistentCollection
    (cons [_ o](lookup-proxy (conj obj o)))
    (seq [this] (if-not (clojure.string/blank? (str obj)) (list obj) nil))
    clojure.lang.Associative
    (containsKey [_ key] (if (val-at key obj) true false))
    Object
    (toString [_] (str obj))))

(defn create-counter []
  (let [state (atom -1)] (fn [] (swap! state inc) @state)))

(defn collection-proxy [k obj]
  (reify
    clojure.lang.Sequential
    clojure.lang.ILookup
    (valAt [_ k] (val-at k obj))
    clojure.lang.IPersistentCollection
    (cons [_ o](mustache-proxy k (conj obj o)))
    (seq [this]
      (if (or (map? obj) (string? obj))
        (list (mustache-proxy k obj))
        (let [c1 (create-counter)]
          (seq (map (fn[idx] (mustache-proxy k (if (map? idx) (assoc idx (str (name k) "-elem-at-" (c1)) true) idx))) obj)))))
    Object
    (toString [_] (str obj))))

(def ^{:private true} registered-templates (atom #{}))

(defn register [name template]
  (swap! registered-templates conj name)
  (loader/unregister-template name)
  (loader/register-template name template))

(defn registered? [name]
  (if (@registered-templates name) true false))

(defn render [name feature]
  (stencil/render-file name (lookup-proxy feature)))

(defn render-resource
  ([path feature]
   (stencil/render-file path (lookup-proxy feature))))

