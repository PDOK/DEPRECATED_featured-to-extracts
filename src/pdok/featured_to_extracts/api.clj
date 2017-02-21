(ns pdok.featured-to-extracts.api
  (:require [pdok.featured-to-extracts
             [config :as config]
             [core :as core]
             [mustache :as mustache]
             [template :as template]
             [zipfiles :as zipfiles]]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [clj-time [core :as t] [local :as tl]]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :refer :all]
            [ring.util.response :as r]
            [schema.core :as s]
            [clojure.java.io :as io])
  (:import (clojure.lang PersistentQueue)
           (org.joda.time DateTime)
           (com.fasterxml.jackson.core JsonGenerator)
           (java.net URISyntaxException)
           (schema.utils ValidationError)
           (java.io File)))

(extend-protocol cheshire.generate/JSONable
  DateTime
  (to-json [t ^JsonGenerator jg] (.writeString jg (str t)))
  ValidationError
  (to-json [t ^JsonGenerator jg] (.writeString jg (pr-str t))))

(defn uri [str]
  (try
    (let [uri (java.net.URI. str)]
      uri)
    (catch URISyntaxException e nil)))

(def URI (s/pred uri 'URI ))

(def ExtractRequest
  "A schema for a JSON extract request"
  {:dataset                   s/Str
   :changeLog                 URI
   (s/optional-key :format)   (s/enum "csv" "zip")
   :extractTypes              [s/Str]
   (s/optional-key :callback) URI})

(def TemplateRequest
  "A schema for a JSON template request"
  {:dataset s/Str
   :extractType s/Str
   :templateName s/Str
   :template s/Str})

(defn- callbacker [uri run-stats]
  (try
    (http/post uri {:body (json/generate-string run-stats)
                    :headers {"Content-Type" "application/json"}})
    (catch Exception e (log/error "Callback error" e))))

(defn- stats-on-callback [callback-chan request stats]
  (when (:callback request)
    (go (>! callback-chan [(:callback request) stats]))))

(defn download-file [uri zipped?]
  "returns [file err]"
  (try
    (let [tmp (File/createTempFile "featured-to-extracts" (if zipped? ".zip" ".csv"))
          in (io/input-stream uri)]
      (log/info "Downloading" uri)
      (io/copy in tmp)
      (.close in)
      (if zipped?
        (do
          (log/info "Extracting" uri)
          (let [entry (zipfiles/first-file-from-zip tmp)]
            (io/delete-file tmp)
            [entry nil]))
        [tmp nil]))
    (catch Exception e
      [nil e])))

(defn process* [worker-id stats callback-chan request]
  (log/info "Processing extract: " request)
  (swap! stats assoc-in [:processing worker-id] request)
  (swap! stats update-in [:queued] pop)
  (let [dataset (:dataset request)
        extract-types (:extractTypes request)
        zipped? (if (nil? (:format request)) true (= (:format request) "zip"))
        [file err] (download-file (:changeLog request) zipped?)]
    (if-not file
      (let [msg (if err (str err) "Something went wrong downloading")
            error-stats (merge request {:status "error" :msg msg})]
        (log/warn msg error-stats)
        (swap! stats assoc-in [:processing worker-id] nil)
        (stats-on-callback callback-chan request error-stats))
      (try
        (with-open [in (io/input-stream file)]
          (let [_ (log/info "Processing file:" (:changeLog request))
                result (core/update-extracts dataset extract-types in)
                run-stats (merge request result)]
            (swap! stats assoc-in [:processing worker-id] nil)
            (stats-on-callback callback-chan request run-stats)))
        (catch Exception e
          (let [error-str (if (instance? Iterable e)
                            (clojure.string/join " Next: " (map str (seq e)))
                            (str e))
                error-stats (merge request {:status "error" :msg error-str})]
            (log/warn e error-stats)
            (swap! stats assoc-in [:processing worker-id] nil)
            (stats-on-callback callback-chan request error-stats)))
        (finally (io/delete-file file)))
      )))

(defn create-workers [stats callback-chan process-chan]
  (let [factory-fn (fn [worker-id]
                     (swap! stats assoc-in [:processing worker-id] nil)
                     (log/info "Creating worker " worker-id)
                     (go (while true (process* worker-id stats callback-chan (<! process-chan)))))]
    (config/create-workers factory-fn)))

(defn wrap-exception-handling
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e)
        {:status 400 :body (.getMessage e)}))))

(defn- process-request [stats schema queue-id request-chan http-req]
  (let [request (:body http-req)
        invalid (s/check schema request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (if (a/offer! request-chan request)
        (do (swap! stats update-in [queue-id] #(conj % request)) (r/response {:result :ok}))
        (r/status (r/response {:error "queue full"}) 429)))))

(defn- template-request [http-req]
  (let [request (:body http-req)
        invalid (s/check TemplateRequest request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (r/response (if (template/add-or-update-template {:dataset      (:dataset request)
                                                        :extract-type (:extractType request)
                                                        :name         (:templateName request)
                                                        :template     (:template request)})
                    {:status "ok"}
                    {:status "error"})))))

(defn api-routes [process-chan stats]
  (defroutes api-routes
             (context "/api" []
               (GET "/info" [] (r/response {:version (slurp (clojure.java.io/resource "version"))}))
               (GET "/ping" [] (r/response {:pong (tl/local-now)}))
               (POST "/ping" [] (fn [r] (log/info "!ping pong!" (:body r)) (r/response {:pong (tl/local-now)})))
               (GET "/stats" [] (r/response @stats))
               (POST "/process" [] (partial process-request stats ExtractRequest :queued process-chan))
               (POST "/template" [] (fn [r] (r/response (template-request r)))))
             (route/not-found "NOT FOUND")))

(def process-chan (chan 100))
(def callback-chan (chan 10))
(def stats  (atom {:processing {}
                   :queued     (PersistentQueue/EMPTY)}))

(defn init! []
  (create-workers stats callback-chan process-chan)
  (go (while true (apply callbacker (<! callback-chan)))))

(def app (-> (api-routes process-chan stats)
             (wrap-json-body {:keywords? true :bigdecimals? true})
             (wrap-json-response)
             (wrap-defaults api-defaults)
             (wrap-exception-handling)
             (routes)))
