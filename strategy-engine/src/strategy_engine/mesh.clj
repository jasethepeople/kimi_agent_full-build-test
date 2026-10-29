(ns strategy-engine.mesh
  "NATS pub/sub via jnats. Side effects live here."
  (:import [io.nats.client Nats Options Message]
           [java.nio.charset StandardCharsets]))

(defn connect
  "Connect to NATS at `url` with automatic reconnect enabled."
  [url]
  (let [opts (-> (Options/builder)
                 (.server url)
                 (.maxReconnects -1)
                 (.reconnectWait (java.time.Duration/ofSeconds 2))
                 (.connectionName "strategy-engine")
                 (.build))]
    (Nats/connect opts)))

(defn publish-json!
  "Publish a UTF-8 JSON string to `subject`."
  [conn subject json-str]
  (.publish conn subject (.getBytes ^String json-str StandardCharsets/UTF_8)))

(defn subscribe!
  "Subscribe to `subject`; handler receives a decoded UTF-8 string payload.
  Returns the Subscription (call .unsubscribe or close conn to stop)."
  [conn subject handler]
  (let [dispatcher (.createDispatcher conn
                       (proxy [io.nats.client.MessageHandler] []
                         (onMessage [^Message msg]
                           (try
                             (handler (String. (.getData msg) StandardCharsets/UTF_8))
                             (catch Throwable t
                               (println "mesh: handler error on" subject ":" (.getMessage t)))))))]
    (.subscribe dispatcher subject)))

(defn close! [conn]
  (when conn
    (try (.close conn) (catch Throwable _))))
