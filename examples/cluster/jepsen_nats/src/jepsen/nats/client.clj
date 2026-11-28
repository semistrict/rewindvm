(ns jepsen.nats.client
  "Wrapper for the NATS JetStream Java client (adapted from jepsen-io/nats)."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt]
            [jepsen.codec :as codec])
  (:import (io.nats.client Connection
                           JetStream
                           JetStreamApiException
                           JetStreamManagement
                           JetStreamSubscription
                           Message
                           Nats
                           Options
                           PullSubscribeOptions
                           Subscription)
           (io.nats.client.api DiscardPolicy
                               StorageType
                               StreamConfiguration)
           (java.time Duration)))

(def port 4222)

(defrecord Client [^Connection conn
                   ^JetStream  js])

(defn open
  "Opens a JetStream connection to a single NATS node."
  [_test node]
  (let [opts (.. (Options/builder)
                 (server (str "nats://" node ":" port))
                 (userInfo "jepsen" "jepsenpw")
                 (ignoreDiscoveredServers)
                 (pingInterval (Duration/ofMillis 5000))
                 (connectionTimeout (Duration/ofMillis 5000))
                 (socketWriteTimeout (Duration/ofMillis 10000))
                 (socketReadTimeoutMillis 10000)
                 (maxReconnects -1)
                 (reconnectWait (Duration/ofSeconds 1))
                 (build))
        conn (Nats/connect opts)]
    (try
      (Client. conn (.jetStream conn))
      (catch Throwable t
        (.close conn)
        (throw t)))))

(defn close!
  [^Client client]
  (when client
    (.close ^Connection (.conn client))))

(defn ^JetStreamManagement jsm
  [^Client client]
  (.jetStreamManagement ^Connection (.conn client)))

(defn create-stream!
  [client opts]
  (let [stream-config (.. (StreamConfiguration/builder)
                          (name          (:name opts))
                          (storageType   StorageType/File)
                          (subjects      ^java.util.Collection (:subjects opts))
                          (replicas      (:replicas opts 3))
                          (discardPolicy DiscardPolicy/New)
                          (build))]
    (.addStream (jsm client) stream-config)))

(defn publish!
  [^Client client ^String subject message]
  (.publish ^JetStream (.js client) subject ^bytes (codec/encode message)))

(defn subscribe!
  [^Client client subject]
  (let [opts (.. (PullSubscribeOptions/builder)
                 (build))]
    (.subscribe ^JetStream (.js client) subject opts)))

(defn unsubscribe!
  [^Subscription subscription]
  (.unsubscribe subscription))

(defn fetch
  ([sub]
   (fetch sub 128))
  ([sub batch-size]
   (fetch sub batch-size 100))
  ([^JetStreamSubscription subscription ^long batch-size ^long timeout]
   (.fetch subscription batch-size timeout)))

(defn next-message
  ([sub]
   (next-message sub 0))
  ([^JetStreamSubscription subscription ^long timeout]
   (.nextMessage subscription timeout)))

(defn ack!
  [^Message msg]
  (.ack msg))

(defn data
  [^Message msg]
  (codec/decode (.getData msg)))

(defmacro with-retry
  [& body]
  `(dt/with-retry [tries# 15]
     ~@body
     (catch JetStreamApiException e#
       (if (pos? tries#)
         (do (info (.getMessage e#) "- retrying")
             (Thread/sleep 1000)
             (~'retry (dec tries#)))
         (throw e#)))
     (catch java.io.IOException e#
       (if (pos? tries#)
         (do (info (.getMessage e#) "- retrying")
             (Thread/sleep 1000)
             (~'retry (dec tries#)))
         (throw e#)))))

(defmacro with-errors
  [op & body]
  `(try ~@body
        (catch java.lang.IllegalStateException e#
          (Thread/sleep 500)
          (let [msg# (or (.getMessage e#) "")]
            (cond
              (re-find #"Connection is Closed" msg#)
              (assoc ~op :type :fail, :error [:conn-closed])

              (re-find #"(?i)not connected|closed|subscription" msg#)
              (assoc ~op :type :info, :error [:illegal-state msg#])

              :else
              (throw e#))))

        (catch java.io.IOException e#
          (Thread/sleep 500)
          (let [msg# (or (.getMessage e#) "")]
            (cond
              (re-find #"503 No Responders Available For Request" msg#)
              (assoc ~op :type :fail, :error [:no-responders])

              (re-find #"Timeout or no response waiting for NATS" msg#)
              (assoc ~op :type :info, :error [:timeout])

              :else
              (assoc ~op :type :info, :error [:io-error msg#]))))

        (catch JetStreamApiException e#
          (Thread/sleep 500)
          (let [msg# (or (.getMessage e#) "")]
            (cond
              (re-find #"(?i)jetstream system temporarily unavailable" msg#)
              (assoc ~op :type :fail, :error [:jetstream-temporarily-unavailable])

              (re-find #"(?i)stream not found|no stream response|timeout" msg#)
              (assoc ~op :type :info, :error [:js-api msg#])

              :else
              (assoc ~op :type :info, :error [:js-api msg#]))))))
