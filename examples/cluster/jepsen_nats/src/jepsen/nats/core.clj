(ns jepsen.nats.core
  "CLI entrypoint running real Jepsen (`jepsen.core/run!`) against NATS 2.12.1
  JetStream inside a deterministic Rewind VM cluster."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [core :as jepsen]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.nats.queue :as queue]
            [jepsen.rewind :as rewind]))

(defn parse-args
  [args]
  (loop [m {:nodes                ["n1" "n2" "n3"]
            :nemesis              #{}
            :time-limit           20
            :final-time-limit     12
            :initial-quiet-period 2
            :rate                 25
            :concurrency          3
            :nemesis-interval     4
            :db-targets           [:one :majority]}
         xs args]
    (if (empty? xs)
      m
      (let [[k v & more] xs]
        (case k
          "--nodes"
          (recur (assoc m :nodes (vec (str/split v #","))) more)

          "--nemesis"
          (let [fs (if (or (str/blank? v) (= v "none"))
                     #{}
                     (set (map keyword (str/split v #","))))]
            (recur (assoc m :nemesis fs) more))

          "--time-limit"
          (recur (assoc m :time-limit (parse-long v)) more)

          "--final-time-limit"
          (recur (assoc m :final-time-limit (parse-long v)) more)

          "--initial-quiet-period"
          (recur (assoc m :initial-quiet-period (parse-long v)) more)

          "--rate"
          (recur (assoc m :rate (parse-long v)) more)

          "--concurrency"
          (recur (assoc m :concurrency (parse-long v)) more)

          "--nemesis-interval"
          (recur (assoc m :nemesis-interval (parse-long v)) more)

          "--db-targets"
          (recur (assoc m :db-targets (mapv keyword (str/split v #","))) more)

          (throw (ex-info (str "Unknown flag: " k) {:arg k})))))))

(defn build-nemesis-package
  [db opts]
  (let [faults (:nemesis opts)
        pkg-opts {:db        db
                  :nodes     (:nodes opts)
                  :faults    faults
                  :interval  (:nemesis-interval opts)
                  :partition {:targets [:one :majority]}
                  :pause     {:targets (:db-targets opts)}
                  :kill      {:targets (:db-targets opts)}}]
    (nc/compose-packages
      [(nc/partition-package pkg-opts)
       (nc/db-package pkg-opts)
       (rewind/clock-package pkg-opts)])))

(defn nats-test
  [opts]
  (let [workload     (queue/workload opts)
        db           (rewind/db)
        net          (rewind/net)
        nem-pkg      (build-nemesis-package db opts)
        nem-gen      (:generator nem-pkg)
        workload-gen (->> (:generator workload)
                          (gen/stagger (/ 1 (:rate opts)))
                          gen/clients)
        main-gen     (if nem-gen
                       (->> workload-gen
                            (gen/nemesis
                              (gen/phases
                                (gen/sleep (:initial-quiet-period opts))
                                nem-gen))
                            (gen/time-limit (:time-limit opts)))
                       (gen/time-limit (:time-limit opts) workload-gen))
        full-gen     (gen/phases
                       main-gen
                       (when-let [fg (:final-generator nem-pkg)]
                         (gen/nemesis
                           [(gen/log "Recovering cluster faults")
                            fg
                            (gen/sleep 2)
                            (gen/log "Beginning final drain")]))
                       (->> (:final-generator workload)
                            (gen/time-limit (:final-time-limit opts))
                            gen/clients))
        wrapped-gen  ((:wrap-generator workload) full-gen)]
    (merge tests/noop-test
           opts
           {:name            (str "nats-2.12.1-rewind-"
                                  (if (seq (:nemesis opts))
                                    (str/join "," (sort (map name (:nemesis opts))))
                                    "none"))
            :ssh             {:dummy? true}
            :os              os/noop
            :db              db
            :net             net
            :pure-generators true
            :client          (:client workload)
            :nemesis         (or (:nemesis nem-pkg) nemesis/noop)
            :generator       wrapped-gen
            :checker         (checker/compose
                               {:stats      (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                :workload   (:checker workload)})
            :logging         {:overrides
                              {"io.nats.client.impl.ErrorListenerLoggerImpl" "FATAL"}}})))

(defn -main
  [& args]
  (try
    (let [opts    (parse-args args)
          test    (nats-test opts)
          _       (info "Starting Jepsen test on Rewind VM:" (:name test))
          result  (jepsen/run! test)
          res     (:results result)
          wl      (:workload res)
          summary {:name               (:name test)
                   :valid              (boolean (:valid? res))
                   :workload_valid     (boolean (:valid? wl))
                   :attempt_count      (:attempt-count wl)
                   :acknowledged_count (:acknowledged-count wl)
                   :read_count         (:read-count wl)
                   :ok_count           (:ok-count wl)
                   :recovered_count    (:recovered-count wl)
                   :unexpected_count   (:unexpected-count wl)
                   :lost               (:lost wl)
                   :lost_on_node       (:lost-on-node wl)
                   :stats              (:stats res)}]
      (println "\n=== JEPSEN RESULTS ===")
      (pprint res)
      (rewind/signal! "jepsen-result" summary)
      (System/exit 0))
    (catch Throwable t
      (.printStackTrace t)
      (try
        (rewind/signal! "jepsen-error" {:error (str t)})
        (catch Throwable _))
      (System/exit 1))))
