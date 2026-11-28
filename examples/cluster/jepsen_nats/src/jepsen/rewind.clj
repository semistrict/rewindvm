(ns jepsen.rewind
  "Reusable Jepsen adapter for Rewind VM deterministic clusters.

  Routes Jepsen's pluggable fault protocols (`jepsen.net.proto/Net`,
  `PartitionAll`, `jepsen.db/Process`, `jepsen.db/Pause`, and clock skew)
  through Rewind's partition-immune `http://rewind` virtual coordinator
  (`POST http://rewind/barrier/jepsen-fault`), where the host Rewind driver
  applies the exact hypervisor fault (`asymmetric_partition`, `heal`,
  `clog_link`, `set_link`, `crash`, `restart`, `pause`, `resume`,
  `step_clock`, `set_clock_drift`) and releases the barrier."
  (:require [cheshire.core :as json]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [nemesis :as nemesis]]
            [jepsen.net.proto :as net.proto])
  (:import (java.net URI)
           (java.net.http HttpClient
                          HttpClient$Version
                          HttpRequest
                          HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers)
           (java.time Duration)))

(def ^:private control-base "http://rewind")

(def ^:private http-client
  (.. (HttpClient/newBuilder)
      (version HttpClient$Version/HTTP_1_1)
      (connectTimeout (Duration/ofSeconds 10))
      (build)))

(defn signal!
  "Emit a one-way structured JSON signal to `POST http://rewind/signal/<name>`."
  [sig-name payload]
  (let [body (json/generate-string payload)
        req  (.. (HttpRequest/newBuilder
                   (URI/create (str control-base "/signal/" sig-name)))
                 (timeout (Duration/ofSeconds 30))
                 (header "Content-Type" "application/json")
                 (POST (HttpRequest$BodyPublishers/ofString body))
                 (build))
        resp (.send http-client req (HttpResponse$BodyHandlers/ofString))]
    (.body resp)))

(defn fault!
  "Synchronously ask the Rewind host driver to apply a hypervisor fault via
  `POST http://rewind/barrier/jepsen-fault` and wait for release."
  [cmd]
  (let [body (json/generate-string cmd)
        req  (.. (HttpRequest/newBuilder
                   (URI/create (str control-base "/barrier/jepsen-fault")))
                 (timeout (Duration/ofSeconds 60))
                 (header "Content-Type" "application/json")
                 (POST (HttpRequest$BodyPublishers/ofString body))
                 (build))
        resp (.send http-client req (HttpResponse$BodyHandlers/ofString))]
    (info "Rewind fault applied:" (pr-str cmd) "->" (.body resp))
    (.body resp)))

(defn grudge->blocked
  "Convert a Jepsen grudge `{dst [src1 src2 ...]}` to directed `[src dst]`
  pairs for Rewind's `asymmetric_partition`."
  [grudge]
  (vec (for [[dst srcs] grudge
             src srcs]
         [(str src) (str dst)])))

(defrecord RewindNet []
  net.proto/Net
  (drop! [this _test src dest]
    (fault! {:op "drop" :src (str src) :dst (str dest)}))

  (heal! [this _test]
    (fault! {:op "heal"}))

  (slow! [this test]
    (net.proto/slow! this test {:mean 50 :variance 10}))

  (slow! [this test opts]
    (fault! {:op "slow"
             :nodes (mapv str (:nodes test))
             :mean_ms (long (:mean opts 50))
             :jitter_ms (long (:variance opts 10))}))

  (flaky! [this test]
    (fault! {:op "flaky"
             :nodes (mapv str (:nodes test))
             :drop_ppm 200000}))

  (fast! [this test]
    (fault! {:op "fast"
             :nodes (mapv str (:nodes test))}))

  (shape! [this _test nodes behavior]
    (fault! {:op "shape"
             :nodes (mapv str nodes)
             :behavior behavior}))

  net.proto/PartitionAll
  (drop-all! [this _test grudge]
    (let [blocked (grudge->blocked grudge)]
      (if (seq blocked)
        (fault! {:op "asymmetric_partition" :blocked blocked})
        (fault! {:op "heal"})))))

(defn net
  "Return a Jepsen `Net` + `PartitionAll` backed by Rewind's L2 virtual switch."
  []
  (RewindNet.))

(defrecord RewindDB []
  db/DB
  (setup! [_ _test _node])
  (teardown! [_ _test _node])

  db/Process
  (start! [_ _test node]
    (fault! {:op "restart" :node (str node)})
    :started)

  (kill! [_ _test node]
    (fault! {:op "crash" :node (str node)})
    :killed)

  db/Pause
  (pause! [_ _test node]
    (fault! {:op "pause" :node (str node)})
    :paused)

  (resume! [_ _test node]
    (fault! {:op "resume" :node (str node)})
    :resumed))

(defn db
  "Return a Jepsen `DB` + `Process` + `Pause` backed by Rewind's hypervisor
  `crash`/`restart` and `pause`/`resume`."
  []
  (RewindDB.))

(defn clock-nemesis
  "Jepsen nemesis that drives Rewind's hypervisor `step_clock` / `set_clock_drift`."
  []
  (reify
    nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (let [nodes (vec (or (:value op) (:nodes test)))]
        (case (:f op)
          :bump-clock
          (do (doseq [n nodes]
                (fault! {:op "step_clock" :node (str n) :delta_ms 5000}))
              (assoc op :value {:bumped nodes :delta_ms 5000}))

          :drift-clock
          (do (doseq [n nodes]
                (fault! {:op "set_clock_drift" :node (str n) :drift_ppm 50000}))
              (assoc op :value {:drifted nodes :drift_ppm 50000}))

          :reset-clock
          (do (doseq [n (:nodes test)]
                (fault! {:op "set_clock_drift" :node (str n) :drift_ppm 0}))
              (assoc op :value :reset)))))
    (teardown! [_ _test])

    nemesis/Reflection
    (fs [_] #{:bump-clock :drift-clock :reset-clock})))

(defn clock-package
  "A `jepsen.nemesis.combined`-compatible clock fault package using Rewind's
  hypervisor clock primitives."
  [{:keys [faults interval nodes]}]
  (let [needed? (contains? (set faults) :clock)]
    {:nemesis         (when needed? (clock-nemesis))
     :generator       (when needed?
                        (->> (gen/repeat
                               (fn [_ _]
                                 {:type  :info
                                  :f     :bump-clock
                                  :value [(rand-nth (vec nodes))]}))
                             (gen/stagger (or interval 5))))
     :final-generator (when needed?
                        [{:type :info :f :reset-clock :value nil}])
     :perf            #{{:name  "clock"
                         :fs    #{:bump-clock :drift-clock :reset-clock}
                         :color "#A0E0A0"}}}))
