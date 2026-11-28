(ns jepsen.nats.queue
  "JetStream queue workload and consistency checker (adapted from
  jepsen-io/nats, December 2025 NATS 2.12.1 analysis)."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :refer [meh secs->nanos]]]
            [jepsen.nats.client :as c]
            [tesser.core :as t]))

(def stream-name "jepsen-stream")
(def subjects ["jepsen.*"])

(defn k->subject [k] (str "jepsen." k))

(defn pair->process [pair] (parse-long (re-find #"^\d+" pair)))
(defn pair->counter [pair] (parse-long (re-find #"\d+$" pair)))
(defn pair [process counter] (str process "-" counter))

(defn sub!
  [client]
  (let [subscription (:subscription client)]
    (when-not (realized? subscription)
      (when-let [sub (try (c/subscribe! (:client client) (k->subject 0))
                          (catch java.lang.IllegalStateException e
                            (info "Can't subscribe," (.getMessage e)))
                          (catch java.io.IOException e
                            (info "Can't subscribe," (.getMessage e)))
                          (catch io.nats.client.JetStreamApiException e
                            (info "Can't subscribe," (.getMessage e))))]
        (deliver subscription sub)))
    (when (realized? subscription)
      @subscription)))

(defrecord Client [client subscription]
  client/Client
  (open! [this test node]
    (try
      (assoc this
             :client       (c/open test node)
             :subscription (promise))
      (catch java.io.IOException e
        (Thread/sleep 500)
        (throw e))))

  (setup! [_ test]
    (c/with-retry
      (c/create-stream! client
                        {:name     stream-name
                         :subjects subjects
                         :replicas (min 3 (count (:nodes test)))})))

  (invoke! [this _test {:keys [f value] :as op}]
    (c/with-errors op
      (case f
        :crash (assoc op :type :info, :error :crash)

        :publish
        (do (c/publish! client (k->subject 0) value)
            (assoc op :type :ok))

        :next-message
        (if-let [sub (sub! this)]
          (if-let [m (c/next-message sub 1000)]
            (do (c/ack! m)
                (assoc op :type :ok, :value (c/data m)))
            (assoc op :type :fail, :error :caught-up))
          (do (Thread/sleep 500)
              (assoc op :type :info, :error :not-subscribed)))

        :fetch
        (if-let [sub (sub! this)]
          (let [batch (c/fetch sub)]
            (when (empty? batch)
              (Thread/sleep 300))
            (mapv c/ack! batch)
            (assoc op :type :ok, :value (mapv c/data batch)))
          (do (Thread/sleep 500)
              (assoc op :type :info, :error :not-subscribed))))))

  (teardown! [_ _test])

  (close! [_ _test]
    (when (realized? subscription)
      (meh (c/unsubscribe! @subscription)))
    (c/close! client))

  client/Reusable
  (reusable? [_ _test] true))

(defrecord TrackCounters [context-key fs counters gen]
  gen/Generator
  (op [_ test ctx]
    (let [ctx' (assoc ctx context-key counters)]
      (when-let [[op gen'] (gen/op gen test ctx')]
        [op (TrackCounters. context-key fs counters gen')])))

  (update [_ test ctx {:keys [f type value] :as event}]
    (if (and (identical? type :ok)
             (contains? fs f))
      (let [pairs     (case f
                        :publish      [value]
                        :next-message [value]
                        :fetch        value)
            counters' (reduce (fn [counters p]
                                (let [proc (pair->process p)
                                      cnt  (max (pair->counter p)
                                                (get counters proc 0))]
                                  (assoc counters proc cnt)))
                              counters
                              pairs)
            ctx'      (assoc ctx context-key counters')
            gen'      (gen/update gen test ctx' event)]
        (TrackCounters. context-key fs counters' gen'))
      (let [ctx' (assoc ctx context-key counters)
            gen' (gen/update gen test ctx' event)]
        (TrackCounters. context-key fs counters gen')))))

(defn track-writes [gen] (TrackCounters. :writes #{:publish} {} gen))
(defn track-reads  [gen] (TrackCounters. :reads #{:fetch :next-message} {} gen))

(defn caught-up?
  [{:keys [reads writes]}]
  (every? (fn [[process counter]]
            (<= counter (get reads process -1)))
          writes))

(defn until-caught-up
  [gen]
  (gen/on-update
    (fn [_this test ctx op]
      (if (caught-up? ctx)
        (gen/log "Caught up")
        (-> gen
            (gen/update test ctx op)
            until-caught-up)))
    gen))

(defrecord UntilSubFails [deadline gen]
  gen/Generator
  (op [this test ctx]
    (if-not deadline
      (let [limit    (:final-time-limit test)
            deadline (+ (:time ctx) (secs->nanos limit))]
        (gen/op (assoc this :deadline deadline) test ctx))
      (when-let [[op gen'] (gen/op gen test ctx)]
        [op (assoc this :gen gen')])))

  (update [this test ctx op]
    (if (and deadline
             (< deadline (:time ctx))
             (= :not-subscribed (:error op)))
      (gen/log (str "Giving up on drain; still don't have a subscription after "
                    (:final-time-limit test) " seconds"))
      (assoc this :gen (gen/update gen test ctx op)))))

(defn until-sub-fails [gen] (UntilSubFails. nil gen))

(defn drain
  [_opts]
  (gen/each-thread
    (->> (gen/repeat {:f :fetch})
         until-caught-up
         until-sub-fails
         track-reads)))

(defrecord Writes [next-counters]
  gen/Generator
  (op [_ _test ctx]
    (if-let [p (gen/some-free-process ctx)]
      (let [counter (get next-counters p 0)
            ncs'    (assoc next-counters p (inc counter))]
        [(h/->Op -1
                 (:time ctx)
                 :invoke
                 p
                 :publish
                 (pair p counter))
         (Writes. ncs')])
      [:pending (Writes. next-counters)]))

  (update [this _test _ctx _event]
    this))

(defn generator [_opts] (Writes. {}))

(defn lost-breakout
  "Partition lost values into :lost-prefix, :lost-middle (log holes), and
  :lost-postfix per writer process (from jepsen-io/nats)."
  [lost read]
  (loopr [first-reads (transient {})
          last-reads  (transient {})]
         [p read]
         (let [proc (pair->process p)
               cnt  (pair->counter p)]
           (recur
             (if (< cnt (first-reads proc Long/MAX_VALUE))
               (assoc! first-reads proc cnt)
               first-reads)
             (if (< (last-reads proc Long/MIN_VALUE) cnt)
               (assoc! last-reads proc cnt)
               last-reads)))
         (loopr [prefix  (transient #{})
                 middle  (transient #{})
                 postfix (transient #{})]
                [p lost]
                (let [proc (pair->process p)
                      cnt  (pair->counter p)]
                  (cond
                    (< cnt (first-reads proc Long/MIN_VALUE))
                    (recur (conj! prefix p) middle postfix)

                    (< cnt (last-reads proc Long/MIN_VALUE))
                    (recur prefix (conj! middle p) postfix)

                    true
                    (recur prefix middle (conj! postfix p))))
                {:lost         lost
                 :lost-prefix  (persistent! prefix)
                 :lost-middle  (persistent! middle)
                 :lost-postfix (persistent! postfix)})))

(defrecord Checker []
  checker/Checker
  (check [_ test history _opts]
    (let [nodes    (:nodes test)
          n        (count nodes)
          op->node (fn [op] (nth nodes (mod (:process op) n)))
          {:keys [attempts published next-messaged fetched fetched-by-node]}
          (->> (t/fuse
                 {:attempts (->> (t/filter (h/has-f? :publish))
                                 (t/filter h/invoke?)
                                 (t/map :value)
                                 (t/set))
                  :published (->> (t/filter (h/has-f? :publish))
                                  (t/filter h/ok?)
                                  (t/map :value)
                                  (t/set))
                  :next-messaged (->> (t/filter (h/has-f? :next-message))
                                      (t/filter h/ok?)
                                      (t/map :value)
                                      (t/set))
                  :fetched (->> (t/filter (h/has-f? :fetch))
                                (t/filter h/ok?)
                                (t/mapcat :value)
                                (t/set))
                  :fetched-by-node (->> (t/filter h/ok?)
                                        (t/filter (h/has-f? :fetch))
                                        (t/group-by op->node)
                                        (t/mapcat :value)
                                        (t/set))})
               (h/tesser history))
          fetched-by-node (->> fetched-by-node
                               (merge (zipmap nodes (repeat #{})))
                               (into (sorted-map)))
          read         (set/union next-messaged fetched)
          ok           (set/intersection attempts read)
          unexpected   (set/difference read attempts)
          recovered    (set/difference ok published)
          lost         (set/difference published read)
          lost         (lost-breakout lost read)
          lost-on-node (into (sorted-map)
                             (map (fn [[node fset]]
                                    [node (lost-breakout (set/difference published fset)
                                                         fset)])
                                  fetched-by-node))
          short-lost   (fn [m]
                         (reduce (fn [acc k]
                                   (assoc acc
                                          k (vec (take 16 (sort (m k))))
                                          (keyword (str (name k) "-count")) (count (m k))))
                                 (sorted-map)
                                 (keys m)))]
      {:valid?             (and (empty? (:lost lost))
                                (empty? unexpected)
                                (every? empty? (map :lost (vals lost-on-node))))
       :attempt-count      (count attempts)
       :acknowledged-count (count published)
       :read-count         (count read)
       :ok-count           (count ok)
       :unexpected-count   (count unexpected)
       :recovered-count    (count recovered)
       :lost               (short-lost lost)
       :lost-on-node       (update-vals lost-on-node short-lost)
       :unexpected         (vec (take 16 (sort unexpected)))})))

(defn workload
  [opts]
  {:generator       (generator opts)
   :final-generator (drain opts)
   :wrap-generator  track-writes
   :client          (Client. nil nil)
   :checker         (Checker.)})
