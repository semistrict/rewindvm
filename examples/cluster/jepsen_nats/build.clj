(ns build
  (:require [clojure.tools.build.api :as b]))

(def class-dir "target/classes")
(def uber-file "target/jepsen-nats.jar")
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn uber [_]
  (b/delete {:path "target"})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/compile-clj {:basis @basis
                  :ns-compile '[jepsen.nats.core]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @basis
           :main 'jepsen.nats.core}))
