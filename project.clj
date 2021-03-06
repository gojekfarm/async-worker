(defproject com.gojek/async-worker "0.0.7"
  :description "An asynchronous job worker library for Clojure"
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.novemberain/langohr "5.1.0"
                  :exclusions [org.clojure/clojure]]
                 [com.taoensso/nippy "2.14.0"]
                 [camel-snake-kebab "0.4.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [pool "0.2.1"]]
  :main async-worker.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}}
  :plugins [[lein-cljfmt "0.6.8"]])
