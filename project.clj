(defproject manila-john "0.2.0-SNAPSHOT"
  :description "A Clojure library for Apache CouchDB."
  :url "https://github.com/john-shaffer/manila-john"
  :license {:name "BSD"
            :url "http://www.opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [cheshire "5.9.0"]
                 [clj-http "3.10.0"]
                 [com.ashafa/clutch "0.4.0"]
                 [com.arohner/uri "0.1.2"]
                 [commons-codec "1.13"]
                 [org.clojure/clojurescript "1.10.520" :optional true]]
  :min-lein-version "2.0.0"
  :repl-options {:init-ns manila-john})
