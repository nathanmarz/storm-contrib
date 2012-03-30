(defproject storm/storm-scribe "0.0.1-SNAPSHOT"
  :description "Spout implementation for scribe"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :resources-path "config"
  :dependencies []
  :dev-dependencies [[storm "0.7.1-SNAPSHOT"]
                     [org.clojure/clojure "1.2.1"]
                     [org.clojure/clojure-contrib "1.2.0"]                     
                     ])
