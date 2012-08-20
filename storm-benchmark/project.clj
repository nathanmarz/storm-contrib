(defproject storm/storm-benchmark "0.0.1-SNAPSHOT"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :dependencies []
  :dev-dependencies [[storm "0.8.1-wip5"]
                     [org.clojure/clojure "1.4.0"]]
)
