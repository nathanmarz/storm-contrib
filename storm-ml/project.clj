(defproject storm-ml "0.0.1-SNAPSHOT"
  :description "Online machine learning library on top of Storm"
  :url "https://github.com/nathanmarz/storm-contrib/tree/master/storm-ml"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :aot :all
  :dev-dependencies [[org.clojure/clojure "1.4.0"]
                     [org.javatuples/javatuples "1.2"]
                     [storm "0.7.2"]])
