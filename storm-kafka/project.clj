(defproject storm/storm-kafka "0.8.0-wip4" 
  :url "https://github.com/nathanmarz/storm-contrib/tree/master/storm-kafka"
  :description "Kafka support for Storm"
  :license {:url "Eclipse Public License - Version 1.0"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :dependencies [[org.clojars.jasonjckn/kafka_2.9.1 "0.7.0"
                  :exclusions [org.apache.zookeeper/zookeeper log4j/log4j]]]
                  
  :profiles {:provided {:dependencies [[storm "0.8.0"] [org.clojure/clojure "1.4.0"]]}}

  :min-lein-version "2.0.0"
  :javac-options {:debug "true"}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"])
