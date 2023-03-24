(defproject storm/storm-kafka "0.9.2-curator1.3.3-scala292"
  :java-source-paths ["src/jvm"]
  :repositories {"scala-tools" "http://scala-tools.org/repo-releases"
                  "conjars" "http://conjars.org/repo/"}
  :dependencies [[org.scala-lang/scala-library "2.9.2"]
                 [com.netflix.curator/curator-framework "1.3.3"
                  :exclusions [log4j/log4j
                               org.slf4j/slf4j-log4j12]]
                  [com.twitter/kafka_2.9.2 "0.7.0"
                  :exclusions [org.apache.zookeeper/zookeeper
                               log4j/log4j]]]
  :profiles
  {:provided {:dependencies [[org.apache.storm/storm-core "0.9.2-incubating-SNAPSHOT"]
                             [org.slf4j/log4j-over-slf4j "1.6.6"]
                             ;;[ch.qos.logback/logback-classic "1.0.6"]
                             [org.clojure/clojure "1.4.0"]]}}
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
  :min-lein-version "2.0")
