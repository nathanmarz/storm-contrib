(defproject storm/storm-kafka_2.9.2 "0.8.2.1"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"conjars" "http://conjars.org/repo/"}


  :dependencies [
                 [com.twitter/kafka_2.9.2 "0.7.0"
                  :exclusions
                  [org.apache.zookeeper/zookeeper
                   log4j/log4j
                   ]
                  ]
                 [org.scala-lang/scala-library "2.9.2"]

                 ]




  :dev-dependencies [[storm "0.8.2"]
                     [org.clojure/clojure "1.4.0"]]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
)
