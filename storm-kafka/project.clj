(defproject storm/storm-kafka "0.8.2"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"releases" "http://artifactory.local.twitter.com/libs-releases-local"
                 "snapshots" "http://artifactory.local.twitter.com/libs-snapshots-local"}


  :dependencies [
                 [storm/kafka "0.7.0-incubating"
                  :exclusions
                  [org.apache.zookeeper/zookeeper
                   log4j/log4j
                   ]
                  ]
                 [org.scala-lang/scala-library "2.8.0"]

                 ]




  :dev-dependencies [[storm "0.8.2"]
                     [org.clojure/clojure "1.4.0"]]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
)
