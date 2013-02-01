(defproject storm-redis-pubsub "0.0.2-SNAPSHOT"
  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib"]
  :aot :all
  :dependencies [[redis.clients/jedis "2.0.0"]]
  :profiles {:provided
             {:dependencies [[storm "0.8.2"]]}})
