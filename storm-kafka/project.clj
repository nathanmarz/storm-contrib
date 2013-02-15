(defproject storm/storm-kafka "0.8.2"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"releases" "http://artifactory.local.twitter.com/libs-releases-local"
                 "snapshots" "http://artifactory.local.twitter.com/libs-snapshots-local"}
;;   :dependencies [[
;;                   [org.clojars.paul/core-kafka_2.8.0 "0.7.0-1"
;;                    :exclusions
;;                    [javax.mail/mail
;;                     javax.jms/jms
;;                     com.sun.jdmk/jmxtools
;;                     com.sun.jmx/jmxri
;;                     jline/jline
;;                     net.sf.jopt-simple/jopt-simple
;;                     junit/junit]]
;;                   [org.scala-lang/scala-library "2.8.0"]
;;                   org.apache.zookeeper/zookeeper
;;                   log4j/log4j
;;                   ]
;;                  ]
  ;; ]


  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojars.paul/core-kafka_2.8.0 "0.7.0-1"
                  :exclusions
                  [javax.mail/mail
                   javax.jms/jms
                   com.sun.jdmk/jmxtools
                   com.sun.jmx/jmxri
                   jline/jline
                   net.sf.jopt-simple/jopt-simple
                   junit/junit]]
                 [org.scala-lang/scala-library "2.8.0"]
                 [zookeeper-clj "0.9.2"]]

  :dev-dependencies [[storm "0.8.2"]
                     [org.clojure/clojure "1.4.0"]]
  :jvm-opts ["-Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib"]
)
