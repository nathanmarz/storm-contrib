(defproject storm/storm-backport "0.7.3"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"releases" "http://artifactory.local.twitter.com/libs-releases-local"
                 "snapshots" "http://artifactory.local.twitter.com/libs-snapshots-local"
                 "artifactory" "http://artifactory.local.twitter.com/repo"}
  :dependencies []
  :dev-dependencies [[storm "0.7.3"]
                     [org.clojure/clojure "1.4.0"]]
)
