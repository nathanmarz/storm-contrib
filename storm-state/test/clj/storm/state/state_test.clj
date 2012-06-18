(ns storm.state.state-test
  (:import [storm.state MapState])
  (:import [storm.state.hdfs HDFSBackingStore])
  (:import [java.math BigInteger])
  (:use [backtype.storm.testing :only [with-local-tmp]])
  (:use [backtype.storm.util :only [letlocals]])
  (:use [clojure test]))

(defn bint [v]
  (BigInteger. (str v)))

(deftest test-map-state
  (with-local-tmp [tmpdir]
    (letlocals
      (bind m (MapState. (HDFSBackingStore. tmpdir)))
      (.put m "a" 1)
      (is (= 1 (.get m "a")))
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= nil (.get m2 "a")))
      
      (.commit m)
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= 1 (.get m "a")))
      (is (= 1 (.get m2 "a")))
      
      (.put m "a" 2)
      (.put m "b" 3)
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= 1 (.get m2 "a")))

      (.commit m)
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= 2 (.get m "a")))
      (is (= 2 (.get m2 "a")))
      (is (= 3 (.get m "b")))
      (is (= 3 (.get m2 "b")))
      
      (.compact m)
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= 2 (.get m "a")))
      (is (= 2 (.get m2 "a")))
      (is (= 3 (.get m "b")))
      (is (= 3 (.get m2 "b")))
      )))

(deftest test-map-state-versioned
 (with-local-tmp [tmpdir]
    (letlocals
      (bind m (MapState. (HDFSBackingStore. tmpdir)))
      (.put m "a" 1)
      (.commit m (bint 1))
      (is (= 1 (.get m "a")))
      
      (.rollback m)
      (is (= nil (.get m "a")))
      (.put m "a" 1)
      (.put m "b" 2)
      (.commit m (bint 1))
      (.put m "b" 3)
      (.put m "c" 4)
      (.commit m (bint 2))
      (is (= 1 (.get m "a")))
      (is (= 3 (.get m "b")))
      (is (= 4 (.get m "c")))
      (is (= (bint 2) (.getVersion m)))
      
      (.rollback m)
      (is (= 1 (.get m "a")))
      (is (= 2 (.get m "b")))
      (is (= nil (.get m "c")))
      (is (= nil (.getVersion m)))
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= 1 (.get m2 "a")))
      (is (= 3 (.get m2 "b")))
      (is (= 4 (.get m2 "c")))
      (is (= (bint 2) (.getVersion m2)))      
      )))

(deftest test-map-state-initial-rollback
 (with-local-tmp [tmpdir]
    (letlocals
      (bind m (MapState. (HDFSBackingStore. tmpdir)))
      (.rollback m)
      (.put m "a" 1)
      (.commit m (bint 1))
      (is (= 1 (.get m "a")))
      (.rollback m)

      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= nil (.get m "a")))
      (is (= 1 (.get m2 "a")))
      (.put m "a" 2)
      (.commit m (bint 1))
      (is (= 2 (.get m "a")))
      (bind m2 (MapState. (HDFSBackingStore. tmpdir)))
      (is (= 2 (.get m2 "a")))
      )))
