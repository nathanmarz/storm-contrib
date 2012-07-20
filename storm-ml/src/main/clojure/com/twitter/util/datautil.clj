(ns com.twitter.Datautil
  (:require [clojure.string :as sstring])
  (:gen-class))

(defn parse-multiple-to-double
  ""
  [& args]
  (map #(Double/parseDouble %) args))

(def load-dataset
  (let [data-text (map #(sstring/split % #"\t")
                       (sstring/split-lines (slurp "testSet.txt")))]
    (map #(apply parse-multiple-to-double %) data-text)))

(def array-dataset
  (into-array (map (partial into-array Double/TYPE) load-dataset)))