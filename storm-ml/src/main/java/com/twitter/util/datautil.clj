(ns com.twitter.util.DataUtil
  (:gen-class))

(defn parse-multiple-to-float
  ""
  [& args]
  (map #(Double/parseDouble %) args))

(defn load-dataset
  []
  (let [data-text (map #(sutils/split % #"\t")
                       (sutils/split-lines (slurp "testSet.txt")))]
    (map #(apply parse-multiple-to-float %) data-text))