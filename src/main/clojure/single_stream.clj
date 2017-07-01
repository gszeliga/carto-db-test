(ns single-stream
  (require [clojure.core.async :as async]
           [clojure.java.io :as io]))

(defn stream-lines []
  (let [c (async/chan)]
    (async/thread
      (with-open [rdr (io/reader "https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv")]
        (doseq [line (line-seq rdr)]
          (async/>!! c line))))
    c))

(defn spit-it-out [channel]
  (dotimes [_ 8]
    (async/go (while true (println (async/<! channel))))))

(defn partial-counter [channel]
  (async/go-loop [total 0]
    (if-some [_ (async/<! channel)]
      (recur (inc total))
      total)))

(defn count-lines [channel]
  (for [_ (range 8)]
    (partial-counter channel)))

(defn go! []
  (let [c (async/chan)]
    (async/go
      (async/>! c "foo")
      (async/>! c "bar")
      (async/>! c "baz")
      (async/close! c))
    (println (async/<!! (async/reduce + 0 (async/merge (count-lines (stream-lines))))))))
    ;(let [[v c] (async/alts!! (count-lines c))]
    ;  (println v))))
