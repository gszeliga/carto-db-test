(ns single-stream
  (require [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt! alts! go thread close! go-loop]]
           [clojure.java.io :as io]))

(defn stream-lines []
  (let [c (chan)]
    (go
      (with-open [rdr (io/reader "file:///home/gszeliga/development/workspace/carto-db-test/yellow_tripdata_2016-01.csv")]
        (doseq [line (line-seq rdr)]
          (>! c line)))
      (close! c))
    c))

(defn spit-it-out [channel]
  (dotimes [_ 8]
    (async/go (while true (println (async/<! channel))))))

(defn partial-counter [channel]
  (go-loop [total 0]
    (if-some [_ (<! channel)]
      (recur (inc total))
      total)))

(defn count-lines [channel]
  (let [counters (for [_ (range 8)]
                   (partial-counter channel))]
    (async/reduce + 0 (async/merge counters))))

(defn fan-out [in-chan & out-chans]
  (async/go-loop [msg (<! in-chan)]
    (when (some? msg)
      (doseq [c out-chans]
        (>! c msg))
      (recur (conj in-chan out-chans)))))

(defn go! []
  (let [c (async/chan)]
    (println (<!! (count-lines (stream-lines))))))
