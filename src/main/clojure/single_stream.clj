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

(defn go! []
  (let [c (async/chan)]
    (spit-it-out (stream-lines))))
