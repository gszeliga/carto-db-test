(ns single-stream
  (require [clojure.core.async :as async]
           [clojure.java.io :as io]))

(defn stream-lines [channel]
  (async/go
    (with-open [rdr (io/reader "https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv")]
      (doseq [line (line-seq rdr)]
        (async/>! channel line)))))

(defn spit-it-out [channel]
  (async/go (while true (println (async/<! channel)))))

(defn go! []
  (let [c (async/chan)]
    (stream-lines c)
    (spit-it-out c)))
