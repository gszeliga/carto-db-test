(ns single-stream
  (require [clojure.core.async :as async :refer [<! >! <!! >!! mult tap timeout chan alt! alts! go thread close! go-loop pipe pipeline]]
           [clojure.java.io :as io]
           [clojure.string :refer [split]]))

(defn stream-lines []
  (let [c (chan)]
    (go
      (with-open [rdr (io/reader "file:///home/gszeliga/development/workspace/carto-db-test/yellow_tripdata_2016-01.csv")]
        (doseq [line (line-seq rdr)]
          (>! c line)))
      (close! c))
    c))

(defn partial-counter [channel]
  (go-loop [total 0]
    (if-some [_ (<! channel)]
      (recur (inc total))
      total)))

(defn count-lines [channel]
  (let [counters (for [_ (range 8)]
                   (partial-counter channel))]
    (async/reduce + 0 (async/merge counters))))

(defn partial-aggregate [channel]
  (go-loop [c 0 s 0.0]
    (if-some [n (<! channel)]
      (recur (inc c) (+ n s))
      [s c])))

(defn field-pos! [fname channel]
  (let [headers (<!! channel)]
    (->> (split headers  #",") (map-indexed vector) (filter (fn [[idx v]] (= v fname))) first first)))

(defn aggregate-field [fname channel]
  (let [fpos (field-pos! fname channel)
        get-field-fn #(java.lang.Double/parseDouble (nth (split % #",") fpos))
        nchan (pipe channel (chan 1024 (map get-field-fn)))
        aggregators (for [_ (range 8)]
                      (partial-aggregate nchan))]

    (go
      (let [[s c] (<! (async/reduce #(apply map + [%1 %2]) [0.0 0] (async/merge aggregators)))]
        (/ s c)))))

(defn go! []
  (let [lines (mult (stream-lines))
        aggregate-tap (tap lines (chan 10))
        count-tap (tap lines (chan 10))]
    (<!! (async/reduce (fn [_ v] (println v)) "" (async/merge [(aggregate-field "tip_amount" aggregate-tap)
                                                               (count-lines count-tap)])))))
