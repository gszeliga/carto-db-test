(ns single-stream
  (require [clojure.core.async :as async :refer [<! >! <!! >!! mult tap timeout chan alt! alts! go thread close! go-loop pipe pipeline]]
           [clojure.java.io :as io]
           [clojure.string :refer [split]]))

(defn stream-lines-from [path]
  (let [c (chan 1024)]
    (go
      (with-open [rdr (io/reader path)]
        (doseq [line (line-seq rdr)]
          (>! c line)))
      (close! c))
    c))

(defn w-counter [channel]
  (go-loop [total 0]
    (if-some [_ (<! channel)]
      (recur (inc total))
      total)))

(defn count-lines [channel npar]
  (let [counters (for [_ (range npar)]
                   (w-counter channel))]
    (async/reduce + 0 (async/merge counters))))

(defn w-aggregate [channel]
  (go-loop [c 0 s 0.0]
    (if-some [n (<! channel)]
      (recur (inc c) (+ n s))
      [s c])))

(defn extract-field-fn [fname channel]
  (let [headers (<!! channel)
        fidx (->> (split headers  #",") (map-indexed vector) (filter (fn [[_ v]] (= v fname))) first first)]
    #(java.lang.Double/parseDouble (nth (split % #",") fidx))))

(defn aggregate-field [fname channel npar]
  (let [as-value-fn (extract-field-fn fname channel)
        fvchan (pipe channel (chan 1024 (map as-value-fn)))
        aggregators (for [_ (range npar)]
                      (w-aggregate fvchan))]
    (go
      (let [[s c] (<! (async/reduce #(apply map + [%1 %2]) [0.0 0] (async/merge aggregators)))]
        (/ s c)))))

(defn process! [path fname npar]
  (let [lines (mult (stream-lines-from path))
        aggregate-tap (tap lines (chan 1024))
        count-tap (tap lines (chan 1024))]
    (<!! (async/reduce (fn [_ v] (println v)) "" (async/merge [(aggregate-field fname aggregate-tap npar)
                                                               (count-lines count-tap npar)])))))
