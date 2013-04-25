(ns ec-load.core
  (:require clj-time.core
            clj-time.coerce
            [clojure.contrib.math :as math]
            [korma.db :as db]
            [clojure.java.io :as io :only reader]
            [clojure-csv.core :as csv])
  (:use [ec-load.orders])
  (:use [ec-load.materials])
  (:use [ec-load.common])
  (:use [clojure.contrib.seq :only (separate)])
  (:gen-class))

(db/defdb dev (db/postgres {:db "eveyl_development"
                            :port 5433
                            :user "eveyl"
                            :password "eveyl"}))

(def counter (atom 0))
(def line-number (atom 0))
(def last-ts (atom (clj-time.core/now)))

(defn row-rate [row-count start-ts]
  (math/round
    (* (float (/ row-count (ts-offset @last-ts))) 1000)))

(defn parse-row [row]
  (let [v (first (csv/parse-csv row))]
     (zipmap [:order_id :region_id :solar_system_id :station_id
              :type_id :bid :price :min_vol :vol_remain
              :vol_enter :issued :duration :range :reported_by
              :reported_ts] v)))

(defn validate-header [filename]
  "verify the csv header fields match the expected ones"
  (with-open [file (io/reader filename :encoding "ISO-8859-1")]
    (let [row (first (line-seq file))]
      (if
        (not= (first (csv/parse-csv row))
          ["orderid" "regionid" "systemid" "stationid" "typeid" "bid" "price"
           "minvolume" "volremain" "volenter" "issued" "duration" "range"
           "reportedby","reportedtime"])
        (throw (Throwable. "Invalid Format"))))))


(defn reset-counter-ts []
  (reset! last-ts (clj-time.core/now)))

(defn status-str [rows-count start-ts]
  (str "Line: " @line-number ", Rows: " @counter ", "
    (row-rate rows-count start-ts) " rows/s"))

(defn check-counter [start-ts]
  (swap! counter inc)
  (swap! line-number inc)
  (if (= (mod @counter 25000) 0)
    (do
      (println (status-str 25000 start-ts))
      (reset-counter-ts))))

(defn handle-line [start-ts line]
  (check-counter start-ts)
  (if (> @line-number 1)
    (sync-order (parse-row line))))

(defn on-odd? [row]
  (odd? (Long. (first (first (csv/parse-csv row))))))

(defn reset-counters [start-line]
  (reset! counter 0)
  (reset! line-number start-line)
  (reset-counter-ts))

; File import handlers
(defn import-orders [file start-line]
  (let [start-ts (clj-time.core/now)
        line-handler (partial handle-line start-ts)]
    (dorun (pmap #(dorun (map line-handler %1))
                 (separate on-odd?
                   (drop start-line (line-seq file)))))))

(defn import-stats [file start-line]
  (reduce (fn [stats row]
            (println stats)
            (let [row-data (parse-row row)]
              (if (= (:solar_system_id row-data) "30000142")
                (assoc stats (:order_id row-data) (select-keys row-data [:type_id :price :vol_remain]))
              stats)))
          {}
          (drop start-line (line-seq file))))

(defn parse-file [filename start-line file-fn]
  (validate-header filename)

  (reset-counters start-line)
  (with-open [file (io/reader filename :encoding "ISO-8859-1")]
    (file-fn file start-line)))

(defn do-load
  ([filename]
   (parse-file filename 1 import-orders))
  ([filename start-line]
   (parse-file filename (Integer. start-line) import-orders)))

(defn -main
  [& args]
  (apply do-load args)
  (println "Load complete"))
