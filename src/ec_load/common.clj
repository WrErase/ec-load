(ns ec-load.common
  (:require [clojure.contrib.math :as math]
            [korma.core :as sql])
  (:use (incanter core stats)))

(defn in?
  "true if seq contains elm"
  [seq elm]
  (some #(= elm %) seq))

(def reported-formatter
  (clj-time.format/formatters :date-hour-minute-second-fraction))

(def issued-formatter
  (clj-time.format/formatters :date-hour-minute-second))

(defn round-places [number decimals]
  (let [factor (math/expt 10 decimals)]
    (bigdec (/ (math/round (* factor number)) factor))))

(defn over-hour-ahead? [ts]
  (clj-time.core/after?
    (clj-time.core/now)
    (clj-time.core/plus ts (clj-time.core/hours 1))))

(defn over-hour-ago? [ts]
  (clj-time.core/before?
    (clj-time.core/plus ts (clj-time.core/hours 1))
    (clj-time.core/now)))

(defn over-day-ago? [ts]
  (clj-time.core/before?
    (clj-time.core/plus ts (clj-time.core/hours 24))
    (clj-time.core/now)))

(defn ts-offset [start-ts]
  (- (clj-time.coerce/to-long (clj-time.core/now))
     (clj-time.coerce/to-long start-ts)))

(defn convert-id [id]
  "convert a string id to a long"
  (if (string? id)
    (Long. id)
    id))

(defn sql-timestamps [k v]
  "convert jode DateTimes in the map to sql timestamps"
  (if (= (type v) org.joda.time.DateTime)
    (clj-time.coerce/to-timestamp v)
    v))

(defn rebuild [m f]
  "rebuild a map, applying f to each key and value"
  (into {} (for [[k v] m] [k (f k v)])))

(defn fences [col fence-type]
  (let [quant (quantile col)
        lower-q (nth quant 1)
        upper-q (nth quant 3)
        iqr (- upper-q lower-q)]
    (if (= fence-type :inner)
      (list (- lower-q (* 1.5 iqr))
            (+ upper-q (* 1.5 iqr)))
      (list (- lower-q (* 3 iqr))
            (+ upper-q (* 3 iqr))))))

(defn get-field
  ([table-name field]
    (flatten
      (map vals
        (sql/select table-name
          (sql/fields field)))))

  ([table-name sel field]
    (flatten
      (map vals
        (sql/select table-name
          (sql/fields field)
          (sql/where sel))))))

(defn divide-prices [val1 val2]
  (round-places
    (with-precision 16 (/ val1 val2)) 2))

(def get-region-ids
  (memoize (fn []
    (get-field :regions :region_id))))

(def get-market-type-ids
  (memoize (fn []
    (get-field :types {:market_group_id [not= nil]} :type_id))))

