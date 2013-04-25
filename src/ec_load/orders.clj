(ns ec-load.orders
  (:require [clojure.string :as string])
  (:require [clojure.contrib.math :as math])
  (:require [korma.db :as db])
  (:require [korma.core :as sql])
  (:use (incanter core stats))
  (:use ec-load.common)
)

; Database tables
(sql/defentity order_logs)
(sql/defentity order_stats)

(sql/defentity orders
  (sql/has-many order_logs))

; Finders
(defn find-by-id
  "find an order by an order_id"
  ([field id]
    (first (sql/select orders
             (sql/where {field (convert-id id)}))))
  ([field id fields]
    (first (sql/select orders
             (sql/fields fields)
             (sql/where {field (convert-id id)})))))

(defn expired-orders []
  "find all expired orders"
  (sql/select orders
    (sql/where {:expires [<= (sql/sqlfn now)]} )))

(defn get-prices [sel]
  (get-field :orders sel :price))

(defn- sim-xact [orders perc]
  "Simulate trasacting a percentage (by volume) of orders"
  (let [total-vol (reduce + (map #(:vol_remain %) orders))
        to-xact (math/round (* perc total-vol))]
    (if (> to-xact 0)
      (loop [paid 0 vol-remain to-xact orders orders]
        (let [order (first orders)
              order-volume (:vol_remain order)
              vol-offset (- vol-remain order-volume)
              price (:price order)]
          (if (<= vol-offset 0)
            (let [last-order-paid (* price (+ order-volume vol-offset))]
              (divide-prices (+ paid last-order-paid) to-xact))
            (recur (+ paid (* (:vol_remain order) price))
                   (- vol-remain order-volume)
                   (rest orders))))))))

(defn sim-buy [orders]
  (if (> (count orders) 1)
    (let [sorted (sort #(compare (:price %1) (:price %2)) orders)]
      (sim-xact sorted 0.05))))

(defn sim-sell [orders]
  (if (> (count orders) 1)
    (let [sorted (sort #(compare (:price %2) (:price %1)) orders)]
      (sim-xact sorted 0.05))))

(defn max-price [orders]
  (if (> (count orders) 1)
    (reduce max (map #(:price %) orders))))

(defn min-price [orders]
  (if (> (count orders) 1)
    (reduce min (map #(:price %) orders))))

(defn mid-buy-sell [max-buy min-sell]
  (if (and max-buy min-sell)
    (divide-prices (+ max-buy min-sell) 2)))

(defn filter-order-outliers [orders fences]
  (filter (fn [order]
            (let [price (:price order)]
              (and (<= price (last fences))
                   (>= price (first fences)))))
          orders))

(defn flag-outliers [orders fences]
  (let [ids (map #(:order_id %) orders)]
    (sql/update :orders
      (sql/set-fields {:outlier false})
      (sql/where {:order_id [in ids]})
      (sql/where {:price [>= (first fences)]})
      (sql/where {:price [<= (last fences)]}))

    (sql/update :orders
      (sql/set-fields {:outlier true})
      (sql/where {:order_id [in ids]})
      (sql/where
        (or
          {:price [< (first fences)]}
          {:price [> (last fences) ]})))))

(defn weighted-avg [orders price-fences]
  (if (> (count orders) 1)
    (let [stats
           (reduce (fn [stats order]
             (let [vol (:vol_remain order)
                   price (:price order)]
               {:total-vol
                  (+ vol (:total-vol stats))
                :total
                  (+ (:total stats)
                    (* (:vol_remain order)
                       (:price order)))}))
                {:total-vol 0 :total 0}
           (filter-order-outliers orders price-fences))]
      (if (> (:total stats) 0)
        (divide-prices (:total stats) (:total-vol stats))))))


(defn build-orders-stats [orders region-id]
  (let [prices (map #(:price %) orders)
        buys (filter #(= (:bid %1) true) orders)
        sells (filter #(= (:bid %1) false) orders)
        max-buy (max-price buys)
        min-sell (min-price sells)
        price-fences (fences prices :outer)]
    (if-not (nil? region-id)
      (flag-outliers orders price-fences))
    (hash-map :ts (clj-time.coerce/to-timestamp (clj-time.core/now)),
              :type_id (:type_id (first orders)),
              :region_id region-id,
              :median (round-places (median prices) 2),
              :max_buy max-buy,
              :min_sell min-sell,
              :mid_buy_sell (mid-buy-sell max-buy min-sell)
              :sim_buy (sim-buy sells),
              :sim_sell (sim-sell buys),
              :weighted_avg (weighted-avg orders price-fences)
              :buy_vol (reduce + (map #(:vol_remain %) buys)),
              :sell_vol (reduce + (map #(:vol_remain %) sells)))))

(defn find-order [id]
  "find a specific order"
  (find-by-id :order_id id))

(defn find-orders [sel]
  (sql/select orders
    (sql/where sel)))

(defn find-last-stat [type-id region-id]
  (first
    (sql/select order_stats
      (sql/where {:type_id type-id :region_id region-id})
      (sql/order :ts :DESC)
      (sql/limit 1))))

(defn last-stat-too-recent? [type-id region-id]
  "is there a stat for this type and region, is it less than an hour old"
  (let [last-stat (find-last-stat type-id region-id)]
    (if (and (not (nil? last-stat))
             (not (nil? (:ts last-stat))))

      (not (over-hour-ago?
             (clj-time.coerce/from-sql-date (:ts last-stat)))))))

(defn get-orders-by-type-region [type-id region-id]
  (let [sel
        (if (nil? region-id)
          {:type_id type-id}
          {:type_id type-id :region_id region-id})]

  (let [start-ts
        (clj-time.coerce/to-timestamp
          (clj-time.core/minus (clj-time.core/now) (clj-time.core/days 5)))]
    (sql/select orders
      (sql/fields :order_id :type_id :region_id :price :bid :vol_remain)
      (sql/where sel)
      (sql/where {:expires [> (sql/sqlfn now)]})
      (sql/where {:reported_ts [> start-ts]})))))

(defn- save-stats [stats]
  (sql/insert order_stats
    (sql/values stats)))

(defn build-all-stats []
  (dorun (pmap
    (fn [pair]
      (let [orders (apply get-orders-by-type-region pair)
            region-id (last pair)]
        (if (and
              (> (count orders) 0)
              (not (apply last-stat-too-recent? pair)))
          (save-stats
            (build-orders-stats orders region-id)))))

    (for [type-id (get-market-type-ids)
          region-id (conj (get-region-ids) nil)]
           [type-id region-id]))))

(defn orders-for-type [type-id]
  "find all orders for a type id"
  (sql/select orders
    (sql/where {:type_id type-id})
    (sql/order :reported_ts :desc)))

(defn last-order []
  "find the last reported order"
  (first (sql/select orders
              (sql/order :reported_ts :desc)
              (sql/limit 1))))

(defn last-log [order]
  "find the last log for an order"
  (sql/select order_logs
    (sql/where {:order_id (:order_id order)})
    (sql/order :reported_ts :desc)
    (sql/limit 1)))

; Operations
(defn order-logs [order]
  (sql/select order_logs
    (sql/where {:order_id (:order_id order)})))

(defn delete-order [order]
  (db/transaction
    (sql/delete orders
      (sql/where {:order_id (:order_id order)}))
    (sql/delete order_logs
      (sql/where {:order_id (:order_id order)}))))

; Parsers
(defn parse-issued [date-str]
  "parse an issued timestamp: 2012-09-08 11:08:35"
  (clj-time.format/parse issued-formatter (string/replace date-str #"(\d)\s(\d)" "$1T$2")))

(defn parse-reported [date-str]
  "parse a reported timestamp: 2012-09-09 04:22:41.722360"
  (let [date-str (string/replace date-str #"(\d)\s(\d)" "$1T$2")]
    (if (re-find #"\.\d+$" date-str)
      (clj-time.format/parse reported-formatter date-str)
      (clj-time.format/parse issued-formatter date-str))))

(defn parse-duration [value]
  "parse a duration string: '90 days, 0:00:00'"
  (if (= value "0:00:00")
    0
    (Integer. (first (string/split value #"\s")))))

; Validations
(defn reported-before-issued? [reported issued]
  "orders cannot be reported before they were created"
  (= (compare reported issued) -1))

(defn valid-order-id? [order]
  (let [order-id (:order_id order)]
    (> order-id 1000)))

(defn valid-type-id? [order]
  (let [type-id (:type_id order)]
    (>= type-id 1)))

(defn valid-range? [order]
  (let [order-range (:range order)]
    (and (>= order-range -1)
         (<= order-range 65535))))

(defn valid-region-id? [order]
  (let [region-id (:region_id order)]
    (and (>= region-id 10000000)
         (< region-id 20000000))))

(defn valid-system-id? [order]
  (let [system-id (:solar_system_id order)]
    (and (>= system-id 30000000)
         (< system-id 40000000))))

(defn valid-price? [order]
  "orders with empty prices, or those under 0.25 are not valid"
  (let [price (:price order)]
    (and (not (nil? price))
         (>= price 0.25))))

(defn valid-volume? [order]
  "orders are not valid if they have a remaining volume greater than start"
  (<= (:vol_remain order)
      (:vol_enter order)))

(defn valid-issued? [order]
  "is the issued date in an order valid?"
  (let [issued (:issued order)]
    (and
      (clj-time.core/after? (:reported_ts order) issued)
      (over-hour-ahead? issued))))

(defn valid-duration? [order]
  "is an order's duration valid?"
  (let [duration (:duration order)]
    (and (<= duration 365)
         (not (in? (range 91 364) duration)))))

(defn valid-order? [order]
  "is an order valid?"
  (and (valid-price? order)
       (valid-duration? order)
       (valid-range? order)
       (valid-order-id? order)
       (valid-type-id? order)
       (valid-system-id? order)
       (valid-region-id? order)
       (valid-issued? order)
       (valid-volume? order)))

(defn remap-value [k v]
  "remap string values from the csv row to appropiate types"
  (cond
    (in? [:range :min_vol :vol_enter :vol_remain] k) (Long. v)
    (in? [:solar_system_id :region_id, :station_id] k) (Long. v)
    (in? [:order_id :type_id :range] k) (Long. v)
    (= :price k) (round-places (Double. v) 2)
    (= :bid k) (not= v "0")
    (= :reported_ts k) (parse-reported v)
    (= :issued k) (parse-issued v)
    (= :duration k) (parse-duration v)
    :else v))

(defn get-expiration [order]
  "get the expiration date for an order (issued + duration days)"
  (clj-time.core/plus (:issued order)
                      (clj-time.core/days (:duration order))))

(defn remap-values [data]
  (let [remapped (rebuild data remap-value)]
    (merge remapped
      {:gen_name "Eve Market Dump"
       :expires (get-expiration remapped)})))

; add gen_source, etc
(defn build-update [order]
  (rebuild (dissoc order :reported_by) sql-timestamps))

; Persistance
(defn add-order [order]
  (sql/insert orders
    (sql/values (build-update order))))

(defn volume-changed? [order found-order]
  (not= (get order :vol_remain)
          (get found-order :vol_remain)))

(defn price-changed? [order found-order]
  (not (== (get order :price)
           (get found-order :price))))

(defn log-exists? [log-data]
  (not= 0
    (:cnt
      (first
        (sql/select order_logs
          (sql/fields [ "count(*)" :cnt])
          (sql/where {:order_id (:order_id log-data)})
          (sql/where {:reported_ts (:reported_ts log-data)}))))))

(defn needs-log? [order found-order]
  "does an order update require a log?"
  (and
    (or (volume-changed? order found-order)
        (price-changed? order found-order))
    (not (log-exists? found-order))))

(defn build-log [order]
  (select-keys order [:order_id, :price, :vol_remain,
                      :reported_ts, :gen_name, :gen_version]))

(defn create-log [order]
  (sql/insert order_logs
    (sql/values (build-log order))))

(defn update-order [order found-order]
  "update an order with new data, creating a log if necessary"
  (let [order-update (build-update order)]
    (db/transaction
      (sql/update orders
        (sql/set-fields order-update)
        (sql/where {:order_id (:order_id order)}))
      (if (needs-log? order-update found-order)
        (create-log found-order)))))


(defn order-newer? [order found-order]
  "compare two orders and determine if the first is newer than the second"
  (let [order-date (:reported_ts order)
        found-date (clj-time.coerce/from-sql-date
                     (:reported_ts found-order ))]
    (clj-time.core/after? order-date found-date)))

(defn sync-order [order]
  (try
    (let [remapped (remap-values order)
          found-order (find-order (:order_id order))]
      (if (valid-order? remapped)
        (if found-order
          (if (order-newer? remapped found-order)
              (update-order remapped found-order))
          (add-order remapped))))
    (catch Exception e (println (str "Exception: " e)))))

