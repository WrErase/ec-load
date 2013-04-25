(ns ec-load.order-stats
  (:require [clojure.contrib.math :as math])
  (:require [korma.core :as sql])
  (:use (incanter core stats))
  (:use ec-load.common)
)

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
