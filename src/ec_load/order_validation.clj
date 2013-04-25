(ns ec-load.order-validation
  (:use ec-load.common)
)

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
