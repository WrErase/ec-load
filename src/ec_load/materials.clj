(ns ec-load.materials
  (:require [clojure.string :as string])
  (:require [korma.db :as db])
  (:require [korma.core :as sql])
;  (:use (incanter core stats))
  (:use ec-load.common)
  (:use ec-load.orders)
)

(sql/defentity types)
(sql/defentity type_materials)
(sql/defentity type_values)

(def stats
  '(:median :mid_buy_sell :sim_buy :sim_sell :weighted_avg))

(def mats-for-type
  (memoize (fn [type-id]
    (sql/select type_materials
      (sql/fields :material_type_id :quantity)
      (sql/where {:type_id type-id})))))

(defn get-portion-size [type-id]
  (:portion_size
    (first
      (sql/select types
        (sql/fields :portion_size)
        (sql/where {:type_id type-id})))))

(def last-stat-memo
  (memoize find-last-stat))

(defn find-last-value [type-id region-id stat]
  (first
    (sql/select type_values
      (sql/where {:type_id type-id :region_id region-id :stat (name stat)})
      (sql/order :ts :DESC)
      (sql/limit 1))))

(defn last-value-too-recent? [type-id region-id stat]
  "is there a stat for this type and region, is it less than an hour old"
  (let [last-value (find-last-value type-id region-id stat)]
    (if (and (not (nil? last-value))
             (not (nil? (:ts last-value))))

      (not (over-day-ago?
             (clj-time.coerce/from-sql-date (:ts last-value)))))))

(defn- save-values [rows]
  (db/transaction
    (dorun
      (map (fn [row]
             (if (not (nil? row))
               (sql/insert type_values
               (sql/values row))))
       rows))))

(defn get-mat-value [type-id region-id stat]
  (/ (reduce (fn [total material]
              (let [quantity (:quantity material)
                    material-type (:material_type_id material)
                    last-stat (last-stat-memo material-type region-id)]
                (if (nil? last-stat)
                  (throw (Exception.
                           (str "No stat for " material-type " / " region-id)))
                  (+ total (* quantity (get last-stat stat))))))
            0
            (mats-for-type type-id))
    (get-portion-size type-id)))

(defn- build-value-stat [type-id region-id stat value]
  (hash-map :ts (clj-time.coerce/to-timestamp (clj-time.core/now)),
            :type_id type-id,
            :region_id region-id,
            :stat (name stat),
            :value (get (last-stat-memo type-id region-id) stat),
            :mat_value value))

; no stat, map through stats then persist together
(defn- build-save-values [type-id region-id]
  (save-values
    (map
      (fn [stat]
        (if (not (last-value-too-recent? type-id region-id stat))
          (let [value (get-mat-value type-id region-id stat)]
            (if (> value 0)
              (build-value-stat type-id region-id stat value)))))
      stats)))

; Don't use stat, just region/type.  Then run stats as a block and persist together in a xact
(defn build-type-values []
  (dorun
    (pmap
      (fn [triple]
        (try
          (apply build-save-values triple)
          (catch Exception e)))
      (for [type-id (get-market-type-ids)
            region-id (conj (get-region-ids) nil)]
        [type-id region-id]))))
