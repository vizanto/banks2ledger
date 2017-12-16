(ns banks2ledger.util)

(defn abs [n] (if (neg? n) (- n) #_else n))

(defn str->decimal [amount]
  (-> (str/trim amount)
      (str/replace #"[.,](?=[0-9]{3})" "")
      (str/replace #",(?=[0-9]{2}$)" ".")
      (BigDecimal.)))
