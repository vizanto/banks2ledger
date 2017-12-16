(ns banks2ledger.util
  (:require [clojure.string :as str]))

(defn abs [n] (if (neg? n) (- n) #_else n))

(defn str->decimal [amount]
  (try
   (-> (str/trim amount)
       (str/replace #"[.,](?=[0-9]{3}(?:[,.]|$))" "")
       (str/replace #",(?=[0-9]{1,2}(?:[^0-9]|$))" ".")
       (BigDecimal.))
   (catch java.lang.NumberFormatException e
    (binding [*out* *err*] (prn 'amount '= amount))
    (throw e))))
