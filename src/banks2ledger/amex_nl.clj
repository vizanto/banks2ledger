(ns banks2ledger.amex-nl
  (:require [clojure.string :as str]))

;;
;; American Express
;;

(def conversion-account-str "Expenses:Bank-fees:NL:AMEX:Conversion")

;;;
;;; Beancount/ledger posting data
;;;


(defn create-postings [liability-account-str,
                       {:keys [amount reference date posting-date payee description
                               liability-account charge-amount charge-currency conversion-fee]}]
  (let [posted-now?  (= date posting-date)
        account      (str liability-account (if posted-now? "Posted" #_else "Authed"))

        fee-posting
        (when conversion-fee
          {:commented true, :account conversion-account-str
           :amount conversion-fee, :currency "EUR"})

        forex-posting
        (when charge-amount
          {:commented true, :account :uncategorized
           :amount (str (when (neg? amount) "-") charge-amount " " charge-currency
                        " @@ " (- amount conversion-fee))
           :currency "EUR"})

        from-transfer
        {:account account}

        to-transfer
        {:account :uncategorized, :amount amount, :currency "EUR"}

        transaction
        {:date      date
         :flag      "!"
         :payee     payee
         :descr     (when posted-now? description)
         :reference reference
         :metas     (when forex-posting
                      {"conversion-fee" (str conversion-fee " EUR")
                       "charged-amount" (str charge-amount " " charge-currency)})
         :postings  (remove nil?
                     [(if (:amount from-transfer) from-transfer to-transfer)
                      (if (:amount from-transfer) to-transfer from-transfer)
                      forex-posting
                      fee-posting])}

         posting
         (when-not posted-now?
           {:date posting-date, :flag "*", :payee "", :descr description, :reference reference
            :postings [{:account (str liability-account-str "Authed")
                        :amount amount, :currency "EUR"}
                       {:account (str liability-account-str "Posted")}]})]
    (if posted-now?
      [(assoc transaction :descr description)]
     ;else
      [transaction posting])))

;;;
;;; CSV Parsing
;;;

(defn normalize-date [date-str]
  (str "20" (subs date-str 6 8) "-" (subs date-str 3 5) "-" (subs date-str 0 2)))

(defn notes->posting-date [notes]
  (->> notes
       (re-seq #"verwerkt\s+([-0-9]{8})")
       first second normalize-date))

(defn notes->description [notes]
  (->> notes
       (re-seq #"Datum.*")
       first))

(defn notes->forex [notes]
  (let [[amount currency fee :as regex]
        (next (re-matches #"([0-9.]+)\s+([A-Z]{3,})\s+COMM\.FEE\s+([0-9.]+).*" notes))]
    (if amount
      [(BigDecimal. amount) currency (BigDecimal. fee)])))

(defn parse-csv-columns [liability-account-str, [date reference amount payee notes :as csv]]
  (let [[charge-amount charge-currency conversion-fee]
        (notes->forex notes)]
    (create-postings liability-account-str,
     {:amount          (BigDecimal. (str/replace (str/trim amount) #"," "."))
      :reference       (-> reference (str/replace #"Reference: " "") (str/replace #"\s" "-"))
      :date            (normalize-date date)
      :posting-date    (notes->posting-date notes)
      :payee           payee
      :description     (notes->description notes)
      :charge-amount   charge-amount
      :charge-currency charge-currency
      :conversion-fee  conversion-fee})))
