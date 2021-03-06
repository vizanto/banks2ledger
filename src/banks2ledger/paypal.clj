(ns banks2ledger.paypal
  (:require [clojure.string :as str]
            [clojure.core.reducers :as reducers]
            [clj-time.core :as t]))

;;
;; Implements support for: PayPal -> Reports -> Statements -> Monthly/Custom CSV
;;
;; https://business.paypal.com/merchantdata/reportHome
;;

(defn create-postings [asset-account-str, conversion-account-str,
                       {:keys [date datetime description currency amount gross-amount fee reference
                               name email-address invoice-id reference-txn-id]}]
  {:pre [amount date]}
  (let [from-transfer
        {:account asset-account-str, :amount amount, :currency currency}

        to-transfer
        {:account :uncategorized, :amount (when-not (= amount gross-amount) (- gross-amount))
         :currency currency}

        payee
        (if (empty? email-address) name #_else (str name " <" email-address ">"))

        metas
        (->>
          {"datetime"         (str \" datetime \")
           "transaction-id"   (str \" reference \")
           "reference-txn-id" (when-not (empty? reference-txn-id) (str \" reference-txn-id \"))
           "invoice-id"       (when-not (empty? invoice-id)       (str \" invoice-id \"))}
          (remove (fn [[k v]] (empty? v)))
          (into {}))

        fee-posting
        (when (or (> fee 0) (< fee 0))
          {:account conversion-account-str
           :amount fee, :currency currency})

        transaction
        {:date      date
         :flag      "!"
         :payee     payee
         :descr     description
         :reference reference
         :links     (when-not (empty? reference-txn-id) [reference-txn-id])
         :metas     metas
         :postings  (remove nil? [from-transfer, fee-posting, to-transfer])
        }]
    [transaction]))

;;;
;;; CSV Parsing
;;;

(defn str->decimal [amount]
 (-> (str/trim amount)
     (str/replace #"[,]" "")
     (BigDecimal.)))

(defn parse-csv-columns [asset-account-str, conversion-account-str,
                         [date time time-zone description currency gross fee net balance
                          transaction-id from-email-address name bank-name bank-account
                          postage-and-packaging-amount vat invoice-id reference-txn-id
                          :as csv]]
  (let [[month day year] (map #(Integer/parseInt %) (str/split date #"/"))
        [hh mm ss]       (map #(Integer/parseInt %) (str/split time #":"))
        date-str
        (str year "-" (when (< month 10) "0") month, "-" (when (< day 10) "0") day)
        datetime
        (t/from-time-zone (t/date-time year month day hh mm ss) (t/time-zone-for-id time-zone))]
    (create-postings asset-account-str, conversion-account-str,
     {:date             date-str
      :datetime         datetime
      :description      (.intern (str description))
      :currency         currency
      :amount           (str->decimal net)
      :gross-amount     (str->decimal gross)
      :fee              (- (str->decimal fee))
      :reference        transaction-id
      :name             name
      :email-address    from-email-address
      :invoice-id       invoice-id
      :reference-txn-id reference-txn-id})))


;;; Fold currency conversion postings into 1 entry

(defn combine-currency-conversions [txn1 txn2]
  (let [[posting1 posting2 :as postings]
        (remove #(-> % :account (= :uncategorized))
                (concat (:postings txn1) (:postings txn2)))
        positive
        (if (-> posting2 :amount pos?) posting2 #_else posting1)
        negative
        (if (-> posting1 :amount neg?) posting1 #_else posting2)
        _
        (assert (= 2 (count postings)) (str "Expected a total of 2 postings for currency conversion, got " (vec postings)))
        _
        (assert (neg? (:amount negative)) (str "Expected " negative " conversion transaction to be negative"))
        _
        (assert (pos? (:amount positive)) (str "Expected " positive " conversion transaction to be positive"))

        posting1
        (assoc negative :amount (str (:amount positive) " " (:currency positive)
                                     " @@ " (- (:amount negative))))
        posting2
        (assoc positive :amount (:amount negative) :currency (:currency negative))

        links
        (vec (apply conj #{(:reference txn2)} (concat (:links txn1) (:links txn2))))]
    ;(binding [*out* *err*] (prn txn1 txn2))
    (assoc txn1
           :flag "*"
           :payee "PayPal"
           :links links
           :postings [posting1 posting2])))

(defrecord CombineState [entry-map entries])

(defn combine-currency-conversion-postings
  ([] '())
  ([state {:keys [flag descr] :as candidate}]
    (let [to-combine (if (instance? CombineState state) (:entry-map state) #_else {})
          entries    (if (instance? CombineState state) (:entries   state) #_else state)]
      (if-not (and (= "!" flag) (= "General Currency Conversion" descr))
        (->CombineState to-combine (conj entries candidate))
      ;else
        (let [reference-txn-id (get (:metas candidate) "reference-txn-id")
              earlier-entry    (to-combine reference-txn-id)]
          ;(binding [*out* *err*] (prn reference-txn-id earlier-entry candidate))
          (if earlier-entry
            (->CombineState to-combine
                            (conj entries (combine-currency-conversions earlier-entry candidate)))
          ;else
            (->CombineState (assoc to-combine reference-txn-id candidate)
                            entries)))))))

(defn fold-related-txns [entries]
  (->> entries
       (reducers/fold combine-currency-conversion-postings)
       :entries
       reverse))
