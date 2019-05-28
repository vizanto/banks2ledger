(ns banks2ledger.ics-nl
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [banks2ledger.util :refer (abs str->decimal)]))

;;
;; International Card Services
;;

(def settlement #{"GEINCASSEERD VORIG SALDO"})

;;;
;;; Beancount/ledger posting data
;;;


(defn create-postings [liability-account-str, conversion-account-str,
                       {:keys [amount currency reference date payee
                               description charge-amount charge-currency regular?]}]
  {:pre [amount date
         (or (nil? charge-amount)  (pos? charge-amount))]}
  (let [posted-account (str liability-account-str ":Posted")

  						settlement?
  						(settlement payee)

        description
        (if regular? description #_else (str/trim (str payee " " description)))

        payee
        (if regular? payee #_else "International Card Services")

        charge-amount
        (if-not (neg? amount) charge-amount #_else (when charge-amount (- charge-amount)))

        forex-posting
        (when charge-amount
          {:commented true, :account :uncategorized
           :amount (str charge-amount " " charge-currency
                        " @@ " (-> amount abs))
           :currency currency})

        from-transfer
        {:account posted-account :amount (- amount), :currency currency}

        to-transfer
        {:account (if-not settlement? :uncategorized #_else liability-account-str)}]

    ; Transaction
    {:date      date
     :flag      (if-not settlement? "!" #_else "*")
     :payee     payee
     :descr     description
     :reference reference
     :metas     (when forex-posting
                  {"charged-amount" (str charge-amount " " charge-currency)})
     :postings  (remove nil?
                 [(if (:amount from-transfer) from-transfer to-transfer)
                  (if (:amount from-transfer) to-transfer from-transfer)
                  forex-posting])}))

;;;
;;; JSON Parsing
;;;

(defn parse-json-transaction
  [liability-account-str, conversion-account-str,
   {:keys [lastFourDigits countryCode transactionDate description billingAmount billingCurrency
   								sourceAmount sourceCurrency merchantCategoryCodeDescription typeOfTransaction
   								batchNr batchSequenceNr typeOfPurchase processingTime indicatorExtraCard embossingName
   								directDebitState mobile loyaltyPoints chargeBackAllowed]}]
  {:pre [(-> billingAmount decimal?)]}
  (create-postings liability-account-str, conversion-account-str,
   {:amount          billingAmount
   	:currency        billingCurrency
    :reference       (str batchNr "_" batchSequenceNr)
    :date            transactionDate
    :payee           description
    :description     (str merchantCategoryCodeDescription " | " typeOfPurchase" | " transactionDate " " processingTime)
    :charge-amount   (when-not (= billingCurrency sourceCurrency) (abs sourceAmount))
    :charge-currency sourceCurrency
    :regular?        chargeBackAllowed}))
