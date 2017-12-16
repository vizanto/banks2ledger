(ns banks2ledger.knab-nl
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [hasch.core :refer (uuid)]
            [banks2ledger.util :refer (abs)]))

;;
;; KNAB
;;

(def internal-payees #{"HARTELIJK BEDANKT VOOR UW BETALING"})

;;;
;;; Beancount/ledger posting data
;;;


(defn create-postings [asset-account-str, conversion-account-str,
                       {:keys [account reference amount currency date datetime location
                               payment-kind posting-date payee payee-account description auth-ID
                               creditor-ID address forex-rate charge-currency charge-amount
                               conversion-fpct]}]
  {:pre [amount date
         (or (nil? conversion-fpct) (pos? conversion-fpct))]}
  (let [posted-now?    (= date posting-date)
        ;authed-account (str asset-account-str ":Authed" (when card-account ":") card-account)
        ;posted-account (str asset-account-str ":Posted" (when card-account ":") card-account)
        payee-account
        (str/trim payee-account)
        
        conversion-fee
        (some-> conversion-fpct (* 0.01) (* amount) abs)

        charge-amount
        (if (and conversion-fee (nil? charge-amount))
          (-> amount abs (- conversion-fee) (/ (or forex-rate 1)))
        ;else
          charge-amount)

        forex-posting
        (when forex-rate
          {:commented true, :account :uncategorized
           :amount (str charge-amount " " charge-currency
                        " @@ " (-> amount abs (- (or conversion-fee 0))))
           :currency currency})

        fee-posting
        (when conversion-fee
          {:commented true, :account conversion-account-str
           :amount conversion-fee, :currency currency})

        from-transfer
        {:account asset-account-str, :amount (when (pos? amount) amount), :currency currency}

        to-transfer
        {:account :uncategorized, :amount (when (neg? amount) (- amount)), :currency currency}

        metas
        (->>
          {"conversion-fee" (when-not (nil?   conversion-fee) (str conversion-fee " " currency))
           "charged-amount" (when-not (nil?   charge-amount)  (str charge-amount " " charge-currency))
           "account"        (when-not (empty? account)        (str \" account \"))
           "datetime"       (when-not (empty? datetime)       (str \" datetime \"))
           "location"       (when-not (empty? location)       (str \" location \"))
           "payment-kind"   (when-not (empty? payment-kind)   (str \" payment-kind  \"))
           "payee-account"  (when-not (empty? payee-account)  (str \" payee-account \"))
           "auth-ID"        (when-not (empty? auth-ID)        (str \" auth-ID \"))
           "creditor-ID"    (when-not (empty? creditor-ID)    (str \" creditor-ID \"))
           "address"        (when-not (empty? address)        (str \" address \"))}
          (remove (fn [[k v]] (empty? v)))
          (into {}))

        transaction
        {:date      date
         :flag      "!"
         :payee     payee
         :descr     description
         :reference reference
         :metas     metas
         :postings  (remove nil?
                     [(if (:amount from-transfer) from-transfer to-transfer)
                      (if (:amount from-transfer) to-transfer from-transfer)
                      forex-posting
                      fee-posting])}

        posting nil
        #_(when-not posted-now?
          {:date posting-date, :flag "*", :payee "", :descr "", :reference reference
           :postings [{:account authed-account, :amount amount, :currency currency}
                      {:account posted-account}]})]
    (if posted-now?
      [transaction]
     ;else
      [transaction #_posting])))

;;;
;;; CSV Parsing
;;;

(defn str->decimal [amount]
 (-> (str/trim amount)
       (str/replace #"[.]" "")
       (str/replace #"," ".")
       (BigDecimal.)))

(defn normalize-date [date-str]
  (str "20" (subs date-str 8 10) "-" (subs date-str 3 5) "-" (subs date-str 0 2)))

(def forex-re
  #"(.*) ([0-9-]{10} [0-9:]{4,5})\s+Pas: [0-9]{4,} ([A-Z]{3,})\s*([0-9.,]+)?\s*Koers: [A-Z/]+: ([0-9.,]+)\s+Kosten: ([0-9.,]+)%.*")

(defn notes->forex [notes]
  (let [[location datetime currency amount rate fee-pct :as regex]
        (next (re-matches forex-re notes))]
    (if currency
      [location datetime currency
       (when amount (str->decimal amount)) (str->decimal rate) (str->decimal fee-pct)])))

(defn parse-csv-columns [asset-account-str, conversion-account-str,
                         [rekeningnummer, transactiedatum, valutacode, creditdebet, bedrag,
                          tegenrekeningnummer, tegenrekeninghouder, valutadatum, betaalwijze,
                          omschrijving, typeBetaling, machtigingsnummer, incassantID, adres
                          :as csv]]
  (let [[location datetime charge-currency charge-amount forex-rate conversion-fpct]
        (notes->forex omschrijving)]
    (create-postings asset-account-str, conversion-account-str,
     {:account         rekeningnummer
      :reference       (str (uuid csv))
      :amount          (({"C" + "D" -} creditdebet) (str->decimal bedrag))
      :currency        valutacode
      :date            (normalize-date transactiedatum)
      :datetime        datetime
      :location        location
      :payment-kind    betaalwijze
      :posting-date    (normalize-date valutadatum)
      :payee           tegenrekeninghouder
      :payee-account   tegenrekeningnummer
      :description     omschrijving
      :auth-ID         machtigingsnummer
      :creditor-ID     incassantID
      :address         adres
      :forex-rate      forex-rate
      :charge-currency charge-currency
      :charge-amount   charge-amount
      :conversion-fpct conversion-fpct
      })))

