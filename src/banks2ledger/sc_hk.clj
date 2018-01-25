(ns banks2ledger.sc-hk
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [hasch.core :refer (uuid)]
            [banks2ledger.util :refer (abs)]))

;;
;; Standard Chartered (Hong Kong)
;;

(def sc-regex
  #"FEE INV#[0-9]{8,} .*| .*? NINT  [0-9]{4,}")

(defn create-postings [asset-account-str, conversion-account-str,
                       {:keys [account reference amount currency date datetime location
                               account-type posting-date description forex-rate]}]
  {:pre [amount date]}
  (let [posted-now?    (= date posting-date)
        ;authed-account (str asset-account-str ":Authed" (when card-account ":") card-account)
        ;posted-account (str asset-account-str ":Posted" (when card-account ":") card-account)
        asset-account-str
        (str asset-account-str ({"SA" ":Savings", "CA" ":Current"} account-type))
        
        ; conversion-fee
        ; (some-> conversion-fpct (* 0.01) (* amount) abs)

        ; charge-amount
        ; (if (and conversion-fee (nil? charge-amount))
        ;   (-> amount abs (- conversion-fee) (/ (or forex-rate 1)))
        ; ;else
        ;   charge-amount)

        ; forex-posting
        ; (when forex-rate
        ;   {:commented true, :account :uncategorized
        ;    :amount (str charge-amount " " charge-currency
        ;                 " @@ " (-> amount abs (- (or conversion-fee 0))))
        ;    :currency currency})

        ; fee-posting
        ; (when conversion-fee
        ;   {:commented true, :account conversion-account-str
        ;    :amount conversion-fee, :currency currency})
        
        payee
        (when (re-matches sc-regex description) "Standard Chartered")

        from-transfer
        {:account asset-account-str, :amount (when (pos? amount) amount), :currency currency}

        to-transfer
        {:account :uncategorized, :amount (when (neg? amount) (- amount)), :currency currency}

        metas
        (->>
          {;"conversion-fee" (when-not (nil?   conversion-fee) (str conversion-fee " " currency))
           ;"charged-amount" (when-not (nil?   charge-amount)  (str charge-amount " " charge-currency))
           "account"        (when-not (empty? account)        (str \" account \"))
           "datetime"       (when-not (empty? datetime)       (str \" datetime \"))}
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
                      ; forex-posting
                      ; fee-posting
                      ])}

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
     (str/replace #"[,]" "")
     (BigDecimal.)))

(defn normalize-date [date-str]
  (str (subs date-str 6) "-" (subs date-str 3 5) "-" (subs date-str 0 2)))

#_(def forex-re
  #"(.*) ([0-9-]{10} [0-9:]{4,5})\s+Pas: [0-9]{4,} ([A-Z]{3,})\s*([0-9.,]+)?\s*Koers: [A-Z/]+: ([0-9.,]+)\s+Kosten: ([0-9.,]+)%.*")

#_(defn notes->forex [notes]
  (let [[location datetime currency amount rate fee-pct :as regex]
        (next (re-matches forex-re notes))]
    (if currency
      [location datetime currency
       (when amount (str->decimal amount)) (str->decimal rate) (str->decimal fee-pct)])))

(defn parse-csv-columns [asset-account-str, conversion-account-str,
                         [company bank country account-number account-name branch account-type
                          currency as-at-date opening-available-balance closing-available-balance
                          opening-ledger-balance closing-ledger-balance value-date cheque-number
                          debit-credit transaction-amount account-owner-reference bank-code
                          exchange-rate information-to-account-owner originating-branch
                          processing-branch processing-time reversal-indicator transaction-date
                          transaction-details sender-details channel-id
                          :as csv]]
  (let [#_[location datetime charge-currency charge-amount forex-rate conversion-fpct]
        #_(notes->forex omschrijving)]
    (create-postings asset-account-str, conversion-account-str,
     {:account         account-number
      :reference       (str (uuid csv))
      :amount          (({"C" + "D" -} debit-credit) (str->decimal transaction-amount))
      :currency        currency
      :date            (normalize-date transaction-date)
      :datetime        processing-time
      :account-type    account-type
      :posting-date    (normalize-date value-date)
      :description     (str/trim transaction-details)
      ; :forex-rate      forex-rate
      ; :charge-currency charge-currency
      ; :charge-amount   charge-amount
      ; :conversion-fpct conversion-fpct
      })))

