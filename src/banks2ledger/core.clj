(ns banks2ledger.core
  (:require [banks2ledger.amex-nl :as amex-nl]
            [banks2ledger.ics-nl :as ics-nl]
            [banks2ledger.knab-nl :as knab-nl]
            [banks2ledger.paypal :as paypal]
            [banks2ledger.sc-hk :as sc-hk]
            [banks2ledger.util :refer (abs)]
            [custom.transforms :refer (apply-entry-transforms fold-related-txns)]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [cheshire.parse :refer (*use-bigdecimals?*)])
  (:import java.text.SimpleDateFormat)
  (:gen-class))

;; Bump account's token counter for token
(defn toktab-inc [toktab [account token]]
  (let [acctab0 (or (get toktab account) {})
        cnt (or (get acctab0 token) 0)
        acctab (conj acctab0 [token (inc cnt)])]
    (conj toktab [account acctab])))

;; Update toktab by bumping all accounts in entry for all tokens
(defn toktab-update [toktab {accs :accs toks :toks}]
  (reduce toktab-inc toktab (for [acc accs
                                  tok toks]
                              [acc tok])))

;; Clip string to the part before the given endmark
;; endmark is first arg to allow meaningful use of `partial'
(defn clip-string [^String endmark ^String string]
  (let [end-idx (.indexOf string endmark)]
    (if (= end-idx -1)
      string
      (.substring string 0 end-idx))))

;; Create tokens from string
;; One string may become one or more tokens, returned as a seq
;; - Convert to uppercase
;; - replace dates with degraded forms
;; - Split at '/' ',' and space
(defn tokenize [str]
  (->>
   (-> str
       (str/upper-case)
       (str/replace #"20\d{6}" "YYYYMMDD")
       (str/replace #"\d{2}[-/]\d{2}[-/]\d{2}" "YY-MM-DD")
       (str/split #",|/| "))
   (filter #(> (count %) 0))))

;; P_occur is the occurrence probability of token among
;; all tokens recorded for account.
;; acc-maps is the output of parse-ledger or parse-beancount.
(defn p_occur [acc-maps token account]
  (let [acc-table (get acc-maps account)
        n_t_a (or (get acc-table token) 0)
        n_all (apply + (vals acc-table))]
    (if (= 0 n_all)
      0.0
      (/ (float n_t_a) n_all))))

;; P_belong is the probability that a transaction with
;; token in its descriptor belongs to account.
;; acc-maps is the output of parse-ledger or parse-beancount.
(defn p_belong-% [acc-maps token account]
  (let [p_occ (p_occur acc-maps token account)
        p_occ_all (apply + (map (fn [acc] (p_occur acc-maps token acc))
                                (keys acc-maps)))]
    (if (= 0.0 p_occ_all)
      0.0
      (/ p_occ p_occ_all))))

(def p_belong (memoize p_belong-%))

;; Combine probability values according to the Bayes theorem
(defn bayes* [probs]
  (let [prod-probs (apply * probs)
        prod-comps (apply * (map #(- 1.0 %) probs))
        prod-added (+ prod-probs prod-comps)]
    (if-not (zero? prod-added) (/ prod-probs prod-added)
     #_else 0.01)))

;; Combined p_belong of given tokens for account
(defn p_belong* [acc-maps tokens account]
  (bayes* (map (fn [tok] (p_belong acc-maps tok account))
               tokens)))

;; Return a list of [p_belong, account] pairs in descending order
;; only accounts with nonzero probs are returned
(defn best-accounts-% [acc-maps token]
  (sort-by first >
           (filter #(> (first %) 0.0)
                   (map (fn [acc] [(p_belong acc-maps token acc) acc])
                        (keys acc-maps)))))

(def best-accounts (memoize best-accounts-%))

;; Print a table of combined probs for given tokens
(defn p_table [acc-maps tokens]
  (let [nz-toks (filter #(> (count (best-accounts acc-maps %)) 0) tokens)]
    (sort-by first >
             (filter #(> (first %) 0.0)
                     (map (fn [acc] [(p_belong* acc-maps nz-toks acc) acc])
                          (keys acc-maps))))))

;; Return the most probable counter-accounts for given descr captured for
;; account. This account will be excluded from possible counter-accounts.
(defn account-for-descr [acc-maps descr account]
  (let [tokens (tokenize descr)
        p_tab (p_table acc-maps tokens)]
    (remove #(str/includes? (second %) account) p_tab)))

(defn decide-account [acc-maps descr account unknown-account]
  (let [accs (account-for-descr (first acc-maps) descr account)]
    ;(prn :decide-account accs descr account (= (first (first accs)) (first (second accs))) '= (first (first accs)) (first (second accs)))
    (if (or (empty? accs) ; No match
            (= (first (first accs)) (first (second accs)))) ; First 2 matches have equal weight
      ; Try again with the next token-table, if available, or return unknown-account
      (or (some-> (next acc-maps) (decide-account descr account unknown-account))
          ; No sub-account match found
          unknown-account)
     ;else, we have a winner!
      (second (first accs)))))

;; Science up to this point. From here, only machinery.

;; Create a token counter table from existing ledger entries
(defn ledger->acc-maps [entries]
  (reduce toktab-update {} entries))

;; Create token counter tables for each src-account sub-account
(defn ledger->sub-acc-maps [src-account ledger-entries]
  (->> ledger-entries
   (group-by (fn [e] (->> e :accs (filter #(str/includes? % src-account)) first)))
   (map (fn [[k v]] [k (reduce toktab-update {} v)]))
   (into {})))

;; Parse a ledger entry from string to acc-map
(defn parse-ledger-entry [entry]
  (let [[first-line0 & rest-lines] (clojure.string/split entry #"\n")
        first-line (clip-string "|" first-line0)
        [date descr] (clojure.string/split first-line #" " 2)
        toks (tokenize descr)
        accs (->> rest-lines
                  (map clojure.string/trim)
                  (map (partial clip-string "  ")))]
    {:date date :toks toks :accs accs}))

;; Check if first line of entry is of type: transaction
(defn beancount-transaction? [entry]
  (re-matches #"(?s)[-0-9]{10}\s+(?:[!*]|txn)\s.+" entry))

;; Read and parse a ledger file; return tokenized entries
(defn parse-ledger [filename]
  (->> (clojure.string/split (slurp filename) #"\n\n")
       (map str/trim) ;; remove odd newlines
       (remove empty?)
       (map parse-ledger-entry)))

(def beancount-transaction-re
  #"([-0-9]{10})\s+([!*]|txn)\s+(?:(?:\"([^\"]*?)\"\s+)?\"([^\"]*)\")?(.*)")

(def beancount-tags-re
  #"#[-A-Za-z0-9_/.]+")

(def beancount-links-re
  #"\^[-A-Za-z0-9_/.]+")

;; Parse a beancount entry from string to acc-map
(defn parse-beancount-entry [entry]
  (let [[first-line & rest-lines] (str/split-lines entry)
        [_ date flag payee descr extra] (re-matches beancount-transaction-re first-line)
        tags  (some->> extra (re-seq beancount-tags-re)  (map #(subs % 1)))
        links (some->> extra (re-seq beancount-links-re) (map #(subs % 1)))
        toks  (when-not (= "!" flag) (concat (tokenize (or payee "")) (tokenize (or descr ""))))
        metas (->> rest-lines
                   (map str/trim)
                   (filter #(re-find #"^[a-z].+:.+" %)) ; meta starts lower-case, followed by :
                   (map #(clojure.string/split % #":\s*" 2))
                   (into {}))
        accs  (->> rest-lines
                   (map str/trim)
                   (filter #(re-find #"^[A-Z].+" %)) ; Accounts are always Capitalized
                   (map (partial clip-string " ")))]
    ;(binding [*out* *err*] (prn first-line metas))
    {:date date :toks toks :accs accs :flag flag :descr (when-not (empty? descr) descr)
     :payee payee :tags tags :metas metas :links links}))

;; Read and parse a beancount file; return tokenized entries
(defn parse-beancount [filename]
  (->> (str/split (slurp filename) #"\n(\n|(?=[0-9]))")
       (map str/trim) ;; remove odd newlines
       (remove empty?)
       (filter beancount-transaction?)
       (map parse-beancount-entry)))

;; command line args spec
(def cl-args-spec
  {:ledger-file
   {:opt "-l" :value "ledger.dat"
    :help "Ledger file to get accounts and probabilities"}

   :csv-file
   {:opt "-f" :value "transactions.csv"
    :help "Input transactions in CSV format"}

   :account
   {:opt "-a" :value "Assets:Checking"
    :help "Originating account of transactions"}

   :file-kind
   {:opt "-k" :value "CSV" :help "Transaction file type: AMEX-JSON, AMEX-CSV, PayPal-CSV, ICS-JSON, SCHK-CSV or CSV (default: CSV)"}

   :csv-field-separator
   {:opt "-F" :value "," :help "CSV field separator"}

   :csv-skip-header-lines
   {:opt "-sa" :value 0 :conv-fun #(Integer. (str %))
    :help "CSV header lines to skip"}

   :csv-skip-trailer-lines
   {:opt "-sz" :value 0 :conv-fun #(Integer. (str %))
    :help "CSV trailer lines to skip"}

   :currency
   {:opt "-c" :value "SEK" :help "Currency: String or column index (zero-based)"}

   :date-format
   {:opt "-D" :value "yyyy-MM-dd" :help "Format of date field in CSV file"}

   :date-col
   {:opt "-d" :value 0 :conv-fun #(Integer. (str %))
    :help "Date column index (zero-based)"}

   :ref-col
   {:opt "-r" :value -1 :conv-fun #(Integer. (str %))
    :help "Payment reference column index (zero-based)"}

   :payee-col
   {:opt "-bp" :value -1 :conv-fun #(Integer. (str %))
    :help "Beancount Payee column index (zero-based)"}

   :links-col
   {:opt "-bl" :value -1 :conv-fun #(Integer. (str %))
    :help "Beancount Links (comma or space seperated) column index (zero-based)"}

   :tags-col
   {:opt "-bt" :value -1 :conv-fun #(Integer. (str %))
    :help "Beancount Tags (comma or space seperated) column index (zero-based)"}

   :foreign-amount-col
   {:opt "-fa" :value -1 :conv-fun #(Integer. (str %))
    :help "Foreign transaction amount column index (zero-based)"}

   :foreign-currency-col
   {:opt "-fc" :value -1 :conv-fun #(Integer. (str %))
    :help "Foreign currency column index (zero-based)"}

   :foreign-conversion-col
   {:opt "-ff" :value -1 :conv-fun #(Integer. (str %))
    :help "Foreign currency conversion-fee column index (zero-based)"}

   :forex-fees-account
   {:opt "-ffa" :value "Expenses:Bank-fees:Forex"
    :help "Expense account for foreign transaction fees"}

   :amount-col
   {:opt "-m" :value 2 :conv-fun #(Integer. (str %))
    :help "Amount column index (zero-based)"}

   :amount-positive
   {:opt "-ap" :value false :conv-fun #(if (#{"true" "1" "yes"} %) true #_else false)
    :help "Debit amounts are positive"}

   :amount-credit
   {:opt "-ac" :value nil :conv-fun #(when-not (empty? %) (Integer. (str %)))
    :help "Debit / Credit column index (zero-based)"}

   :descr-col
   {:opt "-t" :value "%3"
    :help "Text (descriptor) column index specs (zero-based)"}})

(defn print-usage-and-die [message]
  (println message)
  (println)
  (println "Usage: banks2ledger [options]")
  (println "  available options (syntax to set: -x value)")
  (doseq [{:keys [opt value help]} (vals cl-args-spec)]
    (println " " opt ":" help)
    (println "       default:" value))
  (System/exit 0))

(defn optvec [args-spec]
  (into [] (map :opt (vals args-spec))))

;; Get the value of an argument given by key.
;; If present, apply converter function.
(defn get-arg [args-spec key]
  (let [arg-spec (key args-spec)
        conv-fun (:conv-fun arg-spec)
        raw-value (:value arg-spec)]
    (if (nil? conv-fun)
      raw-value
      (conv-fun raw-value))))

(defn set-arg [args-spec arg value]
  (let [key (first (filter #(= arg (:opt (% args-spec)))
                           (keys args-spec)))
        val0 (key args-spec)
        val (conj val0 [:value value])]
    (conj args-spec [key val])))

(declare parse-args)
(defn parse-arg [args-spec arg rest-args]
  (if (some #{arg} (optvec cl-args-spec))
    (let [value (first rest-args)
          rest (rest rest-args)]
      (if (nil? value)
        (print-usage-and-die (str "Value expected for option " arg))
        (parse-args (set-arg args-spec arg value) rest)))
    (print-usage-and-die (str "Invalid argument: " arg))))

;; Go through the args list and return an updated args-spec
(defn parse-args [args-spec args]
  (if (empty? args)
    args-spec
    (parse-arg args-spec (first args) (rest args))))

(defn assoc-non-nil-from
  "Lookup key in map2 and associate its value to map.
   Leaves map as is when value is nil."
  [map map2 key]
  (let [val (get map2 key)]
    (if (nil? val) map #_else (assoc map key val))))

(defn concat-distinct-non-nil-from
  "Lookup key in map2 and concat its distinct values to map.
   Leaves map as is when value is nil."
  [map map2 key]
  (let [val2 (get map2 key)
        val1 (get map key)]
    (if (nil? val2) map #_else (assoc map key (apply conj val1 (remove (set val1) val2))))))

(defn update-flag [new-entry {:keys [flag] :as existing-entry}]
  (if-not (= flag "*") new-entry
   #_else (assoc new-entry :flag "*")))

;; Adjust :postings in new-entry with information from existing parsed ledger
(defn update-uncategorized [{:keys [postings] :as new-entry} src-account existing-entry]
  (if-not (some #(-> % :account (= :uncategorized)) postings)
    new-entry
   ;else
    (assoc new-entry :postings
     (let [known-account-set (->> postings (map :account) (remove #{:uncategorized}) set)]
       (for [p postings]
        (if-not (and (= (:account p) :uncategorized) (not= (:flag existing-entry) "!")) p
         #_else (assoc p :account
                       (first (remove #(or (str/includes? % src-account) (known-account-set %))
                                      (:accs existing-entry))))))))))

;; Adjust entry with information from an existing entry in parsed ledger
(defn update-from-existing-entry [new-entry src-account existing-entry]
  (-> new-entry
   (update-uncategorized src-account existing-entry)
   (update-flag existing-entry)
   (concat-distinct-non-nil-from existing-entry :links)
   (concat-distinct-non-nil-from existing-entry :tags)
   (assoc-non-nil-from existing-entry :payee)
   (assoc-non-nil-from existing-entry :descr)))

;; Adjust entries with information from parsed beancount/ledger
(defn update-from-existing-txn [src-account existing-txns entries]
  (for [{:keys [date reference] :as new-entry} entries
        :let [existing-entries (existing-txns reference)
              [existing-entry] (filter #(= (:date %) date) existing-entries)]]
    (if-not existing-entry
      new-entry
     ;else
      (update-from-existing-entry new-entry src-account existing-entry))))

;; Fold related entries if supported by format or customization
(defn dispatch-fold-related-txns [params entries]
  (case (get-arg params :file-kind)
    "PayPal-CSV" (paypal/fold-related-txns entries)
    #_else       (fold-related-txns entries)))

;; Convert date field from CSV format to Ledger entry format
(defn convert-date [args-spec datestr]
  (.format
   (SimpleDateFormat. (if (-> args-spec :format (= :beancount)) "yyyy-MM-dd" #_else "yyyy/MM/dd"))
   (.parse (SimpleDateFormat. (get-arg args-spec :date-format))
           datestr)))

;; Convert amount string to BigDecimal
;; - strip anything that does not belong to the number
;; - change decimal comma to dot
(defn convert-amount [^String string]
  (->>
   (-> (re-find #"-?\d[\d ]*[,\.]?\d*" string)
       (str/replace #"," ".")
       (.replace " " "")
       (BigDecimal.))))

(defn debit-credit-amount [amount debit-credit-str]
  (case (some-> debit-credit-str str/lower-case)
    ("af" "debit" "-" "d")
    (- amount)
    ;all other values are kept as is
    amount))

;; Remove quotes from start & end of the string, if both present
(defn unquote-string [str]
  (let [len (count str)
        last (dec len)]
    (cond (= "\"\"" str) ""
          (< len 3) str
          (or (and (str/starts-with? str "'")  (str/ends-with? str "'"))
              (and (str/starts-with? str "\"") (str/ends-with? str "\"")))
          (subs str 1 last)
          :else str)))

(defn all-indices-1 [str sub pos acc]
  (if-let [idx (str/index-of str sub pos)]
    (all-indices-1 str sub (inc idx) (conj acc idx))
   ;else
    acc))

;; Return an array of all indices where sub starts within str
(defn all-indices [str sub]
  (all-indices-1 str sub 0 []))

;; Split string at the list of supplied indices and return a
;; list of substrings. Note that the sublists do not contain the
;; characters at the indices, only those in between.
(defn split-by-indices [str ixs]
  (let [op-ixs (concat (list -1) ixs (list (count str)))]
    (map (fn [[s e]]
           (subs str (inc s) e))
         (partition 2 1 op-ixs))))

;; Return a vector of columns split from csv line.
;; NB: delimiters in quoted cells will not split the string
(defn split-csv-line [str delim]
  (let [delim-ixs (all-indices str delim)
        quote-ixs (all-indices str "\"")
        split-ixs (reduce
                   (fn [acc [start end]]
                     (filter #(not (and (< start %) (< % end))) acc))
                   delim-ixs
                   (partition 2 quote-ixs))]
    (into [] (split-by-indices str split-ixs))))

;; Render a colspec to an actual string based on cols;
;; return whitespace-trimmed version.
(defn format-colspec [cols colspec]
  (-> colspec
      (str/replace #"\%(\d+)"
             #(unquote-string (nth cols (Integer. (str (second %1))))))
      (str/trim)))

(defn get-col-1 [cols [spec & spec-list]]
  (let [fmt (format-colspec cols spec)]
    (if (or (> (count fmt) 0)
            (empty? spec-list))
      fmt
      (get-col-1 cols spec-list))))

;; Get column data from cols according to colspec, which is a string
;; similar to a printf format string but allowing alternatives to be
;; used if an earlier spec results in an empty string.
;; "%4" - get fourth column
;; "%4 %5" - get fourth and fifth column separated by a space
;; "%4!%1 %2 %3!%7" - fourth column by default, but if that is empty,
;;   (contains only whitespace) concatenate the first three columns;
;;   if that is empty, take the seventh column.
(defn get-col [cols colspec]
  (let [delim-ixs (all-indices colspec "!")
        spec-list (split-by-indices colspec delim-ixs)]
    (get-col-1 cols spec-list)))

(defn get-trimmed-col [cols n]
  (let [v (-> (nth cols n "") unquote-string str/trim)]
    (when-not (empty? v) v)))

(defn col-or-nil [params cols key]
  (when-let [n (get-arg params key)]
    (when (>= n 0)
      (get-trimmed-col cols n))))

(defn row->postings [params cols amount account forex-fees-account]
  (let [currency (get-arg params :currency)
        currency (if (re-matches #"[0-9]+" currency)
                   (get-trimmed-col cols (Integer. (str currency)))
                  #_else currency)
        amount-positive (get-arg params :amount-positive)
        amount (if amount-positive (- amount) #_else amount)
        foreign-amount (col-or-nil params cols :foreign-amount-col)
        foreign-currency (col-or-nil params cols :foreign-currency-col)
        foreign-conversion (col-or-nil params cols :foreign-conversion-col)]
    (if-not foreign-amount
      (if (neg? amount)
        [{:account :uncategorized :currency currency :amount (- amount)} {:account account}]
       ;else
        [{:account account :currency currency :amount amount} {:account :uncategorized}])
     ;else
      (let [foreign-conversion (BigDecimal. (str (or foreign-conversion "0")))]
        [{:account account :currency currency :amount amount}
         {:account forex-fees-account :currency currency :amount foreign-conversion}
         {:account :uncategorized
          :currency currency
          :amount (if (= foreign-currency currency) foreign-amount
          									;else
          								  (str foreign-amount " " foreign-currency
                         " @@ " (-> (- amount) (- (or foreign-conversion 0)) abs)))}]))))

;; Parse a line of CSV into a map with :date :ref :amount :descr
(defn parse-csv-entry [params cols]
  (let [account (get-arg params :account)
        forex-fees-account (get-arg params :forex-fees-account)]
    (case (get-arg params :file-kind)
      "AMEX-CSV"
      (amex-nl/parse-csv-columns account forex-fees-account cols)

      "KNAB-CSV"
      (knab-nl/parse-csv-columns account forex-fees-account (map unquote-string cols))

      "SCHK-CSV"
      (sc-hk/parse-csv-columns account forex-fees-account (map unquote-string cols))

      "PayPal-CSV"
      (paypal/parse-csv-columns account forex-fees-account (map unquote-string cols))

      "CSV"
      (let [links (some-> (col-or-nil params cols :links-col) (str/split #"\s*,\s*"))
            amount
            (-> (convert-amount (nth cols (get-arg params :amount-col)))
                (debit-credit-amount (col-or-nil params cols :amount-credit)))]
        [{:date (convert-date params (col-or-nil params cols :date-col))
          :flag "!"
          :reference (some-> (or (col-or-nil params cols :ref-col) (first links)) (str/replace #"[ :]" "_"))
          :postings (row->postings params cols (BigDecimal. (str amount)) account forex-fees-account)
          :payee (col-or-nil params cols :payee-col)
          :links links
          :tags  (some-> (col-or-nil params cols :tags-col) (str/split #"\s*,\s*"))
          :descr (unquote-string (get-col cols (get-arg params :descr-col)))}]))))

;; Drop the configured number of header and trailer lines
(defn drop-lines [lines params]
  (subvec lines
          (get-arg params :csv-skip-header-lines)
          (- (count lines) (get-arg params :csv-skip-trailer-lines))))

(defn date-sorted-rows [params seq]
  (if-let [n (get-arg params :date-col)]
    (sort-by #(convert-date params (unquote-string (nth % n))) seq)
   #_else seq))

;; Parse input CSV into a list of maps
(defn parse-csv [params existing-txn]
  (->> (-> (slurp (get-arg params :csv-file))
           (str/split #"\n")
           (drop-lines params))
       (map str/trim-newline)
       (map #(split-csv-line % (get-arg params :csv-field-separator)))
       (date-sorted-rows params)
       (mapcat #(parse-csv-entry params %))
       (map apply-entry-transforms)
       (remove nil?)
       (dispatch-fold-related-txns params)
       (update-from-existing-txn (get-arg params :account) existing-txn)))

;; Parse input JSON into a list of maps
(defn parse-json [params existing-txn]
  (let [account (get-arg params :account)
        ffacct  (get-arg params :forex-fees-account)
        json    (binding [*use-bigdecimals?* true]
                  (-> (get-arg params :csv-file)
                      (io/reader)
                      (json/parse-stream true)))]
    (->> (case (get-arg params :file-kind)
           "AMEX-JSON"
           (mapcat #(amex-nl/parse-json-transaction account ffacct %) (:transactions json))
           "ICS-JSON"
           (map #(ics-nl/parse-json-transaction account ffacct %) json))
         (map apply-entry-transforms)
         (remove nil?)
         (update-from-existing-txn account existing-txn))))

(defn postings->main-account [non-main-accounts postings]
  (->> postings (map :account) (remove non-main-accounts) first))

(defn decide-all-accounts [src-account acc-maps sub-acc-maps unknown-account non-main-accounts
                           payee postings]
  (if-not (some #(-> % :account (= :uncategorized)) postings)
    postings
   ;else
    (let [main-account (postings->main-account non-main-accounts postings)]
      (for [{:keys [account] :as entry} postings]
        (if (= :uncategorized account)
          (assoc entry :account (decide-account [(sub-acc-maps main-account) acc-maps]
                                                payee src-account unknown-account))
         ;else
          entry)))))

;; Concat 2 strings with a space in between
(defn- str+ [payee descr]
  (cond (empty? payee) (or descr "")
        (empty? descr) (or payee "")
        :else (str payee " " descr)))

(defn- not0 [n] (if (>= 0 n) 1 #_else n))

;; format and print a beancount entry to *out*
(defn print-beancount-entry [{:keys [date flag payee descr reference links tags metas postings]}]
  (printf "\n%s %s \"%s\" \"%s\"" date (or flag "!") (str payee) (str descr))
  (when-not (empty? reference)
    (printf (str" %" (not0 (- 60 (count payee) (count descr))) "s")
            (str "^" reference)))
  (doseq [link links]
    (when-not (= link reference) (printf " ^%s" link)))
  (doseq [tag tags] (printf " #%s" tag))
  (println)
  (doseq [[k v] metas]
    (printf (str "  %s %" (not0 (- 75 (count k))) "s\n") (str k ":") v))
  (doseq [{:keys [amount currency account commented]} postings]
    (printf (if-not commented "  %s" #_else " ;%s") account)
    (when amount
      (printf (str " %" (not0 (- 72 (count account))) "s") amount)
      (when currency (printf " %s" currency)))
    (println)))

;; format and print a ledger entry to *out*
(defn print-ledger-entry [{:keys [date payee descr reference postings] :as cm}]
  (let [descr (str+ payee descr)]
    (printf "%s " date)
    (if (and reference (not (empty? reference))) (printf "(%s) " reference))
    (println descr)

    (doseq [{:keys [amount currency account commented]} postings]

      (if amount
        (do (printf (if-not commented "    %-38s" #_else "   ;%-38s") account)
            (printf "%s %,.2f" currency amount))
       ;else
        (printf (if-not commented "    %s" #_else "   ;%s") account))
      (println))
    (println)))

(defn read-file [params existing-txn]
  (case (get-arg params :file-kind)
    ("CSV", "AMEX-CSV", "KNAB-CSV", "PayPal-CSV", "SCHK-CSV")
    (parse-csv params existing-txn)

    ("AMEX-JSON", "ICS-JSON")
    (parse-json params existing-txn)))

(defn dissoc-nested [m & keys]
  (->> (apply dissoc m keys)
       (map (fn [[k v]] [k (if (map? v) (apply dissoc-nested v keys) #_else v)]))
       (into {})))

(defn do-entries [f src-account existing-ledger unknown-account non-main-accounts entries]
  (let [acc-maps     (ledger->acc-maps existing-ledger)
        acc-maps     (apply dissoc-nested acc-maps non-main-accounts)
        sub-acc-maps (ledger->sub-acc-maps src-account existing-ledger)
        sub-acc-maps (apply dissoc-nested sub-acc-maps non-main-accounts)]
    (doseq [{:keys [payee descr postings] :as entry} entries :when postings]
      (f (->> postings
              (decide-all-accounts src-account acc-maps sub-acc-maps unknown-account
                                   non-main-accounts (str+ payee descr))
              (assoc entry :postings))))))

(defn group-by-links [entries]
  (let [transactions-with-id
        (->> entries
             (map #(when-let [id (get-in % [:metas "transaction-id"])] [(unquote-string id) [%]]))
             (remove nil?)
             (into {}))
        transactions-with-only-links
        (->> entries
             (mapcat #(when-not (get-in % [:metas "transaction-id"])
                       (for [l (:links %) :when (nil? (transactions-with-id l))]
                         (assoc % :link l))))
             (group-by :link))]
    (conj transactions-with-only-links transactions-with-id)))

;; Convert CSV of bank account transactions to corresponding ledger entries
(defn -main [& args]
  (let [params      (parse-args cl-args-spec args)
        src-account (get-arg params :account)
        ledger-file (get-arg params :ledger-file)
        non-mains   #{:uncategorized (get-arg params :forex-fees-account)}
        params      (assoc params :format
                           (if (str/ends-with? ledger-file ".beancount") :beancount :ledger))]
    (if (-> params :format (= :beancount))
      (let [ledger-entries (parse-beancount ledger-file)]
        (do-entries print-beancount-entry src-account ledger-entries
                    "Expenses:Uncategorized" non-mains
                    (read-file params (group-by-links ledger-entries))))
     ;else
      (let [ledger-entries (parse-ledger ledger-file)]
        (do-entries print-ledger-entry src-account ledger-entries "Unknown" non-mains
                    (read-file params {}))))
    (flush)))
