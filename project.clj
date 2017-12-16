(defproject banks2ledger "1.0.1"
  :description "Banks' CSV to ledger converter with probabilistic payment matching"
  :url "https://tomszilagyi.github.io/payment-matching"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [io.replikativ/hasch "0.3.4" :exclusions [com.cemerick/austin]]
                 [cheshire "5.7.1"]]
  :global-vars {*warn-on-reflection* true}
  :main ^:skip-aot banks2ledger.core
  :source-paths ["../banks2ledger-customizations" "src" "src/main/clojure"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
