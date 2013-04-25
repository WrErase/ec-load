(defproject ec-load "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :main ec-load.core
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [clojure-csv/clojure-csv "2.0.0-alpha2"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [clj-time "0.4.4"]
;                 [org.zeromq/jzmq "2.1.0-SNAPSHOT"]
                 [incanter "1.5.0-SNAPSHOT"]
                 [korma "0.3.0-RC2"]
                 [postgresql "9.1-901.jdbc4"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]
  :profiles {:dev {:jvm-opts ["-Djava.library.path=./jzmq-2.1.0-SNAPSHOT/lib"]}}
)
