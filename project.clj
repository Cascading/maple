(defproject com.twitter/maple "0.2.1"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :description "All the Cascading taps we have to offer."
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dev-dependencies [[midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [org.clojure/clojure "1.2.1"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [asm "3.2"]
                     [org.apache.hbase/hbase "0.90.2"
                      :exclusions [org.apache.hadoop/hadoop-core asm]]
                     [cascading/cascading-hadoop "2.0.0"
                      :exclusions [org.codehaus.janino/janino
                                   org.apache.hadoop/hadoop-core]]])
