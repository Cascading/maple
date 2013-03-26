(defproject com.twitter/maple "0.2.7"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :description "All the Cascading taps we have to offer."
  :repositories {"conjars" "http://conjars.org/repo/"}
  :profiles {
    :provided {:dependencies [[org.apache.hbase/hbase "0.94.5"
                             :exclusions [org.apache.hadoop/hadoop-core asm]]]}
    :dev {:dependencies [[midje "1.3.1" :exclusions [org.clojure/clojure]]
                        [org.clojure/clojure "1.2.1"]
                        [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                        [asm "3.2"]
                        [cascading/cascading-hadoop "2.0.0"
                        :exclusions [org.codehaus.janino/janino
                                     org.apache.hadoop/hadoop-core]]]}
  }
)
