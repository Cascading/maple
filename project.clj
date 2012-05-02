(defproject com.twitter/maple "0.1.5"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :description "All the Cascading taps we have to offer."
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dev-dependencies [[midje "1.3.1" :exclusions [org.clojure/clojure]]
                     [org.clojure/clojure "1.2.1"]
                     [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [cascading/cascading-hadoop "2.0.0-wip-288"
                      :exclusions [org.codehaus.janino/janino
                                   org.apache.hadoop/hadoop-core]]])
