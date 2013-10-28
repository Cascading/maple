(defproject com.twitter/maple "0.3.1"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :url "http://github.com/Cascading/maple"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :description "All the Cascading taps we have to offer."
  :repositories {"conjars" "http://conjars.org/repo/"}
  :profiles {:provided {:dependencies [[org.apache.hbase/hbase "0.94.5"
                                        :exclusions [org.apache.hadoop/hadoop-core asm]]
                                        [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                                        [cascading/cascading-hadoop "2.0.0"
                                         :exclusions [org.codehaus.janino/janino
                                                      org.apache.hadoop/hadoop-core]]]}})
