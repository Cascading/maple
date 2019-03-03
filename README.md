# This repo is deprecated. Please see: [scalding/maple](https://github.com/twitter/scalding/maple)

which now hosts the HBase, MemoryTap, and Etsy LocalTap. For JDBC, please see: [Cascading-JDBC](https://github.com/Cascading/cascading-jdbc).

The reason for this deprecation is purely due to the management cost of the extra repo. If the old code meets your needs, there is little reason to upgrade.

# maple

A collection of useful Cascading taps.

## Building

Maple uses [Leiningen](https://github.com/technomancy/leiningen/) 2.0 to build.

1. lein with-profile dev deps
2. lein with-profile dev uberjar
3. lein with-profile dev install

The above should build a jar with all dependencies. And then `install` will add this jar to your
local maven repository in ~/.m2/repositories.

## Usage

Maple is hosted on [Conjars](http://conjars.org/com.twitter/maple).
We expect most users will pull "com.twitter/maple" with the version they need. If you are submitting a patch, you will
need to follow the above steps in Building.

## License

Copyright (C) 2012 Twitter Inc

Distributed under the Eclipse Public License, the same as Clojure.
