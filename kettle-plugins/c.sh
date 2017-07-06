#!/bin/sh
set -x

BASE=/data/ee/0611/data-integration

#rm -rf $BASE/system/karaf/caches
#mkdir -p /data/ee/0517/data-integration/system/karaf/system/pentaho/pentaho-big-data-kettle-plugins-parquet/8.0-SNAPSHOT/
#cp parquet/target/pentaho-big-data-kettle-plugins-parquet-8.0-SNAPSHOT.jar /data/ee/0517/data-integration/system/karaf/system/pentaho/pentaho-big-data-kettle-plugins-parquet/8.0-SNAPSHOT/

cd parquet
mvn clean install || exit 1
cd ..
cp parquet/target/pentaho-big-data-kettle-plugins-parquet-8.0-SNAPSHOT.jar $BASE/plugins/pq/
cp ~/.m2/repository/pentaho-kettle/kettle-engine/8.0-SNAPSHOT/kettle-engine-8.0-SNAPSHOT.jar $BASE/lib/
cp ~/.m2/repository/pentaho-kettle/kettle-ui-swt/8.0-SNAPSHOT/kettle-ui-swt-8.0-SNAPSHOT.jar $BASE/lib/

cd $BASE
./spoon.sh
