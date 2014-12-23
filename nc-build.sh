#!/bin/bash

cd ../pentaho-hadoop-shims
ant clean-all resolve dist
cd ../big-data-plugin
ant clean-all resolve dist
cp dist/pentaho-big-data-plugin-TRUNK-SNAPSHOT.zip ~/Downloads/pdi-ee/data-integration/plugins/
cp dist/pentaho-big-data-plugin-TRUNK-SNAPSHOT.zip ~/Downloads/pdi-ee/data-integration-server/pentaho-solutions/system/kettle/plugins/

cd ~/Downloads/pdi-ee/data-integration/plugins
rm -Rf pentaho-big-data-plugin
unzip pentaho-big-data-plugin-TRUNK-SNAPSHOT.zip
rm pentaho-big-data-plugin-TRUNK-SNAPSHOT.zip

cd ~/Downloads/pdi-ee/data-integration-server/pentaho-solutions/system/kettle/plugins/
rm -Rf pentaho-big-data-plugin
unzip pentaho-big-data-plugin-TRUNK-SNAPSHOT.zip
rm pentaho-big-data-plugin-TRUNK-SNAPSHOT.zip

