This plugin contains ParquetInput step with some hacks for loading hadoop jars.

Installation:

1. Put pentaho-big-data-kettle-plugins-parquet-8.0-SNAPSHOT.jar into data-integration/plugins/parquet/

2. Put jars into data-integration/plugins/parquet/lib/ :
	commons-collections-3.2.2.jar
	commons-io-2.4.jar
	commons-lang-2.6.jar
	guava-21.0.jar
	hadoop-auth-2.7.3.jar
	hadoop-common-2.7.3.jar
	hadoop-mapreduce-client-core-2.7.3.jar
	parquet-avro-1.9.0.jar
	parquet-column-1.9.0.jar
	parquet-common-1.9.0.jar
	parquet-encoding-1.9.0.jar
	parquet-format-2.3.1.jar
	parquet-hadoop-1.9.0.jar
	parquet-jackson-1.9.0.jar
	slf4j-api-1.7.25.jar
	slf4j-simple-1.7.25.jar

Execution:

1. Put some parquet file info /tmp/in/ folder

2. Run it
