/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.it;

import java.io.IOException;

/**
 * Shared constants and small helpers for the Big Data plugin integration tests.
 * <p>
 * Container ids and topology values are injected by the docker-maven-plugin / failsafe through the
 * {@code bigdata.it.*} system properties (see integration-test/pentaho-platform/pom.xml).
 */
public final class ITUtils {

  /** Root of the PDI installation inside the {@code automation/pdi-client} image. */
  public static final String DATA_INTEGRATION_DIR = "/home/devuser/pentaho/design-tools/data-integration";
  public static final String PAN_SCRIPT = DATA_INTEGRATION_DIR + "/pan.sh";
  public static final String TRANSFORMATIONS_DIR = DATA_INTEGRATION_DIR + "/transformations";

  // System properties injected by the build.
  public static final String PROP_PDI_CONTAINER = "bigdata.it.pdi-container-id";
  public static final String PROP_HADOOP_CONTAINER = "bigdata.it.hadoop-container-id";
  public static final String PROP_HBASE_CONTAINER = "bigdata.it.hbase-container-id";
  public static final String PROP_NAMED_CLUSTER = "bigdata.it.named-cluster";
  public static final String PROP_HADOOP_HOST = "bigdata.it.hadoop-host";
  public static final String PROP_HBASE_HOST = "bigdata.it.hbase-host";
  public static final String PROP_LOG_LEVEL = "bigdata.it.transformation-log-level";

  private ITUtils() {
  }

  public static String pdiContainerId() {
    return required( PROP_PDI_CONTAINER );
  }

  public static String hadoopContainerId() {
    return required( PROP_HADOOP_CONTAINER );
  }

  public static String hbaseContainerId() {
    return required( PROP_HBASE_CONTAINER );
  }

  public static String namedCluster() {
    return System.getProperty( PROP_NAMED_CLUSTER, "it-cluster" );
  }

  public static String logLevel() {
    return System.getProperty( PROP_LOG_LEVEL, "Basic" );
  }

  /** Reads a file from HDFS by exec'ing {@code hdfs dfs -cat} inside the Hadoop container. */
  public static String hdfsCat( String hdfsPath ) throws IOException, InterruptedException {
    DockerUtils.ExecResult r = DockerUtils.exec( hadoopContainerId(), "hdfs", "dfs", "-cat", hdfsPath );
    if ( !r.isSuccess() ) {
      throw new IOException( "hdfs dfs -cat " + hdfsPath + " failed: " + r.combinedOutput() );
    }
    return r.stdout();
  }

  /** Returns {@code true} when the given HDFS path exists. */
  public static boolean hdfsExists( String hdfsPath ) throws IOException, InterruptedException {
    return DockerUtils.exec( hadoopContainerId(), "hdfs", "dfs", "-test", "-e", hdfsPath ).isSuccess();
  }

  /** Runs an HBase shell command (for example {@code scan 'table'}) and returns its output. */
  public static String hbaseShell( String command ) throws IOException, InterruptedException {
    String script = "echo \"" + command.replace( "\"", "\\\"" ) + "\" | hbase shell -n 2>/dev/null";
    DockerUtils.ExecResult r = DockerUtils.execShell( hbaseContainerId(), script );
    if ( !r.isSuccess() ) {
      throw new IOException( "hbase shell command failed: " + r.combinedOutput() );
    }
    return r.stdout();
  }

  private static String required( String key ) {
    String value = System.getProperty( key );
    if ( value == null || value.isBlank() ) {
      throw new IllegalStateException( "Missing required system property: " + key
        + ". Are the integration test containers running?" );
    }
    return value;
  }
}
