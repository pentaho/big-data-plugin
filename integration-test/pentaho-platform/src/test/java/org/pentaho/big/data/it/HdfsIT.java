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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Functional HDFS tests: a transformation writes rows to HDFS through the {@code hc://} VFS scheme
 * (resolved by the named cluster + apachevanilla shim), and a second transformation reads them back.
 * Results are asserted both on the backend (via {@code hdfs dfs -cat}) and through the PAN logs.
 */
class HdfsIT extends BigDataPluginIT {

  private static final String HDFS_OUTPUT_PATH = "/it/hdfs/cars.csv";

  @Test
  void writeThenReadFromHdfs() throws Exception {
    // 1. Write to HDFS.
    runTransformation(
      "hdfs/hdfs_output.ktr",
      true,
      List.of( "bigdata-it-hdfs-write-ok" ),
      params( "NAMED_CLUSTER", ITUtils.namedCluster(), "HDFS_PATH", HDFS_OUTPUT_PATH ) );

    // 2. Assert backend state directly.
    assertThat( ITUtils.hdfsExists( HDFS_OUTPUT_PATH ) )
      .as( "HDFS file %s should exist after the write transformation", HDFS_OUTPUT_PATH )
      .isTrue();
    String hdfsContent = ITUtils.hdfsCat( HDFS_OUTPUT_PATH );
    assertThat( hdfsContent ).contains( "Toyota" ).contains( "Honda" );

    // 3. Read back through PDI and assert the PAN logs.
    String readOutput = runTransformation(
      "hdfs/hdfs_input.ktr",
      true,
      List.of( "bigdata-it-hdfs-read-ok" ),
      params( "NAMED_CLUSTER", ITUtils.namedCluster(), "HDFS_PATH", HDFS_OUTPUT_PATH ) );
    assertThat( readOutput ).contains( "Toyota" );
  }
}
