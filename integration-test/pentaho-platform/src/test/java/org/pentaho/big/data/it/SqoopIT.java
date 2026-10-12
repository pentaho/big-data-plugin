/* ******************************************************************************
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

/** Runs the Sqoop job entry against the disposable PostgreSQL and Hadoop containers. */
class SqoopIT extends BigDataPluginIT {

  private static final String HDFS_OUTPUT_PATH = "/it/sqoop/hadoop";

  @Test
  void importsPostgresTableIntoHdfs() throws Exception {
    runJob(
      "sqoop/sqoop_import.kjb",
      true,
      List.of(
        "Retrieved 2 records.",
        "File System Counters",
        "Job Counters" ) );

    assertThat( ITUtils.hdfsExists( HDFS_OUTPUT_PATH ) )
      .as( "Sqoop target directory %s should exist in HDFS", HDFS_OUTPUT_PATH )
      .isTrue();

    assertThat( ITUtils.hdfsCat( HDFS_OUTPUT_PATH + "/part-*" ).lines() )
      .containsExactlyInAnyOrder( "1", "2" );
  }
}