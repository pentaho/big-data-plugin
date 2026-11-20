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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

  /** Staggered Sqoop imports must overlap, keep their logs separate, and write the expected rows to HDFS. */
  @Test
  void importsConcurrentlyWithIndependentLogs() throws Exception {
    DockerUtils.ExecResult cleanup = DockerUtils.exec( ITUtils.hadoopContainerId(), "hdfs", "dfs", "-rm", "-r", "-f",
      "/it/sqoop/concurrent-one", "/it/sqoop/concurrent-two" );
    assertThat( cleanup.exitCode() ).as( "Remove previous concurrent import outputs: %s", cleanup.combinedOutput() )
      .isZero();
    String output = runJob( "sqoop/sqoop_concurrent_imports.kjb", true, null );
    String oneLog = entryLog( output, "Sqoop import one row" );
    String twoLog = entryLog( output, "Sqoop import two rows" );
    String oneJob = assertImportLog( oneLog, 1, 2 );
    String twoJob = assertImportLog( twoLog, 2, 1 );

    assertThat( oneJob ).isNotEqualTo( twoJob );
    assertThat( oneLog ).doesNotContain( twoJob );
    assertThat( twoLog ).doesNotContain( oneJob );
    String oneApplication = oneJob.replaceFirst( "^job_", "application_" );
    String twoApplication = twoJob.replaceFirst( "^job_", "application_" );
    assertThat( oneLog ).contains( oneApplication ).doesNotContain( twoApplication );
    assertThat( twoLog ).contains( twoApplication ).doesNotContain( oneApplication );
    assertThat( Math.max( output.indexOf( "Running job: " + oneJob ),
      output.indexOf( "Running job: " + twoJob ) ) )
      .as( "Both Hadoop jobs must start before either completes" )
      .isLessThan( Math.min( output.indexOf( "Job " + oneJob + " completed successfully" ),
        output.indexOf( "Job " + twoJob + " completed successfully" ) ) );

    assertThat( ITUtils.hdfsCat( "/it/sqoop/concurrent-one/part-*" ).lines() ).containsExactly( "1" );
    assertThat( ITUtils.hdfsCat( "/it/sqoop/concurrent-two/part-*" ).lines() )
      .containsExactlyInAnyOrder( "1", "2" );
  }

  /** Returns the Kitchen log lines associated with a particular job entry. */
  private static String entryLog( String output, String entryName ) {
    return output.lines().filter( line -> line.contains( " - " + entryName + " - " ) )
      .collect( Collectors.joining( "\n" ) );
  }

  /** Checks an import's own records and counters, and returns its Hadoop job ID. */
  private static String assertImportLog( String log, int records, int otherRecords ) {
    assertThat( log ).contains( "Retrieved " + records + " records.", "File System Counters", "Job Counters" )
      .doesNotContain( "Retrieved " + otherRecords + " records." );
    Matcher matcher = Pattern.compile( "Running job: (job_\\d+_\\d+)" ).matcher( log );
    assertThat( matcher.find() ).as( "Hadoop job ID in entry log" ).isTrue();
    String jobId = matcher.group( 1 );
    assertThat( matcher.find() ).as( "Only one Hadoop job ID per entry" ).isFalse();
    assertThat( log ).contains( "Job " + jobId + " completed successfully" );
    return jobId;
  }
}