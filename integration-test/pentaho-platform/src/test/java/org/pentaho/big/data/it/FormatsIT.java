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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Functional Formats tests over HDFS: writes and reads Parquet / Avro / ORC files stored in HDFS through
 * the {@code hc://} VFS scheme. Backend state is asserted by checking the files exist in HDFS and by
 * reading the records back through PDI.
 * <p>
 * Disabled until the KTR fixtures are validated against a live environment (docker + registry access
 * required).
 */
@Disabled( "Pending validation of Formats KTR fixtures in a docker-enabled environment" )
class FormatsIT extends BigDataPluginIT {

  @Test
  void parquetRoundTrip() throws Exception {
    formatRoundTrip( "parquet", "/it/formats/cars.parquet" );
  }

  @Test
  void avroRoundTrip() throws Exception {
    formatRoundTrip( "avro", "/it/formats/cars.avro" );
  }

  @Test
  void orcRoundTrip() throws Exception {
    formatRoundTrip( "orc", "/it/formats/cars.orc" );
  }

  private void formatRoundTrip( String format, String hdfsPath ) throws Exception {
    runTransformation(
      "formats/" + format + "_output.ktr",
      true,
      List.of( "bigdata-it-" + format + "-write-ok" ),
      params( "NAMED_CLUSTER", ITUtils.namedCluster(), "HDFS_PATH", hdfsPath ) );

    assertThat( ITUtils.hdfsExists( hdfsPath ) )
      .as( "%s file %s should exist in HDFS", format, hdfsPath )
      .isTrue();

    String readOutput = runTransformation(
      "formats/" + format + "_input.ktr",
      true,
      List.of( "bigdata-it-" + format + "-read-ok" ),
      params( "NAMED_CLUSTER", ITUtils.namedCluster(), "HDFS_PATH", hdfsPath ) );
    assertThat( readOutput ).contains( "Toyota" );
  }
}
