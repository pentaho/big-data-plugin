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
 * Functional HBase tests: writes rows to an HBase table via the HBase Output step and reads them back.
 * Backend state is asserted through the HBase shell.
 * <p>
 * Disabled until the KTR fixtures and the HBase container table bootstrap are validated against a live
 * environment (docker + registry access required).
 */
@Disabled( "Pending validation of HBase KTR fixtures and table bootstrap in a docker-enabled environment" )
class HBaseIT extends BigDataPluginIT {

  private static final String TABLE = "it_cars";

  @Test
  void writeThenReadFromHBase() throws Exception {
    runTransformation(
      "hbase/hbase_output.ktr",
      true,
      List.of( "bigdata-it-hbase-write-ok" ),
      params( "NAMED_CLUSTER", ITUtils.namedCluster(), "HBASE_TABLE", TABLE ) );

    String scan = ITUtils.hbaseShell( "scan '" + TABLE + "'" );
    assertThat( scan ).contains( "Toyota" );

    String readOutput = runTransformation(
      "hbase/hbase_input.ktr",
      true,
      List.of( "bigdata-it-hbase-read-ok" ),
      params( "NAMED_CLUSTER", ITUtils.namedCluster(), "HBASE_TABLE", TABLE ) );
    assertThat( readOutput ).contains( "Toyota" );
  }
}
