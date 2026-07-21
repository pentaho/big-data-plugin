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

/**
 * Smoke test: verifies that the PDI container starts, the Big Data plugin loads, and a trivial
 * transformation runs to completion via PAN. This does not touch any Hadoop backend.
 */
class BasicIT extends BigDataPluginIT {

  @Test
  void pluginLoadsAndTrivialTransformationRuns() throws Exception {
    String output = runTransformation(
      "basic/basic_smoke.ktr",
      true,
      List.of( "bigdata-it-smoke-ok" ) );

    // The Big Data plugin registers Hadoop VFS schemes; their presence is a good signal the plugin loaded.
    // (Assertions on backend state are covered by the HDFS/HBase/Formats tests.)
    org.assertj.core.api.Assertions.assertThat( output ).doesNotContain( "ERROR" );
  }
}
