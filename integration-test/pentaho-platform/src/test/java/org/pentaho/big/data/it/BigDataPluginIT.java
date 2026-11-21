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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for Big Data plugin integration tests.
 * <p>
 * Provides a single entry point, {@link #runTransformation}, that executes a {@code .ktr} inside the PDI
 * container using {@code pan.sh} (PAN execution) and validates the exit status and expected log lines.
 * Backend assertions (HDFS / HBase state) are performed by the concrete test classes through {@link ITUtils}.
 */
public abstract class BigDataPluginIT {

  /** PAN exit code that means "success" (0) or "success with warnings" is not tolerated by default. */
  private static final int PAN_SUCCESS = 0;

  /**
   * Executes a transformation and asserts the outcome.
   *
   * @param transformationPath path of the {@code .ktr} relative to the mounted transformations dir
   *                           (e.g. {@code hdfs/hdfs_output.ktr}).
   * @param expectSuccess      whether the transformation is expected to finish successfully.
   * @param expectedLogLines   substrings that must all be present in the PAN output.
   * @param parameters         optional named parameters passed to PAN as {@code -param:key=value}.
   * @return the combined PAN output for further assertions.
   */
  protected String runTransformation( String transformationPath, boolean expectSuccess,
                                       List<String> expectedLogLines, Map<String, String> parameters )
    throws IOException, InterruptedException {

    List<String> command = new ArrayList<>();
    command.add( ITUtils.PAN_SCRIPT );
    command.add( "-file=" + ITUtils.TRANSFORMATIONS_DIR + "/" + transformationPath );
    command.add( "-level=" + ITUtils.logLevel() );
    if ( parameters != null ) {
      for ( Map.Entry<String, String> entry : parameters.entrySet() ) {
        command.add( "-param:" + entry.getKey() + "=" + entry.getValue() );
      }
    }

    DockerUtils.ExecResult result = DockerUtils.exec( ITUtils.pdiContainerId(), command.toArray( new String[ 0 ] ) );
    String output = result.combinedOutput();

    if ( expectSuccess ) {
      assertThat( result.exitCode() )
        .as( "PAN exit code for %s%nOutput:%n%s", transformationPath, output )
        .isEqualTo( PAN_SUCCESS );
    } else {
      assertThat( result.exitCode() )
        .as( "PAN exit code for %s (expected failure)%nOutput:%n%s", transformationPath, output )
        .isNotEqualTo( PAN_SUCCESS );
    }

    if ( expectedLogLines != null ) {
      for ( String expected : expectedLogLines ) {
        assertThat( output )
          .as( "Expected log line '%s' in PAN output for %s", expected, transformationPath )
          .contains( expected );
      }
    }
    return output;
  }

  /** Convenience overload with no parameters. */
  protected String runTransformation( String transformationPath, boolean expectSuccess,
                                       List<String> expectedLogLines ) throws IOException, InterruptedException {
    return runTransformation( transformationPath, expectSuccess, expectedLogLines, null );
  }

  /** Builds an ordered parameter map for a transformation run. */
  protected static Map<String, String> params( String... keyValues ) {
    if ( keyValues.length % 2 != 0 ) {
      throw new IllegalArgumentException( "params() requires an even number of arguments" );
    }
    Map<String, String> map = new LinkedHashMap<>();
    for ( int i = 0; i < keyValues.length; i += 2 ) {
      map.put( keyValues[ i ], keyValues[ i + 1 ] );
    }
    return map;
  }
}
