/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.pig;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;

public class JobEntryPigScriptExecutorIT {

  StringBuffer m_reference;

  private StringBuffer readResource( Reader r ) throws IOException {

    StringBuffer ret = new StringBuffer();
    char[] buf = new char[5];

    for ( int read = r.read( buf ); read > 0; read = r.read( buf ) ) {
      ret.append( new String( buf, 0, read ) );
    }

    r.close();
    return ret;
  }

  @Before
  public void setup() throws IOException {
    BufferedReader br =
        new BufferedReader( new InputStreamReader( JobEntryPigScriptExecutorIT.class.getClassLoader().getResourceAsStream( "org/pentaho/di/job/entries/pig/JobEntryPigScriptExecutorTest.ref" ) ) );

    m_reference = readResource( br );
  }

  @Test
  public void testRegressionTutorialLocal() throws Exception {
    Field providerField = HadoopConfigurationBootstrap.class.getDeclaredField( "provider" );
    providerField.setAccessible( true );
    providerField.set( HadoopConfigurationBootstrap.getInstance(), NoArgJobEntryPigScriptExecutor.getProvider() );

    System.setProperty( "KETTLE_PLUGIN_CLASSES", NoArgJobEntryPigScriptExecutor.class.getCanonicalName() );
    KettleEnvironment.init();
    File directory = new File( "target/pigTest" );
    FileUtils.deleteDirectory( directory );
    FileUtils.copyDirectory( new File( "src/it/resources/pig" ), directory );
    JobMeta meta = new JobMeta( "target/pigTest/pigTest.kjb", null );

    Job job = new Job( null, meta );

    job.start();
    job.waitUntilFinished();

    BufferedReader br = new BufferedReader( new FileReader( "target/pigTest/bin/test/pig/script1-local-results.txt/part-r-00000" ) );
    StringBuffer pigOutput = readResource( br );

    assertEquals( m_reference.toString().replace( '\n', ' ' ).replace( '\t', ' ' ).replace( '\r', ' ' ).replace( " ", "" ), pigOutput.toString().replace( '\n', ' ' ).replace( '\t', ' ' ).replace(
        '\r', ' ' ).replace( " ", "" ) );
  }

}
