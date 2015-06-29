/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.di.job.entries.pig;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs.VFS;
import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;
import org.pentaho.hadoop.shim.common.CommonPigShim;
import org.pentaho.hadoop.shim.common.CommonSqoopShim;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class JobEntryPigScriptExecutorTest {

  StringBuffer m_reference;

  private StringBuffer readResource( Reader r ) throws IOException {

    StringBuffer ret = new StringBuffer();
    char[] buf = new char[ 5 ];

    for ( int read = r.read( buf ); read > 0; read = r.read( buf ) ) {
      ret.append( new String( buf, 0, read ) );
    }

    r.close();
    return ret;
  }

  @Before
  public void setup() throws IOException {
    BufferedReader br =
      new BufferedReader(
        new InputStreamReader( JobEntryPigScriptExecutorTest.class.getClassLoader().getResourceAsStream(
          "org/pentaho/di/job/entries/pig/JobEntryPigScriptExecutorTest.ref" ) ) );

    m_reference = readResource( br );
  }

  @Test
  public void testRegressionTutorialLocal() throws Exception {
    HadoopConfigurationProvider provider = new HadoopConfigurationProvider() {

      HadoopConfiguration config = new HadoopConfiguration( VFS.getManager().resolveFile( "ram:///" ), "test", "test",
        new CommonHadoopShim(), new CommonSqoopShim(), new TestPigShim() );

      @Override
      public boolean hasConfiguration( String id ) {
        return true;
      }

      @Override
      public List<? extends HadoopConfiguration> getConfigurations() {
        return Arrays.asList( config );
      }

      @Override
      public HadoopConfiguration getConfiguration( String id ) throws ConfigurationException {
        return config;
      }

      @Override
      public HadoopConfiguration getActiveConfiguration() throws ConfigurationException {
        return config;
      }
    };

    Field providerField = HadoopConfigurationBootstrap.class.getDeclaredField( "provider" );
    providerField.setAccessible( true );
    providerField.set( HadoopConfigurationBootstrap.getInstance(), provider );

    System.setProperty( "KETTLE_PLUGIN_CLASSES", "org.pentaho.di.job.entries.pig.JobEntryPigScriptExecutor" );
    KettleEnvironment.init();
    File directory = new File( "target/pigTest" );
    FileUtils.deleteDirectory( directory );
    FileUtils.copyDirectory( new File( "src/test/resources/pig" ), directory );
    JobMeta meta = new JobMeta( "target/pigTest/pigTest.kjb", null );

    Job job = new Job( null, meta );

    job.start();
    job.waitUntilFinished();

    BufferedReader br =
      new BufferedReader( new FileReader( "target/pigTest/bin/test/pig/script1-local-results.txt/part-r-00000" ) );
    StringBuffer pigOutput = readResource( br );

    assertEquals( m_reference.toString(), pigOutput.toString() );
  }

  class TestPigShim extends CommonPigShim {
    @Override public int[] executeScript( String pigScript, ExecutionMode executionMode, Properties properties )
      throws Exception {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      try {
        PigServer pigServer = new PigServer( getExecType( executionMode ), properties );
        GruntParser grunt = new GruntParser( new StringReader( pigScript ) );
        grunt.setInteractive( false );
        grunt.setParams( pigServer );
        return grunt.parseStopOnError( false );
      } finally {
        Thread.currentThread().setContextClassLoader( cl );
      }
    }
  }

}
