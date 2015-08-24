/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.impl.shim.pig.PigServiceFactoryImpl;
import org.pentaho.bigdata.api.pig.PigServiceFactory;
import org.pentaho.bigdata.api.pig.impl.PigServiceLocatorImpl;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.common.CommonHadoopShim;
import org.pentaho.hadoop.shim.common.CommonPigShim;
import org.pentaho.hadoop.shim.common.CommonSqoopShim;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 7/15/15.
 */
@JobEntry( id = "HadoopPigScriptExecutorPlugin", image = "PIG.svg", name = "HadoopPigScriptExecutorPlugin.Name",
  description = "HadoopPigScriptExecutorPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.pig",
  documentationUrl = "http://wiki.pentaho.com/display/EAI/Pig+Script+Executor" )
public class NoArgJobEntryPigScriptExecutor extends JobEntryPigScriptExecutor {
  private static final HadoopConfigurationProvider provider = initProvider();

  public NoArgJobEntryPigScriptExecutor() throws FileSystemException, ConfigurationException {
    super( new NamedClusterManager(), mock( ClusterTester.class ), new PigServiceLocatorImpl( Arrays.<PigServiceFactory>asList(
      new PigServiceFactoryImpl( true, provider.getConfiguration( null ) ) ), mock( ClusterInitializer.class ) ) );
  }

  public static HadoopConfigurationProvider getProvider() {
    return provider;
  }

  private static HadoopConfigurationProvider initProvider() {
    try {
      return new TestProvider();
    } catch ( FileSystemException e ) {
      e.printStackTrace();
      return null;
    }
  }

  static class TestProvider implements HadoopConfigurationProvider {
    HadoopConfiguration config;

    TestProvider() throws FileSystemException {
      config = new HadoopConfiguration( VFS.getManager().resolveFile( "ram:///" ), "test", "test",
        new CommonHadoopShim(), new CommonSqoopShim(), new TestPigShim() );
    }

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
  }

  static class TestPigShim extends CommonPigShim {
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
