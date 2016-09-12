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

import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.VFS;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.impl.shim.pig.PigServiceFactoryImpl;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.ShimVersion;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.PigShim;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

/**
 * Created by bryan on 7/15/15.
 */
@JobEntry( id = "HadoopPigScriptExecutorPlugin", image = "PIG.svg", name = "HadoopPigScriptExecutorPlugin.Name", description = "HadoopPigScriptExecutorPlugin.Description",
    categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData", i18nPackageName = "org.pentaho.di.job.entries.pig",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Pig+Script+Executor" )
public class NoArgJobEntryPigScriptExecutor extends JobEntryPigScriptExecutor {
  private static final HadoopConfigurationProvider provider = initProvider();

  public NoArgJobEntryPigScriptExecutor() throws FileSystemException, ConfigurationException {
    super( new NamedClusterManager(), mock( RuntimeTestActionService.class ), mock( RuntimeTester.class ), initNamedClusterServiceLocator() );
  }

  private static NamedClusterServiceLocator initNamedClusterServiceLocator() throws ConfigurationException {
    NamedClusterServiceLocatorImpl namedClusterServiceLocator = new NamedClusterServiceLocatorImpl( mock( ClusterInitializer.class ) );
    namedClusterServiceLocator.factoryAdded( new PigServiceFactoryImpl( true, provider.getConfiguration( null ) ), Collections.emptyMap() );
    return namedClusterServiceLocator;
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
      config = new HadoopConfiguration( VFS.getManager().resolveFile( "ram:///" ), "test", "test", mock( HadoopShim.class ), mock( HadoopShim.class ), new TestPigShim() );
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

  static class TestPigShim implements PigShim {
    @Override
    public int[] executeScript( String pigScript, ExecutionMode executionMode, Properties properties ) throws Exception {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      ClassLoader classLoader = getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader( classLoader );
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

    @Override
    public ShimVersion getVersion() {
      return null;
    }

    @Override
    public boolean isLocalExecutionSupported() {
      return true;
    }

    @Override
    public void configure( Properties properties, Configuration configuration ) {
    }

    @Override
    public String substituteParameters( URL pigScript, List<String> paramList ) throws Exception {
      final InputStream inStream = pigScript.openStream();
      StringWriter writer = new StringWriter();
      // do parameter substitution
      ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor( 50 );
      psp.genSubstitutedFile( new BufferedReader( new InputStreamReader( inStream ) ), writer, paramList.size() > 0 ? paramList.toArray( new String[0] ) : null, null );
      return writer.toString();
    }

    protected ExecType getExecType( ExecutionMode mode ) {
      switch ( mode ) {
        case LOCAL:
          return ExecType.LOCAL;
        case MAPREDUCE:
          return ExecType.MAPREDUCE;
        default:
          throw new IllegalStateException( "unknown execution mode: " + mode );
      }
    }
  }
}
