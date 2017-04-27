/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.tests;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.NoShimSpecifiedException;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by mburgess on 9/1/15.
 */
public class TestShimConfig extends BaseRuntimeTest {

  public static final String HADOOP_CONFIGURATION_TEST_SHIM_CONFIG = "hadoopConfigurationTestShimXConfig";
  public static final String TEST_SHIM_CONFIG_NAME = "TestShimConfig.Name";
  public static final String TEST_SHIM_CONFIG_FS_MATCH_DESC = "TestShimConfig.FileSystemMatch.Desc";
  public static final String TEST_SHIM_CONFIG_FS_MATCH_MESSAGE = "TestShimConfig.FileSystemMatch.Message";
  public static final String TEST_SHIM_CONFIG_FS_NOMATCH_DESC = "TestShimConfig.FileSystemNoMatch.Desc";
  public static final String TEST_SHIM_CONFIG_FS_NOMATCH_MESSAGE = "TestShimConfig.FileSystemNoMatch.Message";

  private static final Class<?> PKG = TestShimConfig.class;

  private final MessageGetterFactory messageGetterFactory;
  private final MessageGetter messageGetter;
  private final HadoopConfigurationBootstrap hadoopConfigurationBootstrap;

  public TestShimConfig( MessageGetterFactory messageGetterFactory ) {
    this( messageGetterFactory, HadoopConfigurationBootstrap.getInstance() );
  }

  public TestShimConfig( MessageGetterFactory messageGetterFactory,
                         HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    super( NamedCluster.class, TestShimLoad.HADOOP_CONFIGURATION_MODULE, HADOOP_CONFIGURATION_TEST_SHIM_CONFIG,
      messageGetterFactory.create( PKG ).getMessage( TEST_SHIM_CONFIG_NAME ), true,
      new HashSet<>( Arrays.asList( TestShimLoad.HADOOP_CONFIGURATION_TEST_SHIM_LOAD ) ) );
    this.messageGetterFactory = messageGetterFactory;
    messageGetter = messageGetterFactory.create( PKG );
    this.hadoopConfigurationBootstrap = hadoopConfigurationBootstrap;
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    String activeConfigurationId = "";
    try {
      activeConfigurationId = hadoopConfigurationBootstrap.getWillBeActiveConfigurationId();
      // Get the active shim
      HadoopConfigurationProvider hadoopConfigurationProvider = hadoopConfigurationBootstrap.getProvider();

      HadoopConfiguration hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();
      activeConfigurationId = hadoopConfiguration.getIdentifier();
      Configuration config = hadoopConfiguration.getHadoopShim().createConfiguration();
      String defaultFS = config.get( HadoopFileSystem.FS_DEFAULT_NAME );

      // Get the named cluster
      NamedCluster namedCluster = (NamedCluster) objectUnderTest;

      // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
      // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
      // Here we try to resolve the parameters if we can:
      Variables variables = new Variables();
      variables.initializeVariablesFrom( null );

      // Build up a "defaultFS" property to check against the config
      StringBuilder ncFS = new StringBuilder( namedCluster.getStorageScheme() + "://" );
      ncFS.append( variables.environmentSubstitute( namedCluster.getHdfsHost() ) );
      String port = variables.environmentSubstitute( namedCluster.getHdfsPort() );
      if ( !Const.isEmpty( port ) ) {
        ncFS.append( ":" );
        ncFS.append( port );
      }

      if ( !ncFS.toString().equalsIgnoreCase( defaultFS ) ) {
        return new RuntimeTestResultSummaryImpl(
          new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
            messageGetter.getMessage( TEST_SHIM_CONFIG_FS_NOMATCH_DESC ),
            messageGetter.getMessage( TEST_SHIM_CONFIG_FS_NOMATCH_MESSAGE, ncFS.toString() ),
            ClusterRuntimeTestEntry.DocAnchor.SHIM_LOAD ) );
      }

      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.INFO,
          messageGetter.getMessage( TEST_SHIM_CONFIG_FS_MATCH_DESC ),
          messageGetter.getMessage( TEST_SHIM_CONFIG_FS_MATCH_MESSAGE ),
          ClusterRuntimeTestEntry.DocAnchor.SHIM_LOAD ) );
    } catch ( NoShimSpecifiedException e ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.ERROR,
          messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC ), e.getMessage(), e,
          ClusterRuntimeTestEntry.DocAnchor.SHIM_LOAD ) );
    } catch ( ConfigurationException e ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.ERROR,
          messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_UNABLE_TO_LOAD_SHIM_DESC, activeConfigurationId ),
          e.getMessage(), e, ClusterRuntimeTestEntry.DocAnchor.SHIM_LOAD ) );
    }
  }
}
