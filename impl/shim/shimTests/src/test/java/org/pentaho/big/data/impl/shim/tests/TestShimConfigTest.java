/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.tests;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by mburgess on 9/1/15.
 */
public class TestShimConfigTest {

  private MessageGetterFactory messageGetterFactory;
  private MessageGetter messageGetter;
  private TestShimConfig testShimConfig;
  private NamedCluster namedCluster;

//  @Before
//  public void setup() {
//    messageGetterFactory = new TestMessageGetterFactory();
//    messageGetter = messageGetterFactory.create( TestShimConfig.class );
//    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
//    testShimConfig = new TestShimConfig( messageGetterFactory, hadoopConfigurationBootstrap );
//    namedCluster = mock( NamedCluster.class );
//  }
//
//  @Test
//  public void testGetName() {
//    assertEquals( messageGetter.getMessage( TestShimConfig.TEST_SHIM_CONFIG_NAME ), testShimConfig.getName() );
//  }
//
//  @Test
//  public void testConfigurationException() throws ConfigurationException {
//    String testMessage = "testMessage";
//    String testId = "testId";
//    when( hadoopConfigurationBootstrap.getWillBeActiveConfigurationId() ).thenReturn( testId );
//    when( hadoopConfigurationBootstrap.getProvider() ).thenThrow( new ConfigurationException( testMessage ) );
//    RuntimeTestResultSummary runtimeTestResultSummary = testShimConfig.runTest( namedCluster );
//    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
//      RuntimeTestEntrySeverity.ERROR,
//      messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_UNABLE_TO_LOAD_SHIM_DESC, testId ),
//      testMessage, ConfigurationException.class );
//    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
//  }
//
//  @Test
//  public void testNoShimSpecified() throws ConfigurationException {
//    String testMessage = "testMessage";
//    when( hadoopConfigurationBootstrap.getProvider() ).thenThrow( new NoShimSpecifiedException( testMessage ) );
//    RuntimeTestResultSummary runtimeTestResultSummary = testShimConfig.runTest( namedCluster );
//    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
//      RuntimeTestEntrySeverity.ERROR, messageGetter.getMessage( TestShimLoad.TEST_SHIM_LOAD_NO_SHIM_SPECIFIED_DESC ),
//      testMessage, NoShimSpecifiedException.class );
//    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
//  }
//
//  @Test
//  public void testFileSystemMatch() throws ConfigurationException {
//    HadoopConfigurationProvider provider = mock( HadoopConfigurationProvider.class );
//    HadoopConfiguration hadoopConfig = mock( HadoopConfiguration.class );
//    HadoopShim hadoopShim = mock( HadoopShim.class );
//    Configuration config = mock( Configuration.class );
//
//    when( hadoopConfigurationBootstrap.getProvider() ).thenReturn( provider );
//    when( provider.getActiveConfiguration() ).thenReturn( hadoopConfig );
//    when( hadoopConfig.getHadoopShim() ).thenReturn( hadoopShim );
//    when( hadoopShim.createConfiguration() ).thenReturn( config );
//    when( config.get( HadoopFileSystem.FS_DEFAULT_NAME ) ).thenReturn( "hdfs://success" );
//    when( namedCluster.getHdfsHost() ).thenReturn( "success" );
//    when( namedCluster.getStorageScheme() ).thenReturn( "hdfs" );
//    when( namedCluster.getHdfsPort() ).thenReturn( null );
//
//    RuntimeTestResultSummary runtimeTestResultSummary = testShimConfig.runTest( namedCluster );
//    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
//      RuntimeTestEntrySeverity.INFO, messageGetter.getMessage( TestShimConfig.TEST_SHIM_CONFIG_FS_MATCH_DESC ),
//      messageGetter.getMessage( TestShimConfig.TEST_SHIM_CONFIG_FS_MATCH_MESSAGE ) );
//    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
//  }
//
//  @Test
//  public void testFileSystemNoMatch() throws ConfigurationException {
//    HadoopConfigurationProvider provider = mock( HadoopConfigurationProvider.class );
//    HadoopConfiguration hadoopConfig = mock( HadoopConfiguration.class );
//    HadoopShim hadoopShim = mock( HadoopShim.class );
//    Configuration config = mock( Configuration.class );
//
//    when( hadoopConfigurationBootstrap.getProvider() ).thenReturn( provider );
//    when( provider.getActiveConfiguration() ).thenReturn( hadoopConfig );
//    when( hadoopConfig.getHadoopShim() ).thenReturn( hadoopShim );
//    when( hadoopShim.createConfiguration() ).thenReturn( config );
//    when( config.get( HadoopFileSystem.FS_DEFAULT_NAME ) ).thenReturn( "hdfs://success" );
//    when( namedCluster.getHdfsHost() ).thenReturn( "success" );
//    when( namedCluster.getStorageScheme() ).thenReturn( "maprfs" );
//    when( namedCluster.getHdfsPort() ).thenReturn( "8020" );
//
//    RuntimeTestResultSummary runtimeTestResultSummary = testShimConfig.runTest( namedCluster );
//    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
//      RuntimeTestEntrySeverity.WARNING,
//      messageGetter.getMessage( TestShimConfig.TEST_SHIM_CONFIG_FS_NOMATCH_DESC ),
//      messageGetter.getMessage( TestShimConfig.TEST_SHIM_CONFIG_FS_NOMATCH_MESSAGE, "maprfs://success:8020" ) );
//    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
//  }

}
