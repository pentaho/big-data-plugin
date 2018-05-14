/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.cluster.tests.zookeeper;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTest;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by bryan on 8/24/15.
 */
public class PingZookeeperEnsembleTestTest {
  private MessageGetterFactory messageGetterFactory;
  private ConnectivityTestFactory connectivityTestFactory;
  private PingZookeeperEnsembleTest pingZookeeperEnsembleTest;
  private NamedCluster namedCluster;
  private String zookeeperHosts;
  private String zookeeperPort;
  private MessageGetter messageGetter;
  private String host1;
  private String host2;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( PingZookeeperEnsembleTest.class );
    connectivityTestFactory = mock( ConnectivityTestFactory.class );
    pingZookeeperEnsembleTest = new PingZookeeperEnsembleTest( messageGetterFactory, connectivityTestFactory );
    host1 = "host1";
    host2 = "host2";
    zookeeperHosts = host1 + "," + host2;
    zookeeperPort = "2181";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getZooKeeperHost() ).thenReturn( zookeeperHosts );
    when( namedCluster.getZooKeeperPort() ).thenReturn( zookeeperPort );
  }

  @Test
  public void testBlankHost() {
    namedCluster = mock( NamedCluster.class );
    RuntimeTestResultSummary runtimeTestResultSummary = pingZookeeperEnsembleTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_MESSAGE ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testBlankPort() {
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getZooKeeperHost() ).thenReturn( zookeeperHosts );
    RuntimeTestResultSummary runtimeTestResultSummary = pingZookeeperEnsembleTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_MESSAGE ) );
  }

  @Test
  public void testNoFailures() {
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory
      .create( messageGetterFactory, host1, zookeeperPort, false, RuntimeTestEntrySeverity.WARNING ) ).thenReturn(
        connectivityTest );
    when( connectivityTestFactory
      .create( messageGetterFactory, host2, zookeeperPort, false, RuntimeTestEntrySeverity.WARNING ) ).thenReturn(
        connectivityTest );
    RuntimeTestResultEntry clusterTestResultEntry = mock( RuntimeTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.INFO );
    when( connectivityTest.runTest() ).thenReturn( clusterTestResultEntry );
    String testDescription = "test-description";
    when( clusterTestResultEntry.getDescription() ).thenReturn( testDescription );
    RuntimeTestResultSummary runtimeTestResultSummary = pingZookeeperEnsembleTest.runTest( namedCluster );
    List<RuntimeTestResultEntry> clusterTestResultEntries = runtimeTestResultSummary
      .getRuntimeTestResultEntries();
    assertEquals( 2, clusterTestResultEntries.size() );
    assertEquals( testDescription, clusterTestResultEntries.get( 0 ).getDescription() );
    assertEquals( testDescription, clusterTestResultEntries.get( 1 ).getDescription() );
  }

  @Test
  public void testOneFailure() {
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    ConnectivityTest connectivityTest2 = mock( ConnectivityTest.class );
    when( connectivityTestFactory
      .create( messageGetterFactory, host1, zookeeperPort, false, RuntimeTestEntrySeverity.WARNING ) ).thenReturn(
        connectivityTest );
    when( connectivityTestFactory
      .create( messageGetterFactory, host2, zookeeperPort, false, RuntimeTestEntrySeverity.WARNING ) ).thenReturn(
        connectivityTest2 );
    RuntimeTestResultEntry clusterTestResultEntry = mock( RuntimeTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.INFO );
    when( connectivityTest.runTest() ).thenReturn( clusterTestResultEntry );
    RuntimeTestResultEntry clusterTestResultEntry2 = mock( RuntimeTestResultEntry.class );
    when( clusterTestResultEntry2.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.WARNING );
    when( connectivityTest2.runTest() ).thenReturn( clusterTestResultEntry2 );
    String testDescription = "test-description";
    when( clusterTestResultEntry.getDescription() ).thenReturn( testDescription );
    String testDescription2 = "test-description2";
    when( clusterTestResultEntry2.getDescription() ).thenReturn( testDescription2 );
    RuntimeTestResultSummary runtimeTestResultSummary = pingZookeeperEnsembleTest.runTest( namedCluster );
    List<RuntimeTestResultEntry> clusterTestResultEntries = runtimeTestResultSummary
      .getRuntimeTestResultEntries();
    assertEquals( 2, clusterTestResultEntries.size() );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(), RuntimeTestEntrySeverity.WARNING,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_SOME_NODES_FAILED_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_SOME_NODES_FAILED_MESSAGE,
        host2 ) );
    assertEquals( testDescription, clusterTestResultEntries.get( 0 ).getDescription() );
    assertEquals( testDescription2, clusterTestResultEntries.get( 1 ).getDescription() );
  }

  @Test
  public void testAllFailures() {
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    ConnectivityTest connectivityTest2 = mock( ConnectivityTest.class );
    when( connectivityTestFactory
      .create( messageGetterFactory, host1, zookeeperPort, false, RuntimeTestEntrySeverity.WARNING ) ).thenReturn(
        connectivityTest );
    when( connectivityTestFactory
      .create( messageGetterFactory, host2, zookeeperPort, false, RuntimeTestEntrySeverity.WARNING ) ).thenReturn(
        connectivityTest2 );
    RuntimeTestResultEntry clusterTestResultEntry = mock( RuntimeTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.WARNING );
    when( connectivityTest.runTest() ).thenReturn( clusterTestResultEntry );
    RuntimeTestResultEntry clusterTestResultEntry2 = mock( RuntimeTestResultEntry.class );
    when( clusterTestResultEntry2.getSeverity() ).thenReturn( RuntimeTestEntrySeverity.WARNING );
    when( connectivityTest2.runTest() ).thenReturn( clusterTestResultEntry2 );
    String testDescription = "test-description";
    when( clusterTestResultEntry.getDescription() ).thenReturn( testDescription );
    String testDescription2 = "test-description2";
    when( clusterTestResultEntry2.getDescription() ).thenReturn( testDescription2 );
    RuntimeTestResultSummary runtimeTestResultSummary = pingZookeeperEnsembleTest.runTest( namedCluster );
    List<RuntimeTestResultEntry> clusterTestResultEntries = runtimeTestResultSummary
      .getRuntimeTestResultEntries();
    assertEquals( 2, clusterTestResultEntries.size() );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(), RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_NO_NODES_SUCCEEDED_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_NO_NODES_SUCCEEDED_MESSAGE,
        host1 + ", " + host2 ) );
    assertEquals( testDescription, clusterTestResultEntries.get( 0 ).getDescription() );
    assertEquals( testDescription2, clusterTestResultEntries.get( 1 ).getDescription() );
  }
}
