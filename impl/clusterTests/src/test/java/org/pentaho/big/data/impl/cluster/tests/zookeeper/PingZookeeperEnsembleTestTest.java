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

package org.pentaho.big.data.impl.cluster.tests.zookeeper;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.TestMessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTest;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.expectOneEntry;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.verifyClusterTestResultEntry;

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
    verifyClusterTestResultEntry( expectOneEntry( pingZookeeperEnsembleTest.runTest( namedCluster ) ),
      ClusterTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_MESSAGE ) );
  }

  @Test
  public void testBlankPort() {
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getZooKeeperHost() ).thenReturn( zookeeperHosts );
    verifyClusterTestResultEntry( expectOneEntry( pingZookeeperEnsembleTest.runTest( namedCluster ) ),
      ClusterTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_MESSAGE ) );
  }

  @Test
  public void testNoFailures() {
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory
      .create( messageGetterFactory, host1, zookeeperPort, false, ClusterTestEntrySeverity.WARNING ) ).thenReturn(
      connectivityTest );
    when( connectivityTestFactory
      .create( messageGetterFactory, host2, zookeeperPort, false, ClusterTestEntrySeverity.WARNING ) ).thenReturn(
      connectivityTest );
    ClusterTestResultEntry clusterTestResultEntry = mock( ClusterTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( ClusterTestEntrySeverity.INFO );
    when( connectivityTest.runTest() ).thenReturn( new ArrayList<>( Arrays.asList( clusterTestResultEntry ) ) );
    List<ClusterTestResultEntry> clusterTestResultEntries = pingZookeeperEnsembleTest.runTest( namedCluster );
    assertEquals( 2, clusterTestResultEntries.size() );
    assertEquals( clusterTestResultEntry, clusterTestResultEntries.get( 0 ) );
    assertEquals( clusterTestResultEntry, clusterTestResultEntries.get( 1 ) );
  }

  @Test
  public void testOneFailure() {
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    ConnectivityTest connectivityTest2 = mock( ConnectivityTest.class );
    when( connectivityTestFactory
      .create( messageGetterFactory, host1, zookeeperPort, false, ClusterTestEntrySeverity.WARNING ) ).thenReturn(
      connectivityTest );
    when( connectivityTestFactory
      .create( messageGetterFactory, host2, zookeeperPort, false, ClusterTestEntrySeverity.WARNING ) ).thenReturn(
      connectivityTest2 );
    ClusterTestResultEntry clusterTestResultEntry = mock( ClusterTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( ClusterTestEntrySeverity.INFO );
    when( connectivityTest.runTest() ).thenReturn( new ArrayList<>( Arrays.asList( clusterTestResultEntry ) ) );
    ClusterTestResultEntry clusterTestResultEntry2 = mock( ClusterTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( ClusterTestEntrySeverity.WARNING );
    when( connectivityTest2.runTest() ).thenReturn( new ArrayList<>( Arrays.asList( clusterTestResultEntry2 ) ) );
    List<ClusterTestResultEntry> clusterTestResultEntries = pingZookeeperEnsembleTest.runTest( namedCluster );
    assertEquals( 2, clusterTestResultEntries.size() );
    assertEquals( clusterTestResultEntry, clusterTestResultEntries.get( 0 ) );
    assertEquals( clusterTestResultEntry2, clusterTestResultEntries.get( 1 ) );
  }

  @Test
  public void testAllFailures() {
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    ConnectivityTest connectivityTest2 = mock( ConnectivityTest.class );
    when( connectivityTestFactory
      .create( messageGetterFactory, host1, zookeeperPort, false, ClusterTestEntrySeverity.WARNING ) ).thenReturn(
      connectivityTest );
    when( connectivityTestFactory
      .create( messageGetterFactory, host2, zookeeperPort, false, ClusterTestEntrySeverity.WARNING ) ).thenReturn(
      connectivityTest2 );
    ClusterTestResultEntry clusterTestResultEntry = mock( ClusterTestResultEntry.class );
    when( clusterTestResultEntry.getSeverity() ).thenReturn( ClusterTestEntrySeverity.WARNING );
    when( connectivityTest.runTest() ).thenReturn( new ArrayList<>( Arrays.asList( clusterTestResultEntry ) ) );
    ClusterTestResultEntry clusterTestResultEntry2 = mock( ClusterTestResultEntry.class );
    when( clusterTestResultEntry2.getSeverity() ).thenReturn( ClusterTestEntrySeverity.WARNING );
    when( connectivityTest2.runTest() ).thenReturn( new ArrayList<>( Arrays.asList( clusterTestResultEntry2 ) ) );
    List<ClusterTestResultEntry> clusterTestResultEntries = pingZookeeperEnsembleTest.runTest( namedCluster );
    assertEquals( 3, clusterTestResultEntries.size() );
    verifyClusterTestResultEntry( clusterTestResultEntries.get( 0 ), ClusterTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_OOZIE_HOST_TEST_NO_NODES_SUCCEEDED_DESC ),
      messageGetter.getMessage( PingZookeeperEnsembleTest.PING_OOZIE_HOST_TEST_NO_NODES_SUCCEEDED_MESSAGE ) );
    assertEquals( clusterTestResultEntry, clusterTestResultEntries.get( 1 ) );
    assertEquals( clusterTestResultEntry2, clusterTestResultEntries.get( 2 ) );
  }
}
