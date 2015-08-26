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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.core.Const;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingZookeeperEnsembleTest extends BaseRuntimeTest {
  public static final String HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST =
    "zookeeperPingZookeeperEnsembleTest";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_NAME = "PingZookeeperEnsembleTest.Name";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_DESC = "PingZookeeperEnsembleTest.BlankHost.Desc";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_MESSAGE =
    "PingZookeeperEnsembleTest.BlankHost.Message";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_DESC = "PingZookeeperEnsembleTest.BlankPort.Desc";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_MESSAGE =
    "PingZookeeperEnsembleTest.BlankPort.Message";
  public static final String PING_OOZIE_HOST_TEST_NO_NODES_SUCCEEDED_DESC = "PingOozieHostTest.NoNodesSucceeded.Desc";
  public static final String PING_OOZIE_HOST_TEST_NO_NODES_SUCCEEDED_MESSAGE =
    "PingOozieHostTest.NoNodesSucceeded.Message";
  private static final Class<?> PKG = PingZookeeperEnsembleTest.class;
  private final MessageGetterFactory messageGetterFactory;
  private final MessageGetter messageGetter;
  private final ConnectivityTestFactory connectivityTestFactory;

  public PingZookeeperEnsembleTest( MessageGetterFactory messageGetterFactory,
                                    ConnectivityTestFactory connectivityTestFactory ) {
    super( NamedCluster.class, Constants.ZOOKEEPER, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.connectivityTestFactory = connectivityTestFactory;
    messageGetter = messageGetterFactory.create( PKG );
  }

  @Override public List<RuntimeTestResultEntry> runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;
    String zooKeeperHost = namedCluster.getZooKeeperHost();
    String zooKeeperPort = namedCluster.getZooKeeperPort();
    if ( Const.isEmpty( zooKeeperHost ) ) {
      return new ArrayList<RuntimeTestResultEntry>( Arrays.asList(
        new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_DESC ),
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_MESSAGE ) ) ) );
    } else if ( Const.isEmpty( zooKeeperPort ) ) {
      return new ArrayList<RuntimeTestResultEntry>( Arrays.asList(
        new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_DESC ),
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_MESSAGE ) ) ) );
    } else {
      String[] quorum = namedCluster.getZooKeeperHost().split( "," );
      List<RuntimeTestResultEntry> clusterTestResultEntries = new ArrayList<>();
      boolean hadSuccess = false;
      for ( String node : quorum ) {
        List<RuntimeTestResultEntry> nodeResults = connectivityTestFactory
          .create( messageGetterFactory, node, zooKeeperPort, false, RuntimeTestEntrySeverity.WARNING ).runTest();
        if ( RuntimeTestEntrySeverity.maxSeverityEntry( nodeResults ) != RuntimeTestEntrySeverity.WARNING ) {
          hadSuccess = true;
        }
        clusterTestResultEntries.addAll( nodeResults );
      }
      if ( !hadSuccess ) {
        List<RuntimeTestResultEntry> newClusterTestResultEntries =
          new ArrayList<>( clusterTestResultEntries.size() + 1 );
        newClusterTestResultEntries.add( new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_OOZIE_HOST_TEST_NO_NODES_SUCCEEDED_DESC ),
          messageGetter.getMessage( PING_OOZIE_HOST_TEST_NO_NODES_SUCCEEDED_MESSAGE ) ) );
        newClusterTestResultEntries.addAll( clusterTestResultEntries );
        clusterTestResultEntries = newClusterTestResultEntries;
      }
      return clusterTestResultEntries;
    }
  }
}
