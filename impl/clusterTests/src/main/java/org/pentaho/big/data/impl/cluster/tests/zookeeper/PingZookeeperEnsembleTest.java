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

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import java.util.ArrayList;
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
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_NO_NODES_SUCCEEDED_DESC =
    "PingZookeeperEnsembleTest.NoNodesSucceeded.Desc";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_NO_NODES_SUCCEEDED_MESSAGE =
    "PingZookeeperEnsembleTest.NoNodesSucceeded.Message";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_SOME_NODES_FAILED_DESC =
    "PingZookeeperEnsembleTest.SomeNodesFailed.Desc";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_SOME_NODES_FAILED_MESSAGE =
    "PingZookeeperEnsembleTest.SomeNodesFailed.Message";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_ALL_NODES_SUCCEEDED_DESC =
    "PingZookeeperEnsembleTest.AllNodesSucceeded.Desc";
  public static final String PING_ZOOKEEPER_ENSEMBLE_TEST_ALL_NODES_SUCCEEDED_MESSAGE =
    "PingZookeeperEnsembleTest.AllNodesSucceeded.Message";
  private static final Class<?> PKG = PingZookeeperEnsembleTest.class;
  private final MessageGetterFactory messageGetterFactory;
  protected final MessageGetter messageGetter;
  private final ConnectivityTestFactory connectivityTestFactory;

  public PingZookeeperEnsembleTest( MessageGetterFactory messageGetterFactory,
                                    ConnectivityTestFactory connectivityTestFactory ) {
    super( NamedCluster.class, Constants.ZOOKEEPER, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.connectivityTestFactory = connectivityTestFactory;
    messageGetter = messageGetterFactory.create( PKG );
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;

    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );

    String zooKeeperHost = variables.environmentSubstitute( namedCluster.getZooKeeperHost() );
    String zooKeeperPort = variables.environmentSubstitute( namedCluster.getZooKeeperPort() );
    if ( Const.isEmpty( zooKeeperHost ) ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_DESC ),
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_HOST_MESSAGE ),
          ClusterRuntimeTestEntry.DocAnchor.ZOOKEEPER ) );
    } else if ( Const.isEmpty( zooKeeperPort ) ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_DESC ),
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_BLANK_PORT_MESSAGE ),
          ClusterRuntimeTestEntry.DocAnchor.ZOOKEEPER ) );
    } else {
      String[] quorum = zooKeeperHost.split( "," );
      List<RuntimeTestResultEntry> clusterTestResultEntries = new ArrayList<>();
      int failedNodes = 0;
      StringBuilder failedNodeString = new StringBuilder();
      for ( String node : quorum ) {
        RuntimeTestResultEntry nodeResults = new ClusterRuntimeTestEntry( messageGetterFactory, connectivityTestFactory
          .create( messageGetterFactory, node, zooKeeperPort, false, RuntimeTestEntrySeverity.WARNING ).runTest(),
          ClusterRuntimeTestEntry.DocAnchor.ZOOKEEPER );
        if ( nodeResults.getSeverity() == RuntimeTestEntrySeverity.WARNING ) {
          failedNodeString.append( node ).append( ", " );
          failedNodes++;
        }
        clusterTestResultEntries.add( nodeResults );
      }
      if ( failedNodes > 0 ) {
        failedNodeString.setLength( failedNodeString.length() - 2 );
      }
      RuntimeTestResultEntryImpl overallResult;
      if ( failedNodes == quorum.length ) {
        overallResult = new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_NO_NODES_SUCCEEDED_DESC ),
          messageGetter
            .getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_NO_NODES_SUCCEEDED_MESSAGE, failedNodeString.toString() ),
          ClusterRuntimeTestEntry.DocAnchor.ZOOKEEPER );
      } else if ( failedNodes > 0 ) {
        overallResult = new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_SOME_NODES_FAILED_DESC ),
          messageGetter
            .getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_SOME_NODES_FAILED_MESSAGE, failedNodeString.toString() ),
          ClusterRuntimeTestEntry.DocAnchor.ZOOKEEPER );
      } else {
        overallResult = new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.INFO,
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_ALL_NODES_SUCCEEDED_DESC ),
          messageGetter.getMessage( PING_ZOOKEEPER_ENSEMBLE_TEST_ALL_NODES_SUCCEEDED_MESSAGE ),
          ClusterRuntimeTestEntry.DocAnchor.ZOOKEEPER );
      }
      return new RuntimeTestResultSummaryImpl( overallResult, clusterTestResultEntries );
    }
  }
}
