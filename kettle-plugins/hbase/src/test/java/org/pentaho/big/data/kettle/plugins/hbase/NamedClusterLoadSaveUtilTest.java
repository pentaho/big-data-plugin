/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.big.data.kettle.plugins.hbase;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * User: Dzmitry Stsiapanau Date: 02/12/2016 Time: 14:10
 */

public class NamedClusterLoadSaveUtilTest {
  public static final String ZOOKEPER_HOST = "someHost";
  public static final String ZOOKEEPER_PORT = "2181";
  public static final String ZOOKEEPER_HOSTS_KEY = "zookeeper_hosts";
  public static final String ZOOKEEPER_PORT_KEY = "zookeeper_port";
  private static String xml1 = "<step><" + ZOOKEEPER_HOSTS_KEY + ">" + ZOOKEPER_HOST + "</" + ZOOKEEPER_HOSTS_KEY
    + "><" + ZOOKEEPER_PORT_KEY + ">"
    + ZOOKEEPER_PORT + "</" + ZOOKEEPER_PORT_KEY + "></step>";
  public static final String SOME_CLUSTER_NAME = "someClusterName";
  public static final String CLUSTER_NAME_KEY = "cluster_name";
  private static String xml2 =
    "<step><" + CLUSTER_NAME_KEY + ">" + SOME_CLUSTER_NAME + "</" + CLUSTER_NAME_KEY + "><" + ZOOKEEPER_HOSTS_KEY + ">"
      + ZOOKEPER_HOST
      + "</" + ZOOKEEPER_HOSTS_KEY + "><" + ZOOKEEPER_PORT_KEY + ">"
      + ZOOKEEPER_PORT + "</" + ZOOKEEPER_PORT_KEY + "></step>";

  private NamedClusterService ncs = spy( new NamedClusterManager() );
  private IMetaStore metaStore = mock( IMetaStore.class );
  private Repository repository = mock( Repository.class );
  private ObjectId objectId = mock( ObjectId.class );

  @Before
  public void setUp() throws Exception {
    ncs = spy( new NamedClusterManager() );
    metaStore = mock( IMetaStore.class );
    repository = mock( Repository.class );
    objectId = mock( ObjectId.class );
  }

  @Test
  public void testLoadClusterConfigXML() throws Exception {
    NamedClusterLoadSaveUtil util = new NamedClusterLoadSaveUtil();
    NamedCluster nc = util.loadClusterConfig( ncs, objectId, repository, metaStore,
      XMLHandler.loadXMLString( xml1 ).getDocumentElement(),
      mock( LogChannelInterface.class ) );
    assertEquals( "", nc.getName() );
    assertEquals( ZOOKEPER_HOST, nc.getZooKeeperHost() );
    assertEquals( ZOOKEEPER_PORT, nc.getZooKeeperPort() );
    nc = util.loadClusterConfig( ncs, mock( ObjectId.class ), mock(
      Repository.class ), metaStore, XMLHandler.loadXMLString( xml2 ).getDocumentElement(),
      mock( LogChannelInterface.class ) );
    assertEquals( SOME_CLUSTER_NAME, nc.getName() );
    assertEquals( ZOOKEPER_HOST, nc.getZooKeeperHost() );
    assertEquals( ZOOKEEPER_PORT, nc.getZooKeeperPort() );
  }

  @Test
  public void testLoadClusterConfigRepo() throws Exception {
    NamedClusterLoadSaveUtil util = new NamedClusterLoadSaveUtil();
    NamedCluster nc;
    NamedCluster returnNC = ncs.getClusterTemplate();
    returnNC.setName( SOME_CLUSTER_NAME );
    returnNC.setZooKeeperHost( ZOOKEPER_HOST );
    returnNC.setZooKeeperPort( ZOOKEEPER_PORT );

    doReturn( true ).when( ncs ).contains( SOME_CLUSTER_NAME, metaStore );
    doReturn( returnNC ).when( ncs ).read( SOME_CLUSTER_NAME, metaStore );
    doReturn( null ).when( repository ).getJobEntryAttributeString( objectId, CLUSTER_NAME_KEY );
    doReturn( ZOOKEPER_HOST ).when( repository ).getJobEntryAttributeString( objectId, ZOOKEEPER_HOSTS_KEY );
    doReturn( ZOOKEEPER_PORT ).when( repository ).getJobEntryAttributeString( objectId, ZOOKEEPER_PORT_KEY );

    nc = util.loadClusterConfig( ncs, objectId, repository, metaStore, null, mock( LogChannelInterface.class ) );
    assertEquals( "", nc.getName() );
    assertEquals( ZOOKEPER_HOST, nc.getZooKeeperHost() );
    assertEquals( ZOOKEEPER_PORT, nc.getZooKeeperPort() );

    returnNC = ncs.getClusterTemplate();
    returnNC.setName( SOME_CLUSTER_NAME );
    returnNC.setZooKeeperHost( ZOOKEPER_HOST );
    returnNC.setZooKeeperPort( ZOOKEEPER_PORT );

    doReturn( true ).when( ncs ).contains( SOME_CLUSTER_NAME, metaStore );
    doReturn( returnNC ).when( ncs ).read( SOME_CLUSTER_NAME, metaStore );
    doReturn( SOME_CLUSTER_NAME ).when( repository ).getJobEntryAttributeString( objectId, CLUSTER_NAME_KEY );

    nc = util.loadClusterConfig( ncs, objectId, repository, metaStore, null, mock( LogChannelInterface.class ) );
    assertEquals( SOME_CLUSTER_NAME, nc.getName() );
    assertEquals( ZOOKEPER_HOST, nc.getZooKeeperHost() );
    assertEquals( ZOOKEEPER_PORT, nc.getZooKeeperPort() );
  }
}
