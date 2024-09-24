/*! ******************************************************************************
 *
 * Pentaho Data Integration
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
package org.pentaho.big.data.kettle.plugins.hbase;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.never;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * User: Dzmitry Stsiapanau Date: 02/12/2016 Time: 14:10
 */

public class NamedClusterLoadSaveUtilTest {
  public static final String ZOOKEPER_HOST = "someHost";
  public static final String ZOOKEEPER_PORT = "2181";
  public static final String ZOOKEEPER_HOSTS_KEY = "zookeeper_hosts";
  public static final String ZOOKEEPER_PORT_KEY = "zookeeper_port";
  private static String xml1 =
      "<step><" + ZOOKEEPER_HOSTS_KEY + ">" + ZOOKEPER_HOST + "</" + ZOOKEEPER_HOSTS_KEY + "><" + ZOOKEEPER_PORT_KEY
          + ">" + ZOOKEEPER_PORT + "</" + ZOOKEEPER_PORT_KEY + "></step>";

  public static final String SOME_CLUSTER_NAME = "someClusterName";
  public static final String CLUSTER_NAME_KEY = "cluster_name";
  private static String xml2 =
      "<step><" + CLUSTER_NAME_KEY + ">" + SOME_CLUSTER_NAME + "</" + CLUSTER_NAME_KEY + "><" + ZOOKEEPER_HOSTS_KEY
          + ">" + ZOOKEPER_HOST + "</" + ZOOKEEPER_HOSTS_KEY + "><" + ZOOKEEPER_PORT_KEY + ">" + ZOOKEEPER_PORT + "</"
          + ZOOKEEPER_PORT_KEY + "></step>";

  // mocks
  private LogChannelInterface log;
  private NamedClusterService ncs;
  private IMetaStore metaStore;
  private Repository repository;
  private ObjectId jobId;
  private ObjectId stepId;
  private ObjectId transId;
  private NamedCluster namedCluster;

  private NamedClusterLoadSaveUtil util;
  private DocumentBuilder dBuilder;

  @Before
  public void setUp() throws Exception {
    util = new NamedClusterLoadSaveUtil();
    dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();

    // mocks
    log = mock( LogChannelInterface.class );
    namedCluster = mock( NamedCluster.class );
    metaStore = mock( IMetaStore.class );
    jobId = mock( ObjectId.class );
    stepId = mock( ObjectId.class );
    stepId = mock( ObjectId.class );
    ncs = mock( NamedClusterService.class );
    doReturn( true ).when( ncs ).contains( SOME_CLUSTER_NAME, metaStore );
    when( ncs.getClusterTemplate() ).thenReturn( namedCluster );

    repository = mock( Repository.class );
    doReturn( ZOOKEPER_HOST ).when( repository ).getJobEntryAttributeString( jobId, ZOOKEEPER_HOSTS_KEY );
    doReturn( ZOOKEEPER_PORT ).when( repository ).getJobEntryAttributeString( jobId, ZOOKEEPER_PORT_KEY );
  }

  @Test
  public void testLoadClusterConfigXML_WithoutClusterName() throws Exception {
    util.loadClusterConfig( ncs, jobId, repository, metaStore, XMLHandler.loadXMLString( dBuilder, xml1 ).getDocumentElement(), log );
    verify( ncs ).getClusterTemplate();
    verify( namedCluster ).setZooKeeperHost( ZOOKEPER_HOST );
    verify( namedCluster ).setZooKeeperPort( ZOOKEEPER_PORT );
  }

  @Test
  public void testLoadClusterConfigXML_WithClusterName() throws Exception {
    util.loadClusterConfig( ncs, jobId, repository, metaStore, XMLHandler.loadXMLString( dBuilder, xml2 ).getDocumentElement(), log );
    verify( ncs ).getNamedClusterByName( SOME_CLUSTER_NAME, metaStore );
    verify( namedCluster ).setZooKeeperHost( ZOOKEPER_HOST );
    verify( namedCluster ).setZooKeeperPort( ZOOKEEPER_PORT );
  }

  @Test
  public void testLoadClusterConfigRepo_WithoutClusterName() throws Exception {
    doReturn( null ).when( repository ).getJobEntryAttributeString( jobId, CLUSTER_NAME_KEY );

    util.loadClusterConfig( ncs, jobId, repository, metaStore, null, mock( LogChannelInterface.class ) );
    verify( ncs ).getClusterTemplate();
    verify( namedCluster ).setZooKeeperHost( ZOOKEPER_HOST );
    verify( namedCluster ).setZooKeeperPort( ZOOKEEPER_PORT );
  }

  @Test
  public void testLoadClusterConfigRepo_WithClusterName() throws Exception {
    doReturn( SOME_CLUSTER_NAME ).when( repository ).getJobEntryAttributeString( jobId, CLUSTER_NAME_KEY );

    util.loadClusterConfig( ncs, jobId, repository, metaStore, null, mock( LogChannelInterface.class ) );
    verify( ncs ).getNamedClusterByName( SOME_CLUSTER_NAME, metaStore );
    verify( namedCluster ).setZooKeeperHost( ZOOKEPER_HOST );
    verify( namedCluster ).setZooKeeperPort( ZOOKEEPER_PORT );
  }

  @Test
  public void testGetXml_WithoutClusterName() throws Exception {
    when( namedCluster.getZooKeeperHost() ).thenReturn( ZOOKEPER_HOST );
    when( namedCluster.getZooKeeperPort() ).thenReturn( ZOOKEEPER_PORT );

    StringBuilder retval = new StringBuilder();
    util.getXml( retval, ncs, namedCluster, metaStore, log );
    assertTrue( retval.toString().contains( ZOOKEEPER_PORT ) );
    assertTrue( retval.toString().contains( ZOOKEPER_HOST ) );
  }

  @Test
  public void testGetXml_WithClusterName() throws Exception {
    when( namedCluster.getName() ).thenReturn( SOME_CLUSTER_NAME );
    when( namedCluster.getZooKeeperHost() ).thenReturn( ZOOKEPER_HOST );
    when( namedCluster.getZooKeeperPort() ).thenReturn( ZOOKEEPER_PORT );

    StringBuilder retval = new StringBuilder();
    util.getXml( retval, ncs, namedCluster, metaStore, log );
    assertTrue( retval.toString().contains( ZOOKEEPER_PORT ) );
    assertTrue( retval.toString().contains( ZOOKEPER_HOST ) );
    assertTrue( retval.toString().contains( SOME_CLUSTER_NAME ) );
  }

  @Test
  public void testGetXml_WithoutZooKeeper() throws Exception {
    when( namedCluster.getName() ).thenReturn( SOME_CLUSTER_NAME );

    StringBuilder retval = new StringBuilder();
    util.getXml( retval, ncs, namedCluster, metaStore, log );
    assertFalse( retval.toString().contains( ZOOKEEPER_PORT ) );
    assertFalse( retval.toString().contains( ZOOKEPER_HOST ) );
    assertTrue( retval.toString().contains( SOME_CLUSTER_NAME ) );
  }

  @Test
  public void testGetXml_readFromMetastore() throws Exception {
    when( namedCluster.getName() ).thenReturn( SOME_CLUSTER_NAME );
    when( namedCluster.getZooKeeperHost() ).thenReturn( ZOOKEPER_HOST );
    when( namedCluster.getZooKeeperPort() ).thenReturn( ZOOKEEPER_PORT );
    when( ncs.read( SOME_CLUSTER_NAME, metaStore ) ).thenReturn( namedCluster );

    StringBuilder retval = new StringBuilder();
    util.getXml( retval, ncs, namedCluster, metaStore, log );

    verify( ncs ).read( SOME_CLUSTER_NAME, metaStore );
    assertTrue( retval.toString().contains( ZOOKEEPER_PORT ) );
    assertTrue( retval.toString().contains( ZOOKEPER_HOST ) );
    assertTrue( retval.toString().contains( SOME_CLUSTER_NAME ) );
  }

  @Test
  public void testSaveRep_WithoutClusterName() throws Exception {
    when( namedCluster.getZooKeeperHost() ).thenReturn( ZOOKEPER_HOST );
    when( namedCluster.getZooKeeperPort() ).thenReturn( ZOOKEEPER_PORT );

    util.saveRep( repository, metaStore, transId, stepId, ncs, namedCluster, log );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_HOSTS_KEY ), eq( ZOOKEPER_HOST ) );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_PORT_KEY ), eq( ZOOKEEPER_PORT ) );
  }

  @Test
  public void testSaveRep_WithClusterName() throws Exception {
    when( namedCluster.getName() ).thenReturn( SOME_CLUSTER_NAME );
    when( namedCluster.getZooKeeperHost() ).thenReturn( ZOOKEPER_HOST );
    when( namedCluster.getZooKeeperPort() ).thenReturn( ZOOKEEPER_PORT );
    when( ncs.read( SOME_CLUSTER_NAME, metaStore ) ).thenReturn( namedCluster );

    util.saveRep( repository, metaStore, transId, stepId, ncs, namedCluster, log );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_HOSTS_KEY ), eq( ZOOKEPER_HOST ) );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_PORT_KEY ), eq( ZOOKEEPER_PORT ) );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), eq( CLUSTER_NAME_KEY ), eq( SOME_CLUSTER_NAME ) );
  }

  @Test
  public void testSaveRep_WithoutZooKeeper() throws Exception {
    when( namedCluster.getName() ).thenReturn( SOME_CLUSTER_NAME );
    when( ncs.read( SOME_CLUSTER_NAME, metaStore ) ).thenReturn( namedCluster );

    util.saveRep( repository, metaStore, transId, stepId, ncs, namedCluster, log );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), eq( CLUSTER_NAME_KEY ), eq( SOME_CLUSTER_NAME ) );
    verify( repository, never() ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_HOSTS_KEY ), eq( ZOOKEPER_HOST ) );
    verify( repository, never() ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_PORT_KEY ), eq( ZOOKEEPER_PORT ) );
  }

  @Test
  public void testSaveRep_readFromMetastore() throws Exception {
    when( namedCluster.getName() ).thenReturn( SOME_CLUSTER_NAME );
    when( namedCluster.getZooKeeperHost() ).thenReturn( ZOOKEPER_HOST );
    when( namedCluster.getZooKeeperPort() ).thenReturn( ZOOKEEPER_PORT );
    when( ncs.read( SOME_CLUSTER_NAME, metaStore ) ).thenReturn( namedCluster );

    util.saveRep( repository, metaStore, transId, stepId, ncs, namedCluster, log );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_HOSTS_KEY ), eq( ZOOKEPER_HOST ) );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), anyInt(), eq( ZOOKEEPER_PORT_KEY ), eq( ZOOKEEPER_PORT ) );
    verify( repository ).saveStepAttribute( eq( transId ), eq( stepId ), eq( CLUSTER_NAME_KEY ), eq( SOME_CLUSTER_NAME ) );
  }
}

