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

package org.pentaho.big.data.kettle.plugins.mapreduce.entry;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.mapreduce.entry.hadoop.JobEntryHadoopJobExecutor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.eq;

/**
 * Created by bryan on 1/13/16.
 */
public class NamedClusterLoadSaveUtilTest {
  private NamedClusterService namedClusterService;
  private RuntimeTestActionService runtimeTestActionService;
  private RuntimeTester runtimeTester;
  private NamedClusterServiceLocator namedClusterServiceLocator;
  private IMetaStore metaStore;
  private NamedCluster namedCluster;
  private Repository repository;
  private ObjectId objectId;
  private NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;
  private LogChannelInterface logChannelInterface;
  private ObjectId id_job;

  @Before
  public void setup() {
    namedClusterService = mock( NamedClusterService.class );
    runtimeTestActionService = mock( RuntimeTestActionService.class );
    runtimeTester = mock( RuntimeTester.class );
    namedClusterServiceLocator = mock( NamedClusterServiceLocator.class );
    metaStore = mock( IMetaStore.class );
    namedCluster = mock( NamedCluster.class );
    repository = mock( Repository.class );
    when( repository.getMetaStore() ).thenReturn( metaStore );
    objectId = mock( ObjectId.class );
    id_job = mock( ObjectId.class );
    logChannelInterface = mock( LogChannelInterface.class );
    namedClusterLoadSaveUtil = new NamedClusterLoadSaveUtil();
  }

  private void addTag( StringBuilder builder, String tag, String value ) {
    builder.append( "<" ).append( tag ).append( ">" );
    builder.append( value );
    builder.append( "</" ).append( tag ).append( ">" );
  }

  private Node parseNamedClusterXml( String namedClusterXml )
    throws ParserConfigurationException, IOException, SAXException {
    return DocumentBuilderFactory.newInstance().newDocumentBuilder()
      .parse( new InputSource( new StringReader( "<nc>" + namedClusterXml + "</nc>" ) ) ).getFirstChild();
  }

  @Test
  public void testLoadClusterConfigFoundXml()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException {
    String testName = "testName";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testName, metaStore ) ).thenReturn( namedCluster );
    StringBuilder stringBuilder = new StringBuilder( "<job>" );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.CLUSTER_NAME, testName );
    stringBuilder.append( "</job>" );
    String xml = stringBuilder.toString();
    Element documentElement =
      DocumentBuilderFactory.newInstance().newDocumentBuilder().parse( new ByteArrayInputStream( xml.getBytes() ) )
        .getDocumentElement();
    assertEquals( namedCluster, namedClusterLoadSaveUtil
      .loadClusterConfig( namedClusterService, null, null, metaStore, documentElement, logChannelInterface ) );
  }

  @Test
  public void testLoadClusterConfigNotFoundXml()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException {
    String testName = "testName";
    String testHost = "testHost";
    String hdfsPort = "8080";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "8081";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( false );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    StringBuilder stringBuilder = new StringBuilder( "<job>" );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.CLUSTER_NAME, testName );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.HDFS_HOSTNAME, testHost );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.HDFS_PORT, hdfsPort );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.JOB_TRACKER_HOSTNAME, jobTrackerHost );
    addTag( stringBuilder, JobEntryHadoopJobExecutor.JOB_TRACKER_PORT, jobTrackerPort );
    stringBuilder.append( "</job>" );
    String xml = stringBuilder.toString();
    Element documentElement =
      DocumentBuilderFactory.newInstance().newDocumentBuilder().parse( new ByteArrayInputStream( xml.getBytes() ) )
        .getDocumentElement();
    assertEquals( namedCluster, namedClusterLoadSaveUtil
      .loadClusterConfig( namedClusterService, null, null, metaStore, documentElement, logChannelInterface ) );
    verify( namedCluster ).setHdfsHost( testHost );
    verify( namedCluster ).setHdfsPort( hdfsPort );
    verify( namedCluster ).setJobTrackerHost( jobTrackerHost );
    verify( namedCluster ).setJobTrackerPort( jobTrackerPort );
  }

  @Test
  public void testLoadClusterConfigFoundRepo()
    throws ParserConfigurationException, IOException, SAXException, MetaStoreException, KettleException {
    String testName = "testName";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testName, metaStore ) ).thenReturn( namedCluster );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.CLUSTER_NAME ) )
      .thenReturn( testName );
    assertEquals( namedCluster, namedClusterLoadSaveUtil
      .loadClusterConfig( namedClusterService, objectId, repository, metaStore, null, logChannelInterface ) );
  }

  @Test
  public void testLoadClusterConfigNotFoundRepo() throws ParserConfigurationException, IOException, SAXException,
    MetaStoreException, KettleException {
    String testName = "testName";
    String testHost = "testHost";
    String hdfsPort = "8080";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "8081";
    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( false );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.CLUSTER_NAME ) ).thenReturn(
      testName );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_HOSTNAME ) ).thenReturn(
      testHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_PORT ) ).thenReturn(
      hdfsPort );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_HOSTNAME ) )
      .thenReturn( jobTrackerHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_PORT ) ).thenReturn(
      jobTrackerPort );
    assertEquals( namedCluster, namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, objectId, repository,
      metaStore, null, logChannelInterface ) );
    verify( namedCluster ).setHdfsHost( testHost );
    verify( namedCluster ).setHdfsPort( hdfsPort );
    verify( namedCluster ).setJobTrackerHost( jobTrackerHost );
    verify( namedCluster ).setJobTrackerPort( jobTrackerPort );
  }

  @Test
  public void testLoadClusterConfigNoNodeOrRep() {
    assertNull( namedClusterLoadSaveUtil
      .loadClusterConfig( namedClusterService, id_job, null, metaStore, null, logChannelInterface ) );
  }

  @Test
  public void testLoadClusterConfigExceptions() throws MetaStoreException, KettleException {
    String testName = "testName";
    String testHost = "testHost";
    String hdfsPort = "8080";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "8081";
    MetaStoreException metaStoreException = new MetaStoreException( "msg" );

    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testName, metaStore ) ).thenThrow( metaStoreException );

    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.CLUSTER_NAME ) ).thenReturn(
      testName );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_HOSTNAME ) ).thenReturn(
      testHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_PORT ) ).thenReturn(
      hdfsPort );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_HOSTNAME ) )
      .thenReturn( jobTrackerHost );
    KettleException kettleException = new KettleException( "msg2" );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_PORT ) ).thenThrow(
      kettleException );
    assertEquals( namedCluster, namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, objectId, repository,
      metaStore, null, logChannelInterface ) );
    verify( logChannelInterface ).logDebug( metaStoreException.getMessage(), metaStoreException );
    verify( logChannelInterface ).logError( kettleException.getMessage(), kettleException );
    verify( namedCluster ).setHdfsHost( testHost );
    verify( namedCluster ).setHdfsPort( hdfsPort );
    verify( namedCluster ).setJobTrackerHost( jobTrackerHost );
  }

  @Test
  public void testLoadClusterConfigNoMetastore() throws MetaStoreException, KettleException {
    String testName = "testName";
    String testHost = "testHost";
    String hdfsPort = "8080";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "8081";
    MetaStoreException metaStoreException = new MetaStoreException( "msg" );

    when( namedClusterService.contains( testName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testName, metaStore ) ).thenThrow( metaStoreException );

    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.CLUSTER_NAME ) ).thenReturn(
      testName );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_HOSTNAME ) ).thenReturn(
      testHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.HDFS_PORT ) ).thenReturn(
      hdfsPort );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_HOSTNAME ) )
      .thenReturn( jobTrackerHost );
    when( repository.getJobEntryAttributeString( objectId, JobEntryHadoopJobExecutor.JOB_TRACKER_PORT ) )
      .thenReturn( jobTrackerPort );
    assertEquals( namedCluster, namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, objectId, repository,
      null, null, logChannelInterface ) );
    verify( namedClusterService, never() ).read( eq( testName ), any( IMetaStore.class ) );
    verify( namedCluster ).setHdfsHost( testHost );
    verify( namedCluster ).setHdfsPort( hdfsPort );
    verify( namedCluster ).setJobTrackerHost( jobTrackerHost );
    verify( namedCluster ).setJobTrackerPort( jobTrackerPort );
  }

  @Test
  public void testGetXmlNamedCluster_NoNPEWhenNCIsNull() {
    StringBuilder ncString = new StringBuilder();
    try {
      namedClusterLoadSaveUtil.getXmlNamedCluster( null, namedClusterService, null, logChannelInterface, ncString );
      assertEquals( "It should not be added any NamedCluster info but it was:" + ncString.toString(), 0, ncString
        .length() );
    } catch ( NullPointerException ex ) {
      fail( "NPE occured but should not: " + ex );
    }
  }

  @Test
  public void testSaveNamedClusterRep_NoNPEWhenNCIsNull() throws KettleException {
    try {
      namedClusterLoadSaveUtil.saveNamedClusterRep( null, namedClusterService, repository, metaStore, objectId,
        objectId, logChannelInterface );
      verify( repository, never() ).saveJobEntryAttribute( any( ObjectId.class ), any( ObjectId.class ), anyString(),
        anyString() );
    } catch ( NullPointerException ex ) {
      fail( "NPE occured but should not: " + ex );
    }
  }

  @Test
  public void testSaveNamedClusterRep() throws KettleException, MetaStoreException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    NamedCluster readNamedCluster = mock( NamedCluster.class );

    when( readNamedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( readNamedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( readNamedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( readNamedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    when( namedClusterService.contains( testNcName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testNcName, metaStore ) ).thenReturn( readNamedCluster );

    namedClusterLoadSaveUtil
      .saveNamedClusterRep( namedCluster, namedClusterService, repository, metaStore, id_job, objectId,
        logChannelInterface );

    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.CLUSTER_NAME, testNcName );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_HOSTNAME, hdfsHost );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_PORT, hdfsPort );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME, jobTrackerHost );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT, jobTrackerPort );
  }

  @Test
  public void testSaveNamedClusterRepNoName() throws KettleException, MetaStoreException {
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    namedClusterLoadSaveUtil
      .saveNamedClusterRep( namedCluster, namedClusterService, repository, metaStore, id_job, objectId,
        logChannelInterface );

    verify( repository, never() )
      .saveJobEntryAttribute( eq( id_job ), eq( objectId ), eq( NamedClusterLoadSaveUtil.CLUSTER_NAME ), anyString() );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_HOSTNAME, hdfsHost );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_PORT, hdfsPort );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME, jobTrackerHost );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT, jobTrackerPort );
  }

  @Test
  public void testSaveNamedClusterRepExceptionReading() throws KettleException, MetaStoreException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    when( namedClusterService.contains( testNcName, metaStore ) ).thenReturn( true );
    MetaStoreException metaStoreException = new MetaStoreException( "msg" );
    when( namedClusterService.read( testNcName, metaStore ) ).thenThrow( metaStoreException );

    namedClusterLoadSaveUtil
      .saveNamedClusterRep( namedCluster, namedClusterService, repository, metaStore, id_job, objectId,
        logChannelInterface );

    verify( logChannelInterface ).logDebug( metaStoreException.getMessage(), metaStoreException );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.CLUSTER_NAME, testNcName );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_HOSTNAME, hdfsHost );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_PORT, hdfsPort );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME, jobTrackerHost );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT, jobTrackerPort );
  }

  @Test
  public void testSaveNamedClusterRepNotContains() throws KettleException, MetaStoreException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    when( namedClusterService.contains( testNcName, metaStore ) ).thenReturn( false );

    namedClusterLoadSaveUtil
      .saveNamedClusterRep( namedCluster, namedClusterService, repository, metaStore, id_job, objectId,
        logChannelInterface );

    verify( namedClusterService, never() ).read( testNcName, metaStore );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.CLUSTER_NAME, testNcName );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_HOSTNAME, hdfsHost );
    verify( repository ).saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.HDFS_PORT, hdfsPort );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME, jobTrackerHost );
    verify( repository )
      .saveJobEntryAttribute( id_job, objectId, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT, jobTrackerPort );
  }

  @Test
  public void testGetXmlNamedCluster()
    throws KettleException, MetaStoreException, IOException, SAXException, ParserConfigurationException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    NamedCluster readNamedCluster = mock( NamedCluster.class );

    when( readNamedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( readNamedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( readNamedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( readNamedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    when( namedClusterService.contains( testNcName, metaStore ) ).thenReturn( true );
    when( namedClusterService.read( testNcName, metaStore ) ).thenReturn( readNamedCluster );

    StringBuilder stringBuilder = new StringBuilder();
    namedClusterLoadSaveUtil
      .getXmlNamedCluster( namedCluster, namedClusterService, metaStore, logChannelInterface, stringBuilder );

    Node node = parseNamedClusterXml( stringBuilder.toString() );
    assertEquals( testNcName, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.CLUSTER_NAME ) );
    assertEquals( hdfsHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_HOSTNAME ) );
    assertEquals( hdfsPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_PORT ) );
    assertEquals( jobTrackerHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME ) );
    assertEquals( jobTrackerPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT ) );
  }

  @Test
  public void testGetXmlNamedClusterEmptyName()
    throws KettleException, MetaStoreException, IOException, SAXException, ParserConfigurationException {
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    StringBuilder stringBuilder = new StringBuilder();
    namedClusterLoadSaveUtil
      .getXmlNamedCluster( namedCluster, namedClusterService, metaStore, logChannelInterface, stringBuilder );

    Node node = parseNamedClusterXml( stringBuilder.toString() );
    assertEquals( null, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.CLUSTER_NAME ) );
    assertEquals( hdfsHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_HOSTNAME ) );
    assertEquals( hdfsPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_PORT ) );
    assertEquals( jobTrackerHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME ) );
    assertEquals( jobTrackerPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT ) );
  }

  @Test
  public void testGetXmlNamedClusterNullMetastore()
    throws KettleException, MetaStoreException, IOException, SAXException, ParserConfigurationException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    StringBuilder stringBuilder = new StringBuilder();
    namedClusterLoadSaveUtil
      .getXmlNamedCluster( namedCluster, namedClusterService, null, logChannelInterface, stringBuilder );

    Node node = parseNamedClusterXml( stringBuilder.toString() );
    assertEquals( testNcName, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.CLUSTER_NAME ) );
    assertEquals( hdfsHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_HOSTNAME ) );
    assertEquals( hdfsPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_PORT ) );
    assertEquals( jobTrackerHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME ) );
    assertEquals( jobTrackerPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT ) );
  }

  @Test
  public void testGetXmlNamedClusterNotContains()
    throws KettleException, MetaStoreException, IOException, SAXException, ParserConfigurationException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    when( namedClusterService.contains( testNcName, metaStore ) ).thenReturn( false );

    StringBuilder stringBuilder = new StringBuilder();
    namedClusterLoadSaveUtil
      .getXmlNamedCluster( namedCluster, namedClusterService, metaStore, logChannelInterface, stringBuilder );

    Node node = parseNamedClusterXml( stringBuilder.toString() );
    verify( namedClusterService, never() ).read( anyString(), eq( metaStore ) );
    assertEquals( testNcName, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.CLUSTER_NAME ) );
    assertEquals( hdfsHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_HOSTNAME ) );
    assertEquals( hdfsPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_PORT ) );
    assertEquals( jobTrackerHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME ) );
    assertEquals( jobTrackerPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT ) );
  }

  @Test
  public void testGetXmlNamedClusterMetastoreException()
    throws KettleException, MetaStoreException, IOException, SAXException, ParserConfigurationException {
    String testNcName = "testNcName";
    String hdfsHost = "hdfsHost";
    String hdfsPort = "hdfsPort";
    String jobTrackerHost = "jobTrackerHost";
    String jobTrackerPort = "jobTrackerPort";

    when( namedCluster.getName() ).thenReturn( testNcName );

    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );

    when( namedClusterService.contains( testNcName, metaStore ) ).thenReturn( true );
    MetaStoreException metaStoreException = new MetaStoreException( "msg" );
    when( namedClusterService.read( testNcName, metaStore ) ).thenThrow( metaStoreException );

    StringBuilder stringBuilder = new StringBuilder();
    namedClusterLoadSaveUtil
      .getXmlNamedCluster( namedCluster, namedClusterService, metaStore, logChannelInterface, stringBuilder );

    Node node = parseNamedClusterXml( stringBuilder.toString() );
    verify( logChannelInterface ).logDebug( metaStoreException.getMessage(), metaStoreException );
    assertEquals( testNcName, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.CLUSTER_NAME ) );
    assertEquals( hdfsHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_HOSTNAME ) );
    assertEquals( hdfsPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.HDFS_PORT ) );
    assertEquals( jobTrackerHost, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_HOSTNAME ) );
    assertEquals( jobTrackerPort, XMLHandler.getTagValue( node, NamedClusterLoadSaveUtil.JOB_TRACKER_PORT ) );
  }
}
