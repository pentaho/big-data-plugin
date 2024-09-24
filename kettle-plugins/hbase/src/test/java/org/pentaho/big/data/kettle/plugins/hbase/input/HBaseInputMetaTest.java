/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/
package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.hbase.LogInjector;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.hbase.ServiceStatus;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.trans.steps.loadsave.MemoryRepository;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.imageio.metadata.IIOMetadataNode;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class HBaseInputMetaTest {

  @InjectMocks HBaseInputMeta hBaseInputMeta;
  @Mock NamedCluster namedCluster;
  @Mock NamedClusterServiceLocator namedClusterServiceLocator;
  @Mock HBaseService hBaseService;
  @Mock MappingDefinition mappingDefinition;
  @Mock NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;
  @Mock IMetaStore metaStore;
  @Mock NamedClusterService namedClusterService;

  /**
   * actual for bug BACKLOG-9529
   */
  @Test
  public void testLogSuccessfulForGetXml() throws Exception {
    HBaseInputMeta spy = Mockito.spy( hBaseInputMeta );
    spy.setNamedCluster( namedCluster );

    LoggingBuffer loggingBuffer = LogInjector.setMockForLoggingBuffer();

    Mockito.doThrow( new KettleException( "Unexpected error occured" ) ).when( spy ).applyInjection( any() );
    spy.getXML();
    verify( loggingBuffer, atLeast( 1 ) ).addLogggingEvent( any() );
  }

  /**
   * actual for bug BACKLOG-9629
   */
  @SuppressWarnings( "unchecked" )
  @Test
  public void testApplyInjectionDefinitionsExists() throws Exception {
    HBaseInputMeta hBaseInputMetaSpy = Mockito.spy( hBaseInputMeta );
    hBaseInputMetaSpy.setNamedCluster( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class, null ) ).thenReturn( hBaseService );
    hBaseInputMetaSpy.setMappingDefinition( mappingDefinition );
    List list = mock( List.class );
    hBaseInputMetaSpy.setOutputFieldsDefinition( list );
    hBaseInputMetaSpy.setFiltersDefinition( list );
    Mockito.doReturn( list ).when( hBaseInputMetaSpy ).createOutputFieldsDefinition( any(), any() );
    Mockito.doReturn( list ).when( hBaseInputMetaSpy ).createColumnFiltersFromDefinition( any() );
    Mockito.doReturn( null ).when( hBaseInputMetaSpy ).getMapping( any(), any() );

    hBaseInputMetaSpy.getXML();
    verify( hBaseInputMetaSpy, times( 1 ) ).setMapping( any() );
    verify( hBaseInputMetaSpy, times( 1 ) ).setOutputFields( any() );
    verify( hBaseInputMetaSpy, times( 1 ) ).setColumnFilters( any() );
  }

  /**
   * actual for bug BACKLOG-9629
   */
  @Test
  public void testApplyInjectionDefinitionsNull() throws Exception {
    HBaseInputMeta hBaseInputMetaSpy = Mockito.spy( hBaseInputMeta );
    hBaseInputMetaSpy.setNamedCluster( namedCluster );
    hBaseInputMetaSpy.setMappingDefinition( null );
    hBaseInputMetaSpy.setOutputFieldsDefinition( null );
    hBaseInputMetaSpy.setFiltersDefinition( null );

    hBaseInputMetaSpy.getXML();
    verify( hBaseInputMetaSpy, times( 0 ) ).setMapping( any() );
    verify( hBaseInputMetaSpy, times( 0 ) ).getMapping();
    verify( hBaseInputMetaSpy, times( 0 ) ).setOutputFields( any() );
    verify( hBaseInputMetaSpy, times( 0 ) ).setColumnFilters( any() );
  }

  @Test
  public void testLoadXmlDoesntBubleUpException() throws Exception {
    KettleLogStore.init();
    ClusterInitializationException exception = new ClusterInitializationException( new Exception() );
    hBaseInputMeta.setNamedCluster( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class, null ) ).thenThrow( exception );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );

    IIOMetadataNode node = new IIOMetadataNode();
    IIOMetadataNode child = new IIOMetadataNode( "disable_wal" );
    IIOMetadataNode grandChild = new IIOMetadataNode();
    grandChild.setNodeValue( "N" );
    child.appendChild( grandChild );
    node.appendChild( child );

    hBaseInputMeta.loadXML( node, new ArrayList<>(), metaStore );

    ServiceStatus serviceStatus = hBaseInputMeta.getServiceStatus();
    assertNotNull( serviceStatus );
    assertFalse( serviceStatus.isOk() );
    assertEquals( exception, serviceStatus.getException() );
  }

  @Test
  public void testLoadXmlServiceStatusOk() throws Exception {
    KettleLogStore.init();
    hBaseInputMeta.setNamedCluster( namedCluster );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );

    IIOMetadataNode node = new IIOMetadataNode();
    IIOMetadataNode child = new IIOMetadataNode( "disable_wal" );
    IIOMetadataNode grandChild = new IIOMetadataNode();
    grandChild.setNodeValue( "N" );
    child.appendChild( grandChild );
    node.appendChild( child );

    hBaseInputMeta.loadXML( node, new ArrayList<>(), metaStore );

    ServiceStatus serviceStatus = hBaseInputMeta.getServiceStatus();
    assertNotNull( serviceStatus );
    assertTrue( serviceStatus.isOk() );
  }

  @Test
  public void testReadRepDoesntBubleUpException() throws Exception {
    KettleLogStore.init();
    ClusterInitializationException exception = new ClusterInitializationException( new Exception() );
    hBaseInputMeta.setNamedCluster( namedCluster );
    when( namedClusterServiceLocator.getService( namedCluster, HBaseService.class, null ) ).thenThrow( exception );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );

    hBaseInputMeta.readRep( new MemoryRepository(), metaStore, mock( ObjectId.class ), new ArrayList<>() );

    ServiceStatus serviceStatus = hBaseInputMeta.getServiceStatus();
    assertNotNull( serviceStatus );
    assertFalse( serviceStatus.isOk() );
    assertEquals( exception, serviceStatus.getException() );
  }

  @Test
  public void testReadRepServiceStatusOk() throws Exception {
    KettleLogStore.init();
    hBaseInputMeta.setNamedCluster( namedCluster );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    MappingFactory mappingFactory = mock( MappingFactory.class );

    hBaseInputMeta.readRep( new MemoryRepository(), metaStore, mock( ObjectId.class ), new ArrayList<>() );

    ServiceStatus serviceStatus = hBaseInputMeta.getServiceStatus();
    assertNotNull( serviceStatus );
    assertTrue( serviceStatus.isOk() );
  }

  @Test
  public void testLoadingAELMappingFromStepNode() throws Exception {
    KettleLogStore.init();
    hBaseInputMeta.setMapping( null );
    hBaseInputMeta.setNamedCluster( namedCluster );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );

    hBaseInputMeta.loadXML( getMappingNode(), new ArrayList<>(), metaStore );

    assertNotNull( hBaseInputMeta.m_mapping );
  }

  private Node getMappingNode() throws IOException, ParserConfigurationException, SAXException {
    String xml = IOUtils.toString( getClass().getClassLoader().getResourceAsStream( "StubMapping.xml" ) );

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();

    Document doc = builder.parse( new InputSource( new StringReader( xml ) ) );

    return doc.getDocumentElement();
  }
}
