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

package org.pentaho.big.data.impl.cluster;

import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;

/**
 * Created by bryan on 7/14/15.
 */
public class NamedClusterManagerTest {
  private IMetaStore metaStore;
  private MetaStoreFactory<NamedClusterImpl> metaStoreFactory;
  private NamedClusterManager namedClusterManager;

  @Before
  @SuppressWarnings( "unchecked" )
  public void setup() {
    metaStore = mock( IMetaStore.class );
    metaStoreFactory = mock( MetaStoreFactory.class );
    namedClusterManager = new NamedClusterManager();
    namedClusterManager.putMetaStoreFactory( metaStore, metaStoreFactory );
  }

  @SuppressWarnings( { "unchecked", "rawtypes" } )
  @Test
  public void testInitProperties() throws IOException {
    String sampleKey = "sampleKey";
    String sampleValue = "sampleValue";
    Dictionary<String, Object> dictionary = new Hashtable<>();
    dictionary.put( sampleKey, sampleValue );
    Configuration configuration = mock( Configuration.class );
    when( configuration.getProperties() ).thenReturn( dictionary );
    ConfigurationAdmin confAdmin = mock( ConfigurationAdmin.class );
    when( confAdmin.getConfiguration( anyString() ) ).thenReturn( configuration );
    ServiceReference reference = mock( ServiceReference.class );
    BundleContext context = mock( BundleContext.class );
    when( context.getServiceReference( anyString() ) ).thenReturn( reference );
    when( context.getService( any( ServiceReference.class ) ) ).thenReturn( confAdmin );

    namedClusterManager.setBundleContext( context );
    namedClusterManager.initProperties();
    Map<String, Object>  prop = namedClusterManager.getProperties();
    assertEquals( dictionary.size(), prop.keySet().size() );
    Enumeration<String> keys = dictionary.keys();
    while ( keys.hasMoreElements() ) {
      String key = keys.nextElement();
      assertTrue( prop.keySet().contains( key ) );
      assertTrue( prop.values().contains( dictionary.get( key ) ) );
    }
  }

  @Test
  public void testInitProperties_emptyBundleService() {
    BundleContext context = mock( BundleContext.class );
    namedClusterManager.setBundleContext( context );
    namedClusterManager.initProperties();
    Map<String, Object>  prop = namedClusterManager.getProperties();
    assertEquals( 0, prop.keySet().size() );
  }

  @Test
  public void testInitProperties_exceptionDuringLoadService() {
    ServiceReference reference = mock( ServiceReference.class );
    BundleContext context = mock( BundleContext.class );
    when( context.getServiceReference( anyString() ) ).thenReturn( reference );
    namedClusterManager.setBundleContext( context );
    namedClusterManager.initProperties();
    Map<String, Object>  prop = namedClusterManager.getProperties();
    assertEquals( 0, prop.keySet().size() );
  }

  @Test
  public void testGetClusterTemplate() {
    NamedCluster clusterTemplate = namedClusterManager.getClusterTemplate();
    assertFalse( clusterTemplate == namedClusterManager.getClusterTemplate() );
    assertTrue( clusterTemplate.equals( namedClusterManager.getClusterTemplate() ) );
    NamedCluster template = mock( NamedCluster.class );
    NamedCluster clone = mock( NamedCluster.class );
    when( template.clone() ).thenReturn( clone );
    namedClusterManager.setClusterTemplate( template );
    assertEquals( clone, namedClusterManager.getClusterTemplate() );
  }

  @Test
  public void testCreate() throws MetaStoreException {
    NamedClusterImpl namedCluster = new NamedClusterImpl();
    String testName = "testName";
    namedCluster.setName( testName );
    namedClusterManager.create( namedCluster, metaStore );
    verify( metaStoreFactory ).saveElement( eq( namedCluster ) );
  }

  @Test
  public void testRead() throws MetaStoreException {
    String testName = "testName";
    NamedClusterImpl namedCluster = new NamedClusterImpl();
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertTrue( namedCluster == namedClusterManager.read( testName, metaStore ) );
  }

  @Test
  public void testUpdate() throws MetaStoreException {
    NamedClusterImpl namedCluster = new NamedClusterImpl();
    String testName = "testName";
    namedCluster.setName( testName );
    namedClusterManager.update( namedCluster, metaStore );
    verify( metaStoreFactory ).deleteElement( testName );
    verify( metaStoreFactory ).saveElement( eq( namedCluster ) );
  }

  @Test
  public void testDeleteElement() throws MetaStoreException {
    String testName = "testName";
    namedClusterManager.delete( testName, metaStore );
    verify( metaStoreFactory ).deleteElement( testName );
  }

  @Test
  public void testList() throws MetaStoreException {
    NamedClusterImpl namedCluster = new NamedClusterImpl();
    namedCluster.setName( "testName" );
    List<NamedClusterImpl> value = new ArrayList<>( Arrays.asList( namedCluster ) );
    when( metaStoreFactory.getElements() ).thenReturn( value );
    assertEquals( value, namedClusterManager.list( metaStore ) );
  }

  @Test
  public void testListNames() throws MetaStoreException {
    List<String> names = new ArrayList<>( Arrays.asList( "testName" ) );
    when( metaStoreFactory.getElementNames() ).thenReturn( names );
    assertEquals( names, namedClusterManager.listNames( metaStore ) );
  }

  @Test
  public void testListNames_emptymetaStoreFactory() throws MetaStoreException {
    IMetaStore metaStore = mock( IMetaStore.class );
    List<String> expectedNames = new ArrayList<>();
    verify( metaStoreFactory, never() ).getElementNames();
    assertEquals( expectedNames, namedClusterManager.listNames( metaStore ) );
  }

  @Test
  public void testContains() throws MetaStoreException {
    String testName = "testName";
    List<String> names = new ArrayList<>( Arrays.asList( testName ) );
    when( metaStoreFactory.getElementNames() ).thenReturn( names );
    assertFalse( namedClusterManager.contains( testName, null ) );
    assertTrue( namedClusterManager.contains( testName, metaStore ) );
    assertFalse( namedClusterManager.contains( "testName2", metaStore ) );
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testGetNamedClusterByName() throws MetaStoreException {
    String testName = "testName";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getName() ).thenReturn( testName );
    List namedClusters = new ArrayList<>( Arrays.asList( namedCluster ) );
    when( metaStoreFactory.getElements() ).thenReturn( namedClusters ).thenReturn( namedClusters ).thenThrow(
      new MetaStoreException() );
    assertNull( namedClusterManager.getNamedClusterByName( testName, null ) );
    assertEquals( namedCluster, namedClusterManager.getNamedClusterByName( testName, metaStore ) );
    assertNull( namedClusterManager.getNamedClusterByName( "fakeName", metaStore ) );
    assertNull( namedClusterManager.getNamedClusterByName( testName, metaStore ) );
  }
}
