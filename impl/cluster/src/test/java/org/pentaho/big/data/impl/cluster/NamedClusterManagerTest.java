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

package org.pentaho.big.data.impl.cluster;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  @SuppressWarnings( "unchecked" )
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
