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


package org.pentaho.di.core.namedcluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.stores.xml.XmlMetaStore;

public class NamedClusterTest {

  private IMetaStore metaStore = null;

  @Before
  public void before() throws IOException, MetaStoreException {
    File f = File.createTempFile( "NamedClusterTest", "before" );
    f.deleteOnExit();

    metaStore = new XmlMetaStore( f.getParent() );
  }

  @After
  public void after() throws IOException {
    File f = File.createTempFile( "NamedClusterTest", "after" );
    f.deleteOnExit();
    File metaStoreDir = new File( f.getParentFile().getAbsolutePath() + File.separator + "metastore" );
    FileUtils.deleteDirectory( metaStoreDir );
  }

  private NamedCluster createNamedCluster( String name ) {
    NamedCluster nc = new NamedCluster();
    nc.setName( name );
    return nc;
  }

  @Test
  public void testGetClusterTemplate() {
    NamedClusterManager manager = NamedClusterManager.getInstance();

    // add config, test that we can look it up
    String name = "" + System.currentTimeMillis();
    NamedCluster nc = createNamedCluster( name );
    manager.setClusterTemplate( nc );
    assertTrue( manager.getClusterTemplate() != null );
    assertTrue( name.equals( manager.getClusterTemplate().getName() ) );
  }

  @Test
  public void testEmptyStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();
    assertEquals( 0, manager.list( metaStore ).size() );
  }

  @Test
  public void testLargeStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();
    int count = 100;
    for ( int i = 0; i < count; i++ ) {
      manager.create( createNamedCluster( "config-" + i ), metaStore );
    }
    assertEquals( count, manager.list( metaStore ).size() );
  }

  @Test
  public void testCreateUsingMetaStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();
    manager.create( createNamedCluster( "config-1" ), metaStore );
    assertNotNull( manager.read( "config-1", metaStore ) );
  }

  @Test
  public void testUpdateUsingMetaStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();
    NamedCluster nc = createNamedCluster( "config-1" );
    manager.create( nc, metaStore );
    assertNotNull( manager.read( "config-1", metaStore ) );
    nc.setHdfsHost( "test-hdfs-host" );
    manager.update( nc, metaStore );
    nc = manager.read( nc.getName(), metaStore );
    assertNotNull( nc );
    assertEquals( "test-hdfs-host", nc.getHdfsHost() );
  }

  @Test
  public void testDeleteUsingMetaStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();

    String name = "" + System.currentTimeMillis();
    NamedCluster nc = createNamedCluster( name );
    manager.create( nc, metaStore );

    assertNotNull( manager.read( name, metaStore ) );
    manager.delete( name, metaStore );
    assertEquals( null, manager.read( name, metaStore ) );
  }

  @Test
  public void testListUsingMetaStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();

    String name = "" + System.currentTimeMillis();
    NamedCluster nc = createNamedCluster( name );
    manager.create( nc, metaStore );

    List<NamedCluster> list = manager.list( metaStore );
    assertTrue( list.contains( nc ) );
    assertNotNull( list );
  }

  @Test
  public void testListNamesUsingMetaStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();

    String name = "" + System.currentTimeMillis();
    NamedCluster nc = createNamedCluster( name );
    manager.create( nc, metaStore );

    List<String> list = manager.listNames( metaStore );
    assertTrue( list.contains( name ) );
    assertNotNull( list );
  }

  @Test
  public void testContainsUsingMetaStore() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();

    String name = "" + System.currentTimeMillis();
    NamedCluster nc = createNamedCluster( name );
    manager.create( nc, metaStore );

    assertNotNull( manager.contains( name, metaStore ) );
  }

  @Test
  public void testTemplateIsCloned() throws MetaStoreException {
    NamedClusterManager manager = NamedClusterManager.getInstance();

    String name = "" + System.currentTimeMillis();
    NamedCluster nc = createNamedCluster( name );
    manager.setClusterTemplate( nc );

    NamedCluster clone = manager.getClusterTemplate();
    assertNotNull( clone );
    clone.setName( "test-changed" );
    assertNotSame( clone, nc.getName() );
    assertEquals( name, nc.getName() );
  }

  @Test
  public void testToString() {
    NamedCluster other = new NamedCluster();
    assertEquals( "Named cluster: null", other.toString() );
    other.setName( "a" );
    assertEquals( "Named cluster: a", other.toString() );
  }
}
