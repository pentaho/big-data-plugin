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

package org.pentaho.big.data.impl.vfs.hdfs.nc;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.url.UrlFileName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NamedClusterProviderTest {

  private final NamedClusterService ncService = mock( NamedClusterService.class );

  private final MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

  private final IMetaStore metastore = mock( IMetaStore.class );

  private final NamedCluster nc = mock( NamedCluster.class );

  private final NamedCluster ncTemplate = mock( NamedCluster.class );

  private final HadoopFileSystemLocator hdfsLocator = mock( HadoopFileSystemLocator.class );

  private final FileNameParser fileNameParser = mock( FileNameParser.class );

  private final DefaultFileSystemManager fileSystemManager = mock( DefaultFileSystemManager.class );

  private final HadoopFileSystem hfs = mock( HadoopFileSystem.class );

  private final String[] scheme = new String[] { "test" };

  private final String ncName = "ncName";
  private final String path = "/samplePath";

  @Before
  public void setUp() throws MetaStoreException, ClusterInitializationException {
    when( ncService.read( eq( ncName ), eq( metastore ) ) ).thenReturn( nc );
    when( ncService.getClusterTemplate() ).thenReturn( ncTemplate );
    when( ncTemplate.processURLsubstitution( anyString(), AdditionalMatchers.or( any( IMetaStore.class ), isNull() ), any( Variables.class ) ) ).thenReturn( "nc://" + ncName + path );
    when( nc.processURLsubstitution( anyString(), AdditionalMatchers.or( any( IMetaStore.class ), isNull() ), any( Variables.class ) ) ).thenReturn( "nc://" + ncName + path );
    when( hdfsLocator.getHadoopFilesystem( any( NamedCluster.class ), any( URI.class ) ) ).thenReturn( hfs );
  }

  @Test
  public void testGetNamedClusterByName_metastoreExist() throws FileSystemException, MetaStoreException {
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );
    String ncName = "ncName";
    NamedClusterProvider provider = new  NamedClusterProvider( hdfsLocator, ncService, fileSystemManager, fileNameParser, scheme, metastoreLocator );
    assertEquals( nc,  provider.getNamedClusterByName( ncName, null ) );
    verify( ncService, times( 2 ) ).read( eq( ncName ), eq( metastore ) );
  }

  @Test
  public void testGetNamedClusterByName_metastoreNotExist() throws FileSystemException, MetaStoreException {
    String ncName = "ncName";
    NamedClusterProvider provider = new  NamedClusterProvider( hdfsLocator, ncService, fileSystemManager, fileNameParser, scheme, metastoreLocator );
    //should be null because we do not have metastore
    assertNull( provider.getNamedClusterByName( ncName, null ) );
    verify( ncService, never() ).read( eq( ncName ), eq( metastore ) );
  }

  @Test
  public void testGetConfigBuilder() throws FileSystemException {
    NamedClusterProvider provider = new  NamedClusterProvider( hdfsLocator, ncService, fileSystemManager, fileNameParser, scheme, metastoreLocator );
    FileSystemConfigBuilder builder = provider.getConfigBuilder();
    assertNotNull( builder );
    assertTrue( builder instanceof NamedClusterConfigBuilder );
  }

  @Test
  public void testDoCreateFileSystem() throws FileSystemException, ClusterInitializationException {
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );

    UrlFileName name = new UrlFileName( "hc", ncName, 0, 0, null, null, path, null, null );
    NamedClusterProvider provider = new  NamedClusterProvider( hdfsLocator, ncService, fileSystemManager, fileNameParser, scheme, metastoreLocator );
    FileSystem fs = provider.doCreateFileSystem( name, null );
    assertTrue( fs instanceof HDFSFileSystem );

    HDFSFileSystem hdfsFS = (HDFSFileSystem) fs;
    assertEquals( hfs, hdfsFS.getHDFSFileSystem() );

    verify( nc ).processURLsubstitution( anyString(), eq( metastore ), any( Variables.class ) );
    verify( hdfsLocator ).getHadoopFilesystem( eq( nc ), any( URI.class ) );
  }

  @Test
  public void testDoCreateFileSystem_NCTemplate() throws FileSystemException, MetaStoreException, ClusterInitializationException {
    UrlFileName name = new UrlFileName( "hc", ncName, 0, 0, null, null, path, null, null );
    NamedClusterProvider provider = new  NamedClusterProvider( hdfsLocator, ncService, fileSystemManager, fileNameParser, scheme, metastoreLocator );
    FileSystem fs = provider.doCreateFileSystem( name, null );
    assertTrue( fs instanceof HDFSFileSystem );

    HDFSFileSystem hdfsFS = (HDFSFileSystem) fs;
    assertEquals( hfs, hdfsFS.getHDFSFileSystem() );

    verify( ncService, never() ).read( eq( ncName ), eq( metastore ) );
    verify( hdfsLocator ).getHadoopFilesystem( eq( ncTemplate ), any( URI.class ) );
  }

}
