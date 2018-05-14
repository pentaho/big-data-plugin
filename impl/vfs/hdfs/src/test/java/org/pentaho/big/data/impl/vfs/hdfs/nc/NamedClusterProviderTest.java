/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.impl.vfs.hdfs.nc;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

import java.net.URI;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.url.UrlFileName;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

public class NamedClusterProviderTest {

  private NamedClusterService ncService = mock( NamedClusterService.class );

  private MetastoreLocatorOsgi metastoreLocator = mock( MetastoreLocatorOsgi.class );

  private IMetaStore metastore = mock( IMetaStore.class );

  private NamedCluster nc = mock( NamedCluster.class );

  private NamedCluster ncTemplate = mock( NamedCluster.class );

  private HadoopFileSystemLocator hdfsLocator = mock( HadoopFileSystemLocator.class );

  private FileNameParser fileNameParser = mock( FileNameParser.class );

  private DefaultFileSystemManager fileSystemManager = mock( DefaultFileSystemManager.class );

  private HadoopFileSystem hfs = mock( HadoopFileSystem.class );

  private String[] scheme = new String[] { "test" };

  private String ncName = "ncName";
  private String path = "/samplePath";

  @Before
  public void setUp() throws MetaStoreException, ClusterInitializationException {
    when( ncService.read( eq( ncName ), eq( metastore ) ) ).thenReturn( nc );
    when( ncService.getClusterTemplate() ).thenReturn( ncTemplate );
    when( ncTemplate.processURLsubstitution( anyString(), any( IMetaStore.class ), any( Variables.class ) ) ).thenReturn( "nc://" + ncName + path );
    when( nc.processURLsubstitution( anyString(), any( IMetaStore.class ), any( Variables.class ) ) ).thenReturn( "nc://" + ncName + path );
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
  public void testGetConfigBuilder() throws FileSystemException, MetaStoreException {
    NamedClusterProvider provider = new  NamedClusterProvider( hdfsLocator, ncService, fileSystemManager, fileNameParser, scheme, metastoreLocator );
    FileSystemConfigBuilder builder = provider.getConfigBuilder();
    assertNotNull( builder );
    assertTrue( builder instanceof NamedClusterConfigBuilder );
  }

  @Test
  public void testDoCreateFileSystem() throws FileSystemException, MetaStoreException, ClusterInitializationException {
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
