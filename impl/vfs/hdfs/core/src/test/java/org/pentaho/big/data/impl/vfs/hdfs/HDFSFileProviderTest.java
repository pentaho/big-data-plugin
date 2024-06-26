/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/7/15.
 */
public class HDFSFileProviderTest {
  private HadoopFileSystemLocator hadoopFileSystemLocator;
  private NamedClusterService namedClusterService;
  private DefaultFileSystemManager defaultFileSystemManager;
  private HDFSFileProvider hdfsFileProvider;
  private NamedCluster namedCluster;
  private MetastoreLocator metaStoreLocator;
  private FileNameParser fileNameParser;

  @Before
  public void setup() throws FileSystemException {
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    namedClusterService = mock( NamedClusterService.class );
    namedCluster = mock( NamedCluster.class );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    defaultFileSystemManager = mock( DefaultFileSystemManager.class );
    metaStoreLocator = mock( MetastoreLocator.class );
    fileNameParser = mock( FileNameParser.class );
    hdfsFileProvider = new HDFSFileProvider(
      hadoopFileSystemLocator, namedClusterService, defaultFileSystemManager,
      fileNameParser,  new String[] { HDFSFileProvider.SCHEME, HDFSFileProvider.MAPRFS }, metaStoreLocator );
    ArgumentCaptor<String[]> argumentCaptor = ArgumentCaptor.forClass( String[].class );
    verify( defaultFileSystemManager )
      .addProvider( argumentCaptor.capture(), eq( hdfsFileProvider ) );
    String[] schemes = argumentCaptor.getValue();
    assertEquals( 2, schemes.length );
    assertEquals( HDFSFileProvider.SCHEME, schemes[ 0 ] );
    assertEquals( HDFSFileProvider.MAPRFS, schemes[ 1 ] );
  }

  @Test
  public void testDoCreateFileSystemNoPort() throws FileSystemException, ClusterInitializationException {
    String testHostname = "testHostname";
    FileName fileName = mock( FileName.class );
    GenericFileName genericFileName = mock( GenericFileName.class );
    when( fileName.getURI() ).thenReturn( "" );
    when( fileName.getRoot() ).thenReturn( genericFileName );
    when( fileName.getScheme() ).thenReturn( HDFSFileProvider.MAPRFS );
    when( genericFileName.getHostName() ).thenReturn( testHostname );
    when( genericFileName.getPort() ).thenReturn( -1 );
    assertTrue( hdfsFileProvider.doCreateFileSystem( fileName, null ) instanceof HDFSFileSystem );
    verify( hadoopFileSystemLocator ).getHadoopFilesystem( namedCluster, URI.create( "" ) );
    verify( namedClusterService ).updateNamedClusterTemplate( testHostname, -1, true );
  }

  @Test
  public void testGetCapabilities() {
    assertEquals( HDFSFileProvider.capabilities, hdfsFileProvider.getCapabilities() );
  }
}

