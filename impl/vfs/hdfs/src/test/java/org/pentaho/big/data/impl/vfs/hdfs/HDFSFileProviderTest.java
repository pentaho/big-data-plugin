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

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;

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

  @Before
  public void setup() throws FileSystemException {
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    namedClusterService = mock( NamedClusterService.class );
    namedCluster = mock( NamedCluster.class );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    defaultFileSystemManager = mock( DefaultFileSystemManager.class );
    hdfsFileProvider = new HDFSFileProvider( hadoopFileSystemLocator, namedClusterService, defaultFileSystemManager );
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
    when( fileName.getRoot() ).thenReturn( genericFileName );
    when( genericFileName.getHostName() ).thenReturn( testHostname );
    when( genericFileName.getPort() ).thenReturn( -1 );
    assertTrue( hdfsFileProvider.doCreateFileSystem( fileName, null ) instanceof HDFSFileSystem );
    verify( hadoopFileSystemLocator ).getHadoopFilesystem( namedCluster );
    verify( namedCluster ).setHdfsHost( testHostname );
    verify( namedCluster ).setHdfsPort( "" );
  }

  @Test
  public void testDoCreateFileSystemPort() throws FileSystemException, ClusterInitializationException {
    String testHostname = "testHostname";
    FileName fileName = mock( FileName.class );
    GenericFileName genericFileName = mock( GenericFileName.class );
    when( fileName.getRoot() ).thenReturn( genericFileName );
    when( genericFileName.getHostName() ).thenReturn( testHostname );
    when( genericFileName.getPort() ).thenReturn( 111 );
    assertTrue( hdfsFileProvider.doCreateFileSystem( fileName, null ) instanceof HDFSFileSystem );
    verify( hadoopFileSystemLocator ).getHadoopFilesystem( namedCluster );
    verify( namedCluster ).setHdfsHost( testHostname );
    verify( namedCluster ).setHdfsPort( "111" );
  }

  @Test
  public void testGetCapabilities() {
    assertEquals( HDFSFileProvider.capabilities, hdfsFileProvider.getCapabilities() );
  }
}
