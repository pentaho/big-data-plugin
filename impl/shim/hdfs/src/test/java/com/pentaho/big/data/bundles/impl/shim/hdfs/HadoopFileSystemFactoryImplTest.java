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

package com.pentaho.big.data.bundles.impl.shim.hdfs;

import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.api.fs.FileSystem;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/3/15.
 */
public class HadoopFileSystemFactoryImplTest {
  private HadoopConfiguration hadoopConfiguration;
  private boolean isActiveConfiguration;
  private HadoopFileSystemFactoryImpl hadoopFileSystemFactory;
  private NamedCluster namedCluster;
  private String identifier;
  private HadoopShim hadoopShim;
  private Configuration configuration;
  private FileSystem fileSystem;

  @Before
  public void setup() throws IOException {
    namedCluster = mock( NamedCluster.class );
    isActiveConfiguration = false;
    hadoopConfiguration = mock( HadoopConfiguration.class );
    hadoopShim = mock( HadoopShim.class );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( hadoopShim );
    configuration = mock( Configuration.class );
    when( hadoopShim.createConfiguration() ).thenReturn( configuration );
    fileSystem = mock( FileSystem.class );
    when( hadoopShim.getFileSystem( configuration ) ).thenReturn( fileSystem );
    identifier = "testId";
    when( hadoopConfiguration.getIdentifier() ).thenReturn( identifier );
    hadoopFileSystemFactory = new HadoopFileSystemFactoryImpl( isActiveConfiguration, hadoopConfiguration, "hdfs" );
  }

  @Test
  public void testCanHandleActiveConfig() {
    assertFalse( hadoopFileSystemFactory.canHandle( namedCluster ) );
    hadoopFileSystemFactory = new HadoopFileSystemFactoryImpl( true, hadoopConfiguration, "hdfs" );
    assertTrue( hadoopFileSystemFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCreateMapr() throws IOException {
    when( namedCluster.isMapr() ).thenReturn( true );
    HadoopFileSystem hadoopFileSystem = hadoopFileSystemFactory.create( namedCluster );
    verify( configuration ).set( HadoopFileSystem.FS_DEFAULT_NAME, "maprfs:///" );
    assertNotNull( hadoopFileSystem );
  }

  @Test
  public void testCreateNotMaprNoPort() throws IOException {
    String testHost = "testHost";
    when( namedCluster.isMapr() ).thenReturn( false );
    when( namedCluster.getHdfsHost() ).thenReturn( testHost );
    HadoopFileSystem hadoopFileSystem = hadoopFileSystemFactory.create( namedCluster );
    verify( configuration ).set( HadoopFileSystem.FS_DEFAULT_NAME, "hdfs://" + testHost );
    assertNotNull( hadoopFileSystem );
  }

  @Test
  public void testCreateNotMaprPort() throws IOException {
    String testHost = "testHost";
    String testPort = "testPort";
    when( namedCluster.isMapr() ).thenReturn( false );
    when( namedCluster.getHdfsHost() ).thenReturn( testHost );
    when( namedCluster.getHdfsPort() ).thenReturn( testPort );
    HadoopFileSystem hadoopFileSystem = hadoopFileSystemFactory.create( namedCluster );
    verify( configuration ).set( HadoopFileSystem.FS_DEFAULT_NAME, "hdfs://" + testHost + ":" + testPort );
    assertNotNull( hadoopFileSystem );
  }

  @Test( expected = IOException.class )
  public void testLocalFileSystem() throws IOException {
    when( namedCluster.isMapr() ).thenReturn( true );
    when( fileSystem.getDelegate() ).thenReturn( new LocalFileSystem() );
    hadoopFileSystemFactory.create( namedCluster );
  }
}
