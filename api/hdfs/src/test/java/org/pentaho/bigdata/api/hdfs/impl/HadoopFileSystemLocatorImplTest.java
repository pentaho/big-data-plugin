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

package org.pentaho.bigdata.api.hdfs.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/3/15.
 */
public class HadoopFileSystemLocatorImplTest {
  private List<HadoopFileSystemFactory> hadoopFileSystemFactories;
  private HadoopFileSystemLocatorImpl hadoopFileSystemLocator;
  private HadoopFileSystemFactory hadoopFileSystemFactory;
  private HadoopFileSystem hadoopFileSystem;
  private NamedCluster namedCluster;
  private ClusterInitializer clusterInitializer;

  @Before
  public void setup() {
    namedCluster = mock( NamedCluster.class );
    hadoopFileSystem = mock( HadoopFileSystem.class );
    hadoopFileSystemFactories = new ArrayList<>();
    hadoopFileSystemFactory = mock( HadoopFileSystemFactory.class );
    hadoopFileSystemFactories.add( hadoopFileSystemFactory );
    clusterInitializer = mock( ClusterInitializer.class );
    hadoopFileSystemLocator = new HadoopFileSystemLocatorImpl( hadoopFileSystemFactories, clusterInitializer );
  }

  @Test
  public void testIOException() throws IOException, ClusterInitializationException {
    when( hadoopFileSystemFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( hadoopFileSystemFactory.create( namedCluster ) ).thenThrow( new IOException() );
    assertNull( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
  }

  @Test
  public void testNormal() throws IOException, ClusterInitializationException {
    when( hadoopFileSystemFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( hadoopFileSystemFactory.create( namedCluster ) ).thenReturn( hadoopFileSystem );
    assertEquals( hadoopFileSystem, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
  }

  @Test
  public void testNoEligibleFactories() throws IOException, ClusterInitializationException {
    when( hadoopFileSystemFactory.canHandle( namedCluster ) ).thenReturn( false );
    assertNull( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
    verify( hadoopFileSystemFactory ).canHandle( namedCluster );
    verifyNoMoreInteractions( hadoopFileSystemFactory );
  }
}
