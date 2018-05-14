/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.job;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 11/23/15.
 */
public class JobEntryHadoopCopyFilesTest {
  private JobEntryHadoopCopyFiles jobEntryHadoopCopyFiles;
  private String testName;
  private NamedClusterService namedClusterManager;
  private String testUrl;
  private String testNcName;
  private IMetaStore metaStore;
  private Map mappings;
  private NamedCluster namedCluster;

  @Before
  public void setup() {
    testName = "testName";
    namedClusterManager = mock( NamedClusterService.class );
    jobEntryHadoopCopyFiles =
        new JobEntryHadoopCopyFiles( namedClusterManager, mock( RuntimeTestActionService.class ), mock(
            RuntimeTester.class ) );
    jobEntryHadoopCopyFiles.setName( testName );
    testUrl = "testUrl";
    testNcName = "testNcName";
    metaStore = mock( IMetaStore.class );
    mappings = mock( Map.class );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testLoadUrlNullNcName() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( null );
    String loadURL = jobEntryHadoopCopyFiles.loadURL( testUrl, null, metaStore, mappings );
    assertNotNull( loadURL );
    verifyNoMoreInteractions( mappings );
  }

  @Test
  public void testLoadUrlNull() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( null );
    String loadURL = jobEntryHadoopCopyFiles.loadURL( null, null, metaStore, mappings );
    assertNull( loadURL );
    verifyNoMoreInteractions( mappings );
  }

  @Test
  public void testLoadUrlNotNullForNotCluster() {
    testNcName = "LOCAL-SOURCE-FILE-1";
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( null );
    String loadURL = jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings );
    assertNotNull( loadURL );
    assertEquals( testUrl, loadURL );
    verify( mappings ).put( testUrl, testNcName );
  }

  @Test
  public void testLoadUrlMapRNull() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( true );
    assertNull( jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verifyNoMoreInteractions( mappings );
  }

  @Test
  public void testLoadUrlMapRNotNullNoPrefix() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( true );
    String testNewUrl = "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
        .thenReturn( testNewUrl );
    assertEquals( testNewUrl, jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( testNewUrl, testNcName );
  }

  @Test
  public void testLoadUrlMapRNotNullPrefix() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( true );
    String testNewUrl = HadoopSpoonPlugin.MAPRFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
        .thenReturn( testNewUrl );
    assertEquals( testNewUrl, jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( testNewUrl, testNcName );
  }

  @Test
  public void testLoadUrlNotMapR() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( false );
    String testNewUrl = HadoopSpoonPlugin.HDFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
        .thenReturn( testNewUrl );
    assertEquals( testNewUrl, jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( testNewUrl, testNcName );
  }
}
