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

package org.pentaho.big.data.kettle.plugins.hdfs.job;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
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
    assertEquals( testUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( testNewUrl ) );
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
    assertEquals( testUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( testNewUrl ) );
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
    assertEquals( testUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( testNewUrl ) );
  }

  @Test
  public void testLoadUrlHdfsEMPTY_SOURCE_URL() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( false );
    String testNewUrl = HadoopSpoonPlugin.HDFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
      .thenReturn( testNewUrl );
    String prefixUrlSource = JobEntryCopyFiles.SOURCE_URL + 8 + "-";
    String testPrefixSourceUrl = prefixUrlSource + testUrl;
    String expectedPrefixSourceLoadUrl = prefixUrlSource + testNewUrl;
    assertEquals( expectedPrefixSourceLoadUrl, jobEntryHadoopCopyFiles.loadURL( testPrefixSourceUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( expectedPrefixSourceLoadUrl, testNcName );
    assertEquals( testPrefixSourceUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( expectedPrefixSourceLoadUrl ) );
  }

  @Test
  public void testLoadUrlHdfsEMPTY_DEST_URL() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( false );
    String testNewUrl = HadoopSpoonPlugin.HDFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
      .thenReturn( testNewUrl );
    String prefixUrlDest = JobEntryCopyFiles.DEST_URL + 5 + "-";
    String testPrefixDestUrl = prefixUrlDest + testUrl;
    String expectedPrefixDestLoadUrl = prefixUrlDest + testNewUrl;
    assertEquals( expectedPrefixDestLoadUrl, jobEntryHadoopCopyFiles.loadURL( testPrefixDestUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( expectedPrefixDestLoadUrl, testNcName );
    assertEquals( testPrefixDestUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( expectedPrefixDestLoadUrl ) );
  }

  @Test
  public void testSaveUrlMappingsKeyMisses() {
    String testUrl = "/src/path/";
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.clear();
    // populating with other values
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyA", "ValueA" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyB", "ValueB" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src", "ValueC" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src/path/anotherPath", "ValueD" );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.saveURL( testUrl, testNcName, metaStore, mappings ) );

    assertNull( testUrl, jobEntryHadoopCopyFiles.saveURL( null, testNcName, metaStore, mappings ) );
  }

  @Test
  public void testSaveUrlMappingsKeyHits() {
    String testUrl = "/src/path/";
    String testUrlSubstituted = "hdfs://someHostname/src/path";
    // populating with other values
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyA", "ValueA" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyB", "ValueB" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src", "ValueC" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src/path/anotherPath", "ValueD" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( testUrlSubstituted, testUrl );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.saveURL( testUrl, testNcName, metaStore, mappings ) );
  }
}
