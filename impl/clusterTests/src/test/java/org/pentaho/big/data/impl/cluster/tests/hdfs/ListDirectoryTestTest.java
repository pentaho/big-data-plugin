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

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;

import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.hdfs.exceptions.AccessControlException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileStatus;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemPath;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by bryan on 8/21/15.
 */
public class ListDirectoryTestTest {
  private TestMessageGetterFactory testMessageGetterFactory;
  private MessageGetter messageGetter;
  private HadoopFileSystemLocator hadoopFileSystemLocator;
  private String directory;
  private String id;
  private String name;
  private ListDirectoryTest listDirectoryTest;
  private NamedCluster namedCluster;
  private HadoopFileSystem hadoopFileSystem;
  private String namedClusterName;
  private HadoopFileSystemPath directoryPath;
  private HadoopFileSystemPath homeDirectoryPath;

  @Before
  public void setup() throws ClusterInitializationException {
    testMessageGetterFactory = new TestMessageGetterFactory();
    messageGetter = testMessageGetterFactory.create( ListDirectoryTest.class );
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    directory = "directory";
    id = "id";
    name = "name";
    namedCluster = mock( NamedCluster.class );
    namedClusterName = "namedCluster";
    when( namedCluster.getName() ).thenReturn( namedClusterName );
    hadoopFileSystem = mock( HadoopFileSystem.class );
    directoryPath = mock( HadoopFileSystemPath.class );
    when( hadoopFileSystem.getPath( directory ) ).thenReturn( directoryPath );
    homeDirectoryPath = mock( HadoopFileSystemPath.class );
    when( hadoopFileSystem.getHomeDirectory() ).thenReturn( homeDirectoryPath );
    when( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) ).thenReturn( hadoopFileSystem );
    init();
  }

  private void init() {
    listDirectoryTest = new ListDirectoryTest( testMessageGetterFactory, hadoopFileSystemLocator, directory, id, name );
  }

  @Test
  public void testNullHadoopFileSystem() {
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    init();
    RuntimeTestResultSummary runtimeTestResultSummary = listDirectoryTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_DESC ), messageGetter
        .getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE, directory ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testClusterInitializationException() throws ClusterInitializationException {
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    when( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) )
      .thenThrow( new ClusterInitializationException( null ) );
    init();
    RuntimeTestResultSummary runtimeTestResultSummary = listDirectoryTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_DESC ), messageGetter
        .getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE, namedClusterName ),
      ClusterInitializationException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testAccessControlException() throws IOException {
    when( hadoopFileSystem.listStatus( directoryPath ) ).thenThrow( new AccessControlException( null, null ) );
    RuntimeTestResultSummary runtimeTestResultSummary = listDirectoryTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.WARNING,
      messageGetter.getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_DESC ), messageGetter
        .getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_MESSAGE, directoryPath.toString() ),
      AccessControlException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIOException() throws IOException {
    when( hadoopFileSystem.listStatus( directoryPath ) ).thenThrow( new IOException() );
    RuntimeTestResultSummary runtimeTestResultSummary = listDirectoryTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_DESC ), messageGetter
        .getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_MESSAGE, directoryPath.toString() ),
      IOException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testHomeDirectorySuccess() throws IOException {
    directory = "";
    HadoopFileStatus hadoopFileStatus1 = mock( HadoopFileStatus.class );
    HadoopFileStatus hadoopFileStatus2 = mock( HadoopFileStatus.class );
    HadoopFileSystemPath hadoopFileSystemPath1 = mock( HadoopFileSystemPath.class );
    HadoopFileSystemPath hadoopFileSystemPath2 = mock( HadoopFileSystemPath.class );
    when( hadoopFileStatus1.getPath() ).thenReturn( hadoopFileSystemPath1 );
    when( hadoopFileStatus2.getPath() ).thenReturn( hadoopFileSystemPath2 );
    HadoopFileStatus[] hadoopFileStatuses = { hadoopFileStatus1, hadoopFileStatus2 };
    when( hadoopFileSystem.listStatus( homeDirectoryPath ) ).thenReturn( hadoopFileStatuses );
    init();
    RuntimeTestResultSummary runtimeTestResultSummary = listDirectoryTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.INFO,
      messageGetter.getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_SUCCESS_DESC ), messageGetter
        .getMessage( ListDirectoryTest.LIST_DIRECTORY_TEST_SUCCESS_MESSAGE,
          hadoopFileSystemPath1.toString() + ", " + hadoopFileSystemPath2.toString() ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }
}
