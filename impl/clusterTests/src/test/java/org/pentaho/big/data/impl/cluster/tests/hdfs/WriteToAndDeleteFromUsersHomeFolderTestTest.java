/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemPath;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by bryan on 8/21/15.
 */
public class WriteToAndDeleteFromUsersHomeFolderTestTest {
  private TestMessageGetterFactory messageGetterFactory;
  private MessageGetter messageGetter;
  private HadoopFileSystemLocator hadoopFileSystemLocator;
  private WriteToAndDeleteFromUsersHomeFolderTest writeToAndDeleteFromUsersHomeFolderTest;
  private NamedCluster namedCluster;
  private String namedClusterName;
  private HadoopFileSystem hadoopFileSystem;
  private HadoopFileSystemPath hadoopFileSystemPath;
  private HadoopFileSystemPath qualifiedPath;

  @Before
  public void setup() throws ClusterInitializationException {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( WriteToAndDeleteFromUsersHomeFolderTest.class );
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    namedCluster = mock( NamedCluster.class );
    namedClusterName = "namedClusterName";
    when( namedCluster.getName() ).thenReturn( namedClusterName );
    hadoopFileSystem = mock( HadoopFileSystem.class );
    when( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) ).thenReturn( hadoopFileSystem );
    hadoopFileSystemPath = mock( HadoopFileSystemPath.class );
    when( hadoopFileSystem.getPath( WriteToAndDeleteFromUsersHomeFolderTest.PENTAHO_SHIM_TEST_FILE_TEST ) )
      .thenReturn( hadoopFileSystemPath );
    qualifiedPath = mock( HadoopFileSystemPath.class );
    when( hadoopFileSystem.makeQualified( hadoopFileSystemPath ) ).thenReturn( qualifiedPath );
    when( qualifiedPath.getName() ).thenReturn( WriteToAndDeleteFromUsersHomeFolderTest.PENTAHO_SHIM_TEST_FILE_TEST );
    when( qualifiedPath.getPath() ).thenReturn( "" );
    init();
  }

  private void init() {
    writeToAndDeleteFromUsersHomeFolderTest =
      new WriteToAndDeleteFromUsersHomeFolderTest( messageGetterFactory, hadoopFileSystemLocator );
  }

  @Test
  public void testNullFileSystem() {
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    init();
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE,
        namedClusterName ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testClusterInitializationException() throws ClusterInitializationException {
    hadoopFileSystemLocator = mock( HadoopFileSystemLocator.class );
    when( hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) )
      .thenThrow( new ClusterInitializationException( null ) );
    init();
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE,
        namedClusterName ), ClusterInitializationException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIOExceptionExists() throws ClusterInitializationException, IOException {
    when( hadoopFileSystem.exists( hadoopFileSystemPath ) ).thenThrow( new IOException() );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_MESSAGE,
        qualifiedPath.getName(), qualifiedPath.getPath() ), IOException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIOExceptionCreate() throws ClusterInitializationException, IOException {
    when( hadoopFileSystem.create( hadoopFileSystemPath ) ).thenThrow( new IOException() );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.WARNING, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_MESSAGE,
        qualifiedPath.getName(), qualifiedPath.getPath() ), IOException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIOExceptionWrite() throws ClusterInitializationException, IOException {
    OutputStream outputStream = mock( OutputStream.class );
    when( hadoopFileSystem.create( hadoopFileSystemPath ) ).thenReturn( outputStream );
    when( hadoopFileSystem.delete( hadoopFileSystemPath, false ) ).thenReturn( true );
    doThrow( new IOException() ).when( outputStream ).write( isA( byte[].class ) );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.WARNING, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_MESSAGE,
        qualifiedPath.getName(), qualifiedPath.getPath() ), IOException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIOExceptionDelete() throws ClusterInitializationException, IOException {
    OutputStream outputStream = mock( OutputStream.class );
    when( hadoopFileSystem.create( hadoopFileSystemPath ) ).thenReturn( outputStream );
    when( hadoopFileSystem.delete( hadoopFileSystemPath, false ) ).thenThrow( new IOException() );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.WARNING, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_MESSAGE,
        qualifiedPath.getName(), qualifiedPath.getPath() ), IOException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testPathExists() throws IOException {
    when( hadoopFileSystem.exists( hadoopFileSystemPath ) ).thenReturn( true );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.WARNING, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_MESSAGE,
        qualifiedPath.getName(), qualifiedPath.getPath() ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testUnableToDelete() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    when( hadoopFileSystem.create( hadoopFileSystemPath ) ).thenReturn( byteArrayOutputStream );
    when( hadoopFileSystem.delete( hadoopFileSystemPath, false ) ).thenReturn( false );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.WARNING, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_MESSAGE,
        qualifiedPath.getName(), qualifiedPath.getPath() ) );
    assertEquals( WriteToAndDeleteFromUsersHomeFolderTest.HELLO_CLUSTER,
      byteArrayOutputStream.toString( WriteToAndDeleteFromUsersHomeFolderTest.UTF8.name() ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testSuccess() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    when( hadoopFileSystem.create( hadoopFileSystemPath ) ).thenReturn( byteArrayOutputStream );
    when( hadoopFileSystem.delete( hadoopFileSystemPath, false ) ).thenReturn( true );
    RuntimeTestResultSummary runtimeTestResultSummary = writeToAndDeleteFromUsersHomeFolderTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.INFO, messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_DESC ),
      messageGetter.getMessage(
        WriteToAndDeleteFromUsersHomeFolderTest
          .WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_MESSAGE,
        qualifiedPath.toString() ) );
    assertEquals( WriteToAndDeleteFromUsersHomeFolderTest.HELLO_CLUSTER,
      byteArrayOutputStream.toString( WriteToAndDeleteFromUsersHomeFolderTest.UTF8.name() ) );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }
}
