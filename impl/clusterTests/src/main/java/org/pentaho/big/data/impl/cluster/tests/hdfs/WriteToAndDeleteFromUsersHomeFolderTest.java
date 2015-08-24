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

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.api.clusterTest.test.impl.ClusterTestResultEntryImpl;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class WriteToAndDeleteFromUsersHomeFolderTest extends BaseClusterTest {
  public static final String HADOOP_FILE_SYSTEM_WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST =
    "hadoopFileSystemWriteToAndDeleteFromUsersHomeFolderTest";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_NAME =
    "WriteToAndDeleteFromUsersHomeFolderTest.Name";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.CouldntGetFileSystem.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.CouldntGetFileSystem.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.FileExists.Desc";
  public static final String PENTAHO_SHIM_TEST_FILE_TEST = "pentaho-shim-test-file.test";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.FileExists.Message";
  public static final String HELLO_CLUSTER = "Hello, Cluster";
  public static final Charset UTF8 = Charset.forName( "UTF-8" );
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.Success.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.Success.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.UnableToDelete.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.UnableToDelete.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorInitializingCluster.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorInitializingCluster.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorCheckingIfFileExists.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorCheckingIfFileExists.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorCreatingFile.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorCreatingFile.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorWritingToFile.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorWritingToFile.Message";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorDeletingFile.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorDeletingFile.Message";
  private static final Class<?> PKG = WriteToAndDeleteFromUsersHomeFolderTest.class;
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final MessageGetter messageGetter;

  public WriteToAndDeleteFromUsersHomeFolderTest( MessageGetterFactory messageGetterFactory,
                                                  HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( Constants.HADOOP_FILE_SYSTEM, HADOOP_FILE_SYSTEM_WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST,
      messageGetterFactory.create( PKG ).getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_NAME ),
      new HashSet<>(
        Arrays.asList( ListHomeDirectoryTest.HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST ) ) );
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.messageGetter = messageGetterFactory.create( PKG );
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    try {
      HadoopFileSystem hadoopFilesystem = hadoopFileSystemLocator.getHadoopFilesystem( namedCluster );
      if ( hadoopFilesystem == null ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_DESC ),
          messageGetter
            .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE,
              namedCluster.getName() ) ) );
      } else {
        HadoopFileSystemPath path = hadoopFilesystem.getPath( PENTAHO_SHIM_TEST_FILE_TEST );
        HadoopFileSystemPath qualifiedPath = hadoopFilesystem.makeQualified( path );
        Boolean exists = null;
        try {
          exists = hadoopFilesystem.exists( path );
        } catch ( IOException e ) {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
            messageGetter.getMessage(
              WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_DESC ),
            messageGetter.getMessage(
              WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_MESSAGE,
              qualifiedPath.toString() ),
            e ) );
        }
        if ( exists != null ) {
          if ( exists ) {
            clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
              messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_DESC ),
              messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_MESSAGE,
                qualifiedPath.toString() ) ) );
          } else {
            OutputStream outputStream = null;
            try {
              outputStream = hadoopFilesystem.create( path );
            } catch ( IOException e ) {
              clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
                messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_DESC ),
                messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_MESSAGE,
                  qualifiedPath.toString() ), e ) );
            }
            if ( outputStream != null ) {
              IOException writeException = null;
              try {
                outputStream.write( HELLO_CLUSTER.getBytes( UTF8 ) );
              } catch ( IOException e ) {
                writeException = e;
                clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
                  messageGetter
                    .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_DESC ),
                  messageGetter.getMessage(
                    WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_MESSAGE,
                    qualifiedPath.toString() ), e ) );
              } finally {
                try {
                  outputStream.close();
                } catch ( IOException e ) {
                  //Ignore
                }
              }
              try {
                if ( hadoopFilesystem.delete( path, false ) ) {
                  if ( writeException == null ) {
                    clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
                      messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_DESC ),
                      messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_MESSAGE,
                        qualifiedPath.toString() ) ) );
                  }
                } else {
                  clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
                    messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_DESC ),
                    messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_MESSAGE,
                      qualifiedPath.toString() ) ) );
                }
              } catch ( IOException e ) {
                clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
                  messageGetter
                    .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_DESC ),
                  messageGetter.getMessage(
                    WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_MESSAGE,
                    qualifiedPath.toString() ), e ) );
              }
            }
          }
        }
      }
    } catch ( ClusterInitializationException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
        messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_DESC ),
        messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE,
          namedCluster.getName() ),
        e ) );
    }
    return clusterTestResultEntries;
  }
}
