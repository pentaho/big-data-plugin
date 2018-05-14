/*******************************************************************************
 *
 *  Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemPath;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by bryan on 8/14/15.
 */
public class WriteToAndDeleteFromUsersHomeFolderTest extends BaseRuntimeTest {
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
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_DELETING_FILE_DESC =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorWritingDeletingFile.Desc";
  public static final String WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_DELETING_FILE_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.ErrorWritingDeletingFile.Message";
  private static final Class<?> PKG = WriteToAndDeleteFromUsersHomeFolderTest.class;
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final MessageGetterFactory messageGetterFactory;
  private final MessageGetter messageGetter;

  public WriteToAndDeleteFromUsersHomeFolderTest( MessageGetterFactory messageGetterFactory,
                                                  HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( NamedCluster.class, Constants.HADOOP_FILE_SYSTEM,
      HADOOP_FILE_SYSTEM_WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST,
      messageGetterFactory.create( PKG ).getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_NAME ),
      new HashSet<>( Arrays.asList( ListHomeDirectoryTest.HADOOP_FILE_SYSTEM_LIST_HOME_DIRECTORY_TEST ) ) );
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.messageGetterFactory = messageGetterFactory;
    this.messageGetter = messageGetterFactory.create( PKG );
  }

  @Override
  public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;
    try {
      HadoopFileSystem hadoopFilesystem = hadoopFileSystemLocator.getHadoopFilesystem( namedCluster );
      if ( hadoopFilesystem == null ) {
        return new RuntimeTestResultSummaryImpl(
          new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
            messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_DESC ),
            messageGetter.getMessage(
              WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE,
              namedCluster.getName() ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
      } else {
        HadoopFileSystemPath path = hadoopFilesystem.getPath( PENTAHO_SHIM_TEST_FILE_TEST );
        HadoopFileSystemPath qualifiedPath = hadoopFilesystem.makeQualified( path );
        Boolean exists;
        try {
          exists = hadoopFilesystem.exists( path );
        } catch ( IOException e ) {
          return new RuntimeTestResultSummaryImpl(
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL, messageGetter
              .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_DESC ),
              messageGetter
                .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_MESSAGE,
                  qualifiedPath.getName(), qualifiedPath.getPath() ),
              e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
        }
        if ( exists ) {
          return new RuntimeTestResultSummaryImpl(
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
              messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_DESC ),
              messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_MESSAGE,
                qualifiedPath.getName(),
                qualifiedPath.getPath() ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
        } else {
          OutputStream outputStream;
          try {
            outputStream = hadoopFilesystem.create( path );
          } catch ( IOException e ) {
            return new RuntimeTestResultSummaryImpl(
              new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
                messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_DESC ),
                messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_MESSAGE,
                  qualifiedPath.getName(), qualifiedPath.getPath() ),
                e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
          }
          RuntimeTestResultEntry writeExceptionEntry = null;
          try {
            outputStream.write( HELLO_CLUSTER.getBytes( UTF8 ) );
          } catch ( IOException e ) {
            writeExceptionEntry = new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
              messageGetter
                .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_DESC ),
              messageGetter.getMessage(
                WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_MESSAGE,
                qualifiedPath.getName(), qualifiedPath.getPath() ),
              e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY );
          } finally {
            try {
              outputStream.close();
            } catch ( IOException e ) {
              //Ignore
            }
          }

          try {
            if ( hadoopFilesystem.delete( path, false ) ) {
              if ( writeExceptionEntry == null ) {
                return new RuntimeTestResultSummaryImpl(
                  new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.INFO,
                    messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_DESC ),
                    messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_MESSAGE,
                      qualifiedPath.toString() ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
              } else {
                return new RuntimeTestResultSummaryImpl( writeExceptionEntry );
              }
            } else {
              if ( writeExceptionEntry == null ) {
                return new RuntimeTestResultSummaryImpl(
                  new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
                    messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_DESC ),
                    messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_MESSAGE,
                      qualifiedPath.getName(),
                      qualifiedPath.getPath() ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
              } else {
                return new RuntimeTestResultSummaryImpl( writeExceptionEntry );
              }
            }
          } catch ( IOException e ) {
            RuntimeTestResultEntryImpl deleteExceptionEntry =
              new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
                messageGetter
                  .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_DESC ),
                messageGetter.getMessage(
                  WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_MESSAGE,
                  qualifiedPath.getName(), qualifiedPath.getPath() ),
                e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY );
            if ( writeExceptionEntry == null ) {
              return new RuntimeTestResultSummaryImpl( deleteExceptionEntry );
            } else {
              return new RuntimeTestResultSummaryImpl(
                new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
                  messageGetter.getMessage(
                    WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_DELETING_FILE_DESC ), messageGetter
                  .getMessage(
                    WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_DELETING_FILE_MESSAGE ),
                  ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ),
                Arrays.asList( writeExceptionEntry, deleteExceptionEntry ) );
            }
          }
        }
      }
    } catch ( ClusterInitializationException e ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_DESC ),
          messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE,
            namedCluster.getName() ),
          e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
    }
  }
}
