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
import org.pentaho.bigdata.api.hdfs.HadoopFileStatus;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemPath;
import org.pentaho.bigdata.api.hdfs.exceptions.AccessControlException;
import org.pentaho.di.core.Const;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class ListDirectoryTest extends BaseClusterTest {
  public static final String LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_DESC =
    "ListDirectoryTest.CouldntGetFileSystem.Desc";
  public static final String LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE =
    "ListDirectoryTest.CouldntGetFileSystem.Message";
  public static final String LIST_DIRECTORY_TEST_SUCCESS_DESC = "ListDirectoryTest.Success.Desc";
  public static final String LIST_DIRECTORY_TEST_SUCCESS_MESSAGE = "ListDirectoryTest.Success.Message";
  public static final String LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_DESC =
    "ListDirectoryTest.AccessControlException.Desc";
  public static final String LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_MESSAGE =
    "ListDirectoryTest.AccessControlException.Message";
  public static final String LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_DESC =
    "ListDirectoryTest.ErrorListingDirectory.Desc";
  public static final String LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_MESSAGE =
    "ListDirectoryTest.ErrorListingDirectory.Message";
  public static final String LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_DESC =
    "ListDirectoryTest.ErrorInitializingCluster.Desc";
  public static final String LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE =
    "ListDirectoryTest.ErrorInitializingCluster.Message";
  private static final Class<?> PKG = ListDirectoryTest.class;
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final String directory;
  private final MessageGetter messageGetter;

  public ListDirectoryTest( MessageGetterFactory messageGetterFactory, HadoopFileSystemLocator hadoopFileSystemLocator,
                            String directory, String id, String name ) {
    super( Constants.HADOOP_FILE_SYSTEM, id, name, new HashSet<>(
      Arrays.asList( PingFileSystemEntryPointTest.HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST ) ) );
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.directory = directory;
    this.messageGetter = messageGetterFactory.create( PKG );
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    List<ClusterTestResultEntry> clusterTestResultEntries = new ArrayList<>();
    try {
      HadoopFileSystem hadoopFilesystem = hadoopFileSystemLocator.getHadoopFilesystem( namedCluster );
      if ( hadoopFilesystem == null ) {
        clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
          messageGetter.getMessage( LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_DESC ),
          messageGetter.getMessage( LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE, namedCluster.getName() ) ) );
      } else {
        HadoopFileSystemPath hadoopFilesystemPath;
        if ( Const.isEmpty( directory ) ) {
          hadoopFilesystemPath = hadoopFilesystem.getHomeDirectory();
        } else {
          hadoopFilesystemPath = hadoopFilesystem.getPath( directory );
        }
        try {
          HadoopFileStatus[] hadoopFileStatuses = hadoopFilesystem.listStatus( hadoopFilesystemPath );
          StringBuilder paths = new StringBuilder();
          for ( HadoopFileStatus hadoopFileStatus : hadoopFileStatuses ) {
            paths.append( hadoopFileStatus.getPath() );
            paths.append( ", " );
          }
          if ( paths.length() > 0 ) {
            paths.setLength( paths.length() - 2 );
          }
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.INFO,
            messageGetter.getMessage( LIST_DIRECTORY_TEST_SUCCESS_DESC ),
            messageGetter.getMessage( LIST_DIRECTORY_TEST_SUCCESS_MESSAGE, paths.toString() ) ) );
        } catch ( AccessControlException e ) {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.WARNING,
            messageGetter.getMessage( LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_DESC ),
            messageGetter.getMessage( LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_MESSAGE,
              hadoopFilesystemPath.toString() ),
            e ) );
        } catch ( IOException e ) {
          clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
            messageGetter.getMessage( LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_DESC ),
            messageGetter
              .getMessage( LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_MESSAGE, hadoopFilesystemPath.toString() ),
            e ) );
        }
      }
    } catch ( ClusterInitializationException e ) {
      clusterTestResultEntries.add( new ClusterTestResultEntryImpl( ClusterTestEntrySeverity.FATAL,
        messageGetter.getMessage( LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_DESC ),
        messageGetter.getMessage( LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE, namedCluster.getName() ),
        e ) );
    }
    return clusterTestResultEntries;
  }
}
