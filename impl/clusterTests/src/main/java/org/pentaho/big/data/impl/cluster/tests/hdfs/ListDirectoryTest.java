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

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.hdfs.exceptions.AccessControlException;
import org.pentaho.di.core.Const;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileStatus;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystem;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemPath;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by bryan on 8/14/15.
 */
public class ListDirectoryTest extends BaseRuntimeTest {
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
  private final MessageGetterFactory messageGetterFactory;
  private final MessageGetter messageGetter;

  public ListDirectoryTest( MessageGetterFactory messageGetterFactory, HadoopFileSystemLocator hadoopFileSystemLocator,
                            String directory, String id, String name ) {
    super( NamedCluster.class, Constants.HADOOP_FILE_SYSTEM, id, name, new HashSet<>(
      Arrays.asList( PingFileSystemEntryPointTest.HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST ) ) );
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.directory = directory;
    this.messageGetterFactory = messageGetterFactory;
    this.messageGetter = messageGetterFactory.create( PKG );
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;
    try {
      HadoopFileSystem hadoopFilesystem = hadoopFileSystemLocator.getHadoopFilesystem( namedCluster );
      if ( hadoopFilesystem == null ) {
        return new RuntimeTestResultSummaryImpl(
          new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
            messageGetter.getMessage( LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_DESC ),
            messageGetter.getMessage( LIST_DIRECTORY_TEST_COULDNT_GET_FILE_SYSTEM_MESSAGE, directory ),
            ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
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
          return new RuntimeTestResultSummaryImpl(
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.INFO,
              messageGetter.getMessage( LIST_DIRECTORY_TEST_SUCCESS_DESC ),
              messageGetter.getMessage( LIST_DIRECTORY_TEST_SUCCESS_MESSAGE, paths.toString() ),
              ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
        } catch ( AccessControlException e ) {
          return new RuntimeTestResultSummaryImpl(
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
              messageGetter.getMessage( LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_DESC ), messageGetter
              .getMessage( LIST_DIRECTORY_TEST_ACCESS_CONTROL_EXCEPTION_MESSAGE, hadoopFilesystemPath.toString() ),
              e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
        } catch ( IOException e ) {
          return new RuntimeTestResultSummaryImpl(
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
              messageGetter.getMessage( LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_DESC ), messageGetter.getMessage(
                LIST_DIRECTORY_TEST_ERROR_LISTING_DIRECTORY_MESSAGE, hadoopFilesystemPath.toString() ), e,
              ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
        }
      }
    } catch ( ClusterInitializationException e ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_DESC ),
          messageGetter.getMessage( LIST_DIRECTORY_TEST_ERROR_INITIALIZING_CLUSTER_MESSAGE, namedCluster.getName() ),
          e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
    }
  }
}
