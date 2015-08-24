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
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;
import org.pentaho.big.data.impl.cluster.tests.Constants;

import java.util.HashSet;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class PingFileSystemEntryPointTest extends BaseClusterTest {
  public static final String HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST =
    "hadoopFileSystemPingFileSystemEntryPointTest";
  public static final String PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME = "PingFileSystemEntryPointTest.Name";
  private static final Class<?> PKG = PingFileSystemEntryPointTest.class;
  private final MessageGetterFactory messageGetterFactory;
  private final ConnectivityTestFactory connectivityTestFactory;

  public PingFileSystemEntryPointTest( MessageGetterFactory messageGetterFactory,
                                       ConnectivityTestFactory connectivityTestFactory ) {
    super( Constants.HADOOP_FILE_SYSTEM, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME ),
      new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
    return connectivityTestFactory.create( messageGetterFactory, namedCluster.getHdfsHost(), namedCluster.getHdfsPort(),
      true ).runTest();
  }
}
