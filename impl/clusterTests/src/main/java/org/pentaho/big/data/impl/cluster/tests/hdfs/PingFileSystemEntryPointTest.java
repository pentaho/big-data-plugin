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

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.big.data.impl.cluster.tests.Constants;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;

import java.util.HashSet;

/**
 * Created by bryan on 8/14/15.
 */
public class PingFileSystemEntryPointTest extends BaseRuntimeTest {
  public static final String HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST =
    "_hadoopFileSystemPingFileSystemEntryPointTest";
  public static final String PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME = "PingFileSystemEntryPointTest.Name";
  private static final Class<?> PKG = PingFileSystemEntryPointTest.class;
  public static final String PING_FILE_SYSTEM_ENTRY_POINT_TEST_IS_MAPR_DESC =
    "PingFileSystemEntryPointTest.isMapr.Desc";
  public static final String PING_FILE_SYSTEM_ENTRY_POINT_TEST_IS_MAPR_MESSAGE =
    "PingFileSystemEntryPointTest.isMapr.Message";
  protected final MessageGetterFactory messageGetterFactory;
  private final MessageGetter messageGetter;
  protected final ConnectivityTestFactory connectivityTestFactory;

  public PingFileSystemEntryPointTest( MessageGetterFactory messageGetterFactory,
                                       ConnectivityTestFactory connectivityTestFactory ) {
    super( NamedCluster.class, Constants.HADOOP_FILE_SYSTEM, HADOOP_FILE_SYSTEM_PING_FILE_SYSTEM_ENTRY_POINT_TEST,
      messageGetterFactory.create( PKG ).getMessage( PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME ), new HashSet<String>() );
    this.messageGetterFactory = messageGetterFactory;
    this.messageGetter = messageGetterFactory.create( PKG );
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override
  public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;
    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );

    // The connectivity test (ping the name node) is not applicable for MapR clusters due to their native client, so
    // just pass this test and move on
    if ( namedCluster.isMapr() ) {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( RuntimeTestEntrySeverity.INFO,
          messageGetter.getMessage( PING_FILE_SYSTEM_ENTRY_POINT_TEST_IS_MAPR_DESC ),
          messageGetter.getMessage( PING_FILE_SYSTEM_ENTRY_POINT_TEST_IS_MAPR_MESSAGE ), null
        )
      );
    } else {

      return new RuntimeTestResultSummaryImpl( new ClusterRuntimeTestEntry( messageGetterFactory,
        connectivityTestFactory.create( messageGetterFactory,
          variables.environmentSubstitute( namedCluster.getHdfsHost() ),
          variables.environmentSubstitute( namedCluster.getHdfsPort() ),
          true ).runTest(), ClusterRuntimeTestEntry.DocAnchor.CLUSTER_CONNECT ) );
    }
  }
}
