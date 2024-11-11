/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;

/**
 * Created by vamshidhar on 02/02/23.
 */
public class GatewayListHomeDirectoryTest extends ListHomeDirectoryTest {

  public static final String TEST_PATH = "/webhdfs/v1/~?op=LISTSTATUS";

  private final ConnectivityTestFactory connectivityTestFactory;

  public GatewayListHomeDirectoryTest( MessageGetterFactory messageGetterFactory,
                                       ConnectivityTestFactory connectivityTestFactory,
                                       HadoopFileSystemLocator fileSystemLocator ) {
    super( messageGetterFactory, fileSystemLocator );
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;

    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );

    if ( !namedCluster.isUseGateway() ) {
      return super.runTest( objectUnderTest );
    } else {
      return new RuntimeTestResultSummaryImpl( new ClusterRuntimeTestEntry( messageGetterFactory,
        connectivityTestFactory.create( messageGetterFactory,
            variables.environmentSubstitute( namedCluster.getGatewayUrl() ), TEST_PATH,
            variables.environmentSubstitute( namedCluster.getGatewayUsername() ),
            variables.environmentSubstitute( namedCluster.decodePassword( namedCluster.getGatewayPassword() ) ) )
          .runTest(), ClusterRuntimeTestEntry.DocAnchor.CLUSTER_CONNECT_GATEWAY ) );
    }
  }
}