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
public class GatewayListRootDirectoryTest extends ListRootDirectoryTest {

  public static final String TEST_PATH = "/webhdfs/v1/?op=LISTSTATUS";

  private final ConnectivityTestFactory connectivityTestFactory;

  public GatewayListRootDirectoryTest( MessageGetterFactory messageGetterFactory,
                                       ConnectivityTestFactory connectivityTestFactory,
                                       HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( messageGetterFactory,  hadoopFileSystemLocator);
    this.connectivityTestFactory = connectivityTestFactory;
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;

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