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


package org.pentaho.big.data.impl.cluster.tests.zookeeper;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;

/**
 * Created by dstepanov on 27/04/17.
 */
public class GatewayPingZookeeperEnsembleTest extends PingZookeeperEnsembleTest {

  public static final String GATEWAY_PING_ZOOKEEPER_NOT_SUPPORT_DESC =
    "GatewayPingZookeeperEnsembleTest.ZookeeperNotSupport.Desc";
  public static final String GATEWAY_PING_ZOOKEEPER_NOT_SUPPORT_MESSAGE =
    "GatewayPingZookeeperEnsembleTest.ZookeeperNotSupport.Message";

  public GatewayPingZookeeperEnsembleTest( MessageGetterFactory messageGetterFactory,
                                           ConnectivityTestFactory connectivityTestFactory ) {
    super( messageGetterFactory, connectivityTestFactory );
  }

  @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;

    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );

    if ( !namedCluster.isUseGateway()  ) {
      return super.runTest( objectUnderTest );
    } else {
      return new RuntimeTestResultSummaryImpl(
        new ClusterRuntimeTestEntry( RuntimeTestEntrySeverity.SKIPPED,
          messageGetter.getMessage( GATEWAY_PING_ZOOKEEPER_NOT_SUPPORT_DESC ),
          messageGetter.getMessage( GATEWAY_PING_ZOOKEEPER_NOT_SUPPORT_MESSAGE ), null
        )
      );
    }
  }
}
