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

package org.pentaho.big.data.impl.cluster.tests.oozie;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;

/**
 * Created by dstepanov on 27/04/17.
 */
public class GatewayPingOozieHostTest extends PingOozieHostTest {
  private static final String TEST_PATH = "/oozie/v1/admin/status";

  public GatewayPingOozieHostTest( MessageGetterFactory messageGetterFactory,
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

    if ( !namedCluster.isUseGateway() ) {
      return super.runTest( objectUnderTest );
    } else {
      return new RuntimeTestResultSummaryImpl( new ClusterRuntimeTestEntry( messageGetterFactory,
        connectivityTestFactory.create( messageGetterFactory,
          variables.environmentSubstitute( namedCluster.getGatewayUrl() ), TEST_PATH,
          variables.environmentSubstitute( namedCluster.getGatewayUsername() ),
          variables.environmentSubstitute( namedCluster.getGatewayPassword() ) )
          .runTest(), ClusterRuntimeTestEntry.DocAnchor.OOZIE ) );
    }
  }
}
