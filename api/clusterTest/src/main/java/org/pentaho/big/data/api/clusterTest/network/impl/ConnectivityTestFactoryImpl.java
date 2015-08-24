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

package org.pentaho.big.data.api.clusterTest.network.impl;

import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTest;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;

/**
 * Created by bryan on 8/24/15.
 */
public class ConnectivityTestFactoryImpl implements ConnectivityTestFactory {
  @Override public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                                            boolean haPossible ) {
    return create( messageGetterFactory, hostname, port, haPossible, ClusterTestEntrySeverity.FATAL );
  }

  @Override public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                                            boolean haPossible, ClusterTestEntrySeverity severityOfFailures ) {
    return new ConnectivityTestImpl( messageGetterFactory, hostname, port, haPossible, severityOfFailures );
  }
}
