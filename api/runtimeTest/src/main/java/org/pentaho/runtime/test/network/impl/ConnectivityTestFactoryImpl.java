/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test.network.impl;

import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTest;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

import java.net.URI;

/**
 * Created by bryan on 8/24/15.
 */
public class ConnectivityTestFactoryImpl implements ConnectivityTestFactory {
  @Override public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                                            boolean haPossible ) {
    return create( messageGetterFactory, hostname, port, haPossible, RuntimeTestEntrySeverity.FATAL );
  }

  @Override public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                                            boolean haPossible, RuntimeTestEntrySeverity severityOfFailures ) {
    return new ConnectivityTestImpl( messageGetterFactory, hostname, port, haPossible, severityOfFailures );
  }

  @Override
  public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String url, String testPath,
                                  String user, String password ) {
    return new GatewayConnectivityTestImpl( messageGetterFactory, URI.create( url ), testPath, user, password,
      RuntimeTestEntrySeverity.FATAL );
  }

  @Override
  public ConnectivityTest create( MessageGetterFactory messageGetterFactory, String url, String testPath,
                                  String user, String password, RuntimeTestEntrySeverity severityOfFailures ) {
    return new GatewayConnectivityTestImpl( messageGetterFactory, URI.create( url ), testPath, user, password,
      severityOfFailures );
  }
}
