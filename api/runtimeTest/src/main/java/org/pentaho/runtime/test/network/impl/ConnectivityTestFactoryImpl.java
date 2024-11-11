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
