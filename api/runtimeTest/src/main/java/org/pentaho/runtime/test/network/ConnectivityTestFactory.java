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


package org.pentaho.runtime.test.network;

import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

/**
 * Created by bryan on 8/24/15.
 */
public interface ConnectivityTestFactory {
  ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port,
                           boolean haPossible );

  ConnectivityTest create( MessageGetterFactory messageGetterFactory, String hostname, String port, boolean haPossible,
                           RuntimeTestEntrySeverity severityOfFailures );

  ConnectivityTest create( MessageGetterFactory messageGetterFactory, String url, String testPath,
                           String user, String password );

  ConnectivityTest create( MessageGetterFactory messageGetterFactory, String url, String testPath,
                           String user, String password, RuntimeTestEntrySeverity severityOfFailures );
}
