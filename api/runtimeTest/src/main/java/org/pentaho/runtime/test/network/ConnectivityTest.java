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


package org.pentaho.runtime.test.network;


import org.pentaho.runtime.test.result.RuntimeTestResultEntry;

/**
 * Created by bryan on 8/24/15.
 */
public interface ConnectivityTest {
  RuntimeTestResultEntry runTest();
}
