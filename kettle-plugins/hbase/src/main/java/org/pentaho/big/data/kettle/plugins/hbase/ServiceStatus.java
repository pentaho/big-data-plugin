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

package org.pentaho.big.data.kettle.plugins.hbase;


/**
 * Helper class that shows HBaseService status of a Step.
 */
public class ServiceStatus {

  public static ServiceStatus OK = new ServiceStatus();

  private boolean ok = true;
  private Exception exception;

  private ServiceStatus() {
  }

  private ServiceStatus( Exception exception ) {
    this.ok = false;
    this.exception = exception;
  }

  public boolean isOk() {
    return ok;
  }

  public Exception getException() {
    return exception;
  }

  public static ServiceStatus notOk( Exception e ) {
    return new ServiceStatus( e );
  }
}
