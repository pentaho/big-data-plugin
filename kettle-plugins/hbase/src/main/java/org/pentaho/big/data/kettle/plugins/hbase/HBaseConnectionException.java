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
 * @author Tatsiana_Kasiankova
 *
 */
public class HBaseConnectionException extends Exception {

  private static final long serialVersionUID = -6215675067801506240L;

  public HBaseConnectionException( String message, Throwable cause ) {
    super( message, cause );
  }

}
