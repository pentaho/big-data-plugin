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


package org.pentaho.big.data.kettle.plugins.mapreduce;

/**
 * Created by bryan on 1/12/16.
 */
public class DialogClassUtil {
  private static final String PKG_NAME = DialogClassUtil.class.getPackage().getName();
  private static final String UI_PKG_NAME = PKG_NAME + ".ui";

  public static String getDialogClassName( Class<?> clazz ) {
    String className = clazz.getCanonicalName().replace( PKG_NAME, UI_PKG_NAME );
    if ( className.endsWith( "Meta" ) ) {
      className = className.substring( 0, className.length() - 4 );
    }

    className = className + "Dialog";
    return className;
  }
}
