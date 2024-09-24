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
