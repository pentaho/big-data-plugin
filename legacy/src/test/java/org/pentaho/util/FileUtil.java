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

package org.pentaho.util;

import java.io.File;

/**
 * Class to provide simple File services.
 * 
 * @author sflatley
 */
public class FileUtil {

  public static synchronized boolean deleteDir( File dir ) {

    if ( dir.isDirectory() ) {
      String[] children = dir.list();
      for ( int i = 0; i < children.length; i++ ) {
        boolean success = deleteDir( new File( dir, children[i] ) );
        if ( !success ) {
          return false;
        }
      }
    } // The directory is now empty so delete it
    return dir.delete();
  }
}
