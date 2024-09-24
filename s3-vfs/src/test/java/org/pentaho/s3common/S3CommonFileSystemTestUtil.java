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

package org.pentaho.s3common;

import org.mockito.Mockito;

import static org.mockito.Mockito.doReturn;

public class S3CommonFileSystemTestUtil {

  public static S3CommonFileSystem stubRegionUnSet( S3CommonFileSystem fileSystem ) {
    S3CommonFileSystem fileSystemSpy = Mockito.spy( fileSystem );
    doReturn(false).when(fileSystemSpy).isRegionSet();
    return fileSystemSpy;
  }
}
