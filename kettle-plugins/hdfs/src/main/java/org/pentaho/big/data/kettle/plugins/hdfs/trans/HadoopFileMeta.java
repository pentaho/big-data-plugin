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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.URLFileName;
import org.pentaho.di.core.vfs.KettleVFS;

/**
 * Common functionality for a hadoop based {@link org.pentaho.di.trans.steps.file.BaseFileMeta}.
 */
public interface HadoopFileMeta {

  default String getUrlHostName( final String incomingURL ) {
    String hostName = null;
    final FileName fileName = getUrlFileName( incomingURL );
    if ( fileName instanceof URLFileName ) {
      hostName = ( (URLFileName) fileName ).getHostName();
    }
    return hostName;
  }

  default FileName getUrlFileName( final String incomingURL ) {
    FileName fileName = null;
    try {
      final String noVariablesURL = incomingURL.replaceAll( "[${}]", "/" );
      fileName = KettleVFS.getInstance().getFileSystemManager().resolveURI( noVariablesURL );
    } catch ( FileSystemException e ) {
      // no-op
    }
    return fileName;
  }

  String getUrlPath( final String incomingURL );

  String getClusterName( final String incomingURL );
}
