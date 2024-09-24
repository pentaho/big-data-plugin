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

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.URLFileName;
import org.apache.commons.vfs2.provider.URLFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;

public class HDFSFileNameParser extends URLFileNameParser {

  private static final HDFSFileNameParser INSTANCE = new HDFSFileNameParser();

  private HDFSFileNameParser() {
    super( -1 );
  }

  public static HDFSFileNameParser getInstance() {
    return INSTANCE;
  }

  @Override public FileName parseUri( VfsComponentContext context, FileName base, String filename )
    throws FileSystemException {
    URLFileName fileNameURLFileName = (URLFileName) super.parseUri( context, base, filename );

    return new URLFileName(
      fileNameURLFileName.getScheme(),
      getHostNameCaseSensitive( filename ),
      fileNameURLFileName.getPort(),
      fileNameURLFileName.getDefaultPort(),
      fileNameURLFileName.getUserName(),
      fileNameURLFileName.getPassword(),
      fileNameURLFileName.getPath(),
      fileNameURLFileName.getType(),
      fileNameURLFileName.getQueryString() );
  }

  /**
   * PDI-15565
   * <p>
   * the same logic as for extracting in org.apache.commons.vfs2.provider.HostFileNameParser.extractToPath
   *
   * @param fileUri file uri for hdfs file
   * @return case sensitive host name
   * @throws FileSystemException when format of url is not correct
   */
  private String getHostNameCaseSensitive( String fileUri ) throws FileSystemException {
    StringBuilder fullNameBuilder = new StringBuilder();
    UriParser.extractScheme( fileUri, fullNameBuilder );
    if ( fullNameBuilder.length() < 2 || fullNameBuilder.charAt( 0 ) != '/' || fullNameBuilder.charAt( 1 ) != '/' ) {
      throw new FileSystemException( "vfs.provider/missing-double-slashes.error", fileUri );
    }
    fullNameBuilder.delete( 0, 2 );
    extractPort( fullNameBuilder, fileUri );
    extractUserInfo( fullNameBuilder );
    return extractHostName( fullNameBuilder );
  }
}
