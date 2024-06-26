/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
