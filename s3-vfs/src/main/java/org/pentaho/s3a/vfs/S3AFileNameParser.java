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


package org.pentaho.s3a.vfs;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileNameParser;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import org.pentaho.amazon.s3.S3Util;

/**
 * Custom parser for the s3a URL
 *
 * @author asimoes
 * @since 09-11-2017
 */

public class S3AFileNameParser extends AbstractFileNameParser {
  private static final S3AFileNameParser INSTANCE = new S3AFileNameParser();
  private static final String[] SCHEMES = new String[] { S3AFileProvider.SCHEME };

  private S3AFileNameParser() {
    super();
  }

  public static FileNameParser getInstance() {
    return INSTANCE;
  }

  public FileName parseUri( VfsComponentContext context, FileName base, String uri ) throws FileSystemException {
    StringBuilder buffer = new StringBuilder();

    String scheme = UriParser.extractScheme( SCHEMES, uri, buffer );
    UriParser.canonicalizePath( buffer, 0, buffer.length(), this );

    // Normalize separators in the path
    UriParser.fixSeparators( buffer );

    // Normalise the path
    FileType fileType = UriParser.normalisePath( buffer );

    //URI includes credentials
    String keys = S3Util.getFullKeysFromURI( buffer.toString() );
    if ( keys != null ) {
      buffer.replace( buffer.indexOf( keys ), buffer.indexOf( keys ) + keys.length(), "" );
    }

    String path = buffer.toString();
    // Extract bucket name
    String bucketName = UriParser.extractFirstElement( buffer );

    if ( keys != null ) {
      bucketName = keys + bucketName;
      return new S3AFileName( scheme, bucketName, buffer.length() == 0 ? path : buffer.toString(), fileType, keys );
    }
    return new S3AFileName( scheme, bucketName, path, fileType );
  }
}
