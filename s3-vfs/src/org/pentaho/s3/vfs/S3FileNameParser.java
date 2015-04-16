/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.s3.vfs;

import org.apache.commons.vfs.FileName;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.provider.FileNameParser;
import org.apache.commons.vfs.provider.URLFileName;
import org.apache.commons.vfs.provider.URLFileNameParser;
import org.apache.commons.vfs.provider.VfsComponentContext;


public class S3FileNameParser extends URLFileNameParser {

  private static final S3FileNameParser INSTANCE = new S3FileNameParser();

  public S3FileNameParser() {
    // S3 wants port 843, but the web service will use this by default
    super( 843 );
  }

  public static FileNameParser getInstance() {
    return INSTANCE;
  }

  @Override
  public FileName parseUri( VfsComponentContext vfsComponentContext, FileName fileName, String s )
    throws FileSystemException {
    if ( fileName == null ) {
      s = encodeAccessKeys( s );
    }
    URLFileName name = (URLFileName) super.parseUri( vfsComponentContext, fileName, s );
    FileType type = name.getType();

    /* There is a problem with parsing bucket uri which has not char "/" at the end.
     * In this case UrlParser parse URI and return filename with type file.
     * As S3 does not allow to store files without buckets - so bucket is always a folder
      */
    if ( FileType.FILE.equals( type ) && name.getPath().split( "/" ).length == 2 ) {
      type = FileType.FOLDER;
    }
    String user = name.getUserName();
    String password = name.getPassword();
    return new S3FileName(
      name.getScheme(),
      name.getHostName(),
      name.getPort(),
      getDefaultPort(),
      user,
      password,
      name.getPath(),
      type,
      name.getQueryString() );
  }

  public String encodeAccessKeys( String url ) {
    int hostNameIndex = url.indexOf( "@s3" ) == -1 ? url.indexOf( "@S3" ) : url.indexOf( "@s3" );
    if ( url.startsWith( "s3://" ) && hostNameIndex != -1 ) {
      try {
        String auth = url.substring( 5, hostNameIndex );

        // access key is everything up to the first colon (:)
        String accessKey = auth.substring( 0, auth.indexOf( ":" ) ).replaceAll( "\\+", "%2B" ).replaceAll( "/", "%2F" );

        // secret key is everything after it
        String secretKey =
          auth.substring( auth.indexOf( ":" ) + 1 ).replaceAll( "\\+", "%2B" ).replaceAll( "/", "%2F" );

        return "s3://" + accessKey + ":" + secretKey + url.substring( hostNameIndex );
      } catch ( StringIndexOutOfBoundsException e ) {
        return url;
      }
    } else {
      return url;
    }
  }
}
