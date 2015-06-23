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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.url.UrlFileName;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class S3FileName extends UrlFileName {

  private static final char[] USERNAME_RESERVED = { ':', '@', '/' };
  private static final char[] PASSWORD_RESERVED = { '@', '/', '?', '+' };

  public S3FileName( final String scheme, final String hostName, final int port, final int defaultPort,
                     final String userName, final String password, final String path, final FileType type,
                     final String queryString ) {
    super( scheme, hostName, port, defaultPort, userName, password, path, type, queryString );
  }

  @Override
  public String getFriendlyURI() {
    return super.getFriendlyURI();
  }

  public FileName createName( final String absPath, FileType type ) {
    return new S3FileName( getScheme(),
      getHostName(),
      getPort(),
      getDefaultPort(),
      getUserName(),
      getPassword(),
      absPath,
      type,
      getQueryString() );
  }


  /**
   * Builds the root URI for this file name.
   */
  protected void appendRootUri( final StringBuilder buffer, boolean addPassword ) {
    buffer.append( getScheme() );
    buffer.append( "://" );
    if ( addPassword ) {
      appendCredentials( buffer, addPassword );
    }
    buffer.append( getHostName() );
    if ( getPort() != getDefaultPort() ) {
      buffer.append( ':' );
      buffer.append( getPort() );
    }
  }

  /**
   * append the user credentials
   */
  @Override
  protected void appendCredentials( StringBuilder buffer, boolean addPassword ) {
    String userName = getUserName();
    String password = getPassword();

    if ( addPassword && userName != null && userName.length() != 0 ) {
      try {
        userName = URLEncoder.encode( getUserName(), "UTF-8" );
        buffer.append( userName );
      } catch ( UnsupportedEncodingException e ) {
        // fall back to the default
        UriParser.appendEncoded( buffer, userName, USERNAME_RESERVED );
      }

      if ( password != null && password.length() != 0 ) {
        buffer.append( ':' );
        try {
          password = URLEncoder.encode( getPassword(), "UTF-8" );
          buffer.append( password );
        } catch ( UnsupportedEncodingException e ) {
          // fall back to the default
          UriParser.appendEncoded( buffer, password, PASSWORD_RESERVED );
        }
      }
      buffer.append( '@' );
    }
  }


}
