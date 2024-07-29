/*!
* Copyright 2010 - 2024 Hitachi Vantara.  All rights reserved.
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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;

/**
 * Custom filename that represents an S3 file with the bucket and its relative path
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3FileName extends AbstractFileName {
  public static final String DELIMITER = "/";

  private String bucketId;
  private String bucketRelativePath;
  private String keys;

  public S3FileName( String scheme, String bucketId, String path, FileType type, String keys ) {
    this( scheme, bucketId, path, type );
    this.keys = keys;
  }

  public S3FileName( String scheme, String bucketId, String path, FileType type ) {
    super( scheme, StringUtils.prependIfMissing( path, DELIMITER ), type );

    this.bucketId = bucketId;

    if ( path.length() > 1 ) {
      this.bucketRelativePath = path.substring( 1 );
      if ( type.equals( FileType.FOLDER ) ) {
        this.bucketRelativePath += DELIMITER;
      }
    } else {
      this.bucketRelativePath = "";
    }
  }

  @Override
  public String getURI() {
    final StringBuilder buffer = new StringBuilder();
    appendRootUri( buffer, false );
    buffer.append( getPath() );
    return buffer.toString();
  }

  public String getBucketId() {
    return bucketId;
  }

  public String getBucketRelativePath() {
    return bucketRelativePath;
  }

  @Override
  public FileName createName( String absPath, FileType type ) {
    return new S3FileName( getScheme(), bucketId, absPath, type );
  }

  @Override
  protected void appendRootUri( StringBuilder buffer, boolean addPassword ) {
    buffer.append( getScheme() );
    buffer.append( ":/" );
    if ( keys != null ) {
      buffer.append( '/' ).append( bucketId );
    }
  }
}
