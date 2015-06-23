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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.jets3t.service.security.AWSCredentials;

/**
 * created by: rfellows date:       5/21/12
 */
public class S3FileUtil {

  public static FileObject resolveFile( String fileUri, UserAuthenticator userAuthenticator )
    throws FileSystemException {
    FileSystemOptions opts = null;
    if ( VFS.getManager().getBaseFile() != null ) {
      opts = VFS.getManager().getBaseFile().getFileSystem().getFileSystemOptions();
    }
    if ( opts == null ) {
      opts = new FileSystemOptions();
    }
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts, userAuthenticator );
    FileObject file = resolveFile( fileUri, opts );
    return file;
  }

  public static FileObject resolveFile( String fileUri, String username, String password ) throws FileSystemException {
    StaticUserAuthenticator userAuthenticator = new StaticUserAuthenticator( null, username, password );
    return resolveFile( fileUri, userAuthenticator );
  }

  public static FileObject resolveFile( String fileUri, AWSCredentials credentials ) throws FileSystemException {
    return resolveFile( fileUri, credentials.getAccessKey(), credentials.getSecretKey() );
  }

  public static FileObject resolveFile( String fileUri, FileSystemOptions opts ) throws FileSystemException {
    FileObject file = VFS.getManager().resolveFile( fileUri, opts );
    return file;
  }

}
