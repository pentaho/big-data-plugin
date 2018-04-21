/*!
 * Copyright 2010 - 2018 Hitachi Vantara.  All rights reserved.
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

package org.pentaho.s3n.vfs;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.ProviderCredentials;

import java.util.Collection;

public class S3NFileSystem extends AbstractFileSystem implements FileSystem {

  private S3Service service;

  protected S3NFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    super( rootName, null, fileSystemOptions );
  }

  @SuppressWarnings( "unchecked" ) protected void addCapabilities( Collection caps ) {
    caps.addAll( S3NFileProvider.capabilities );
  }

  protected FileObject createFile( AbstractFileName name ) throws Exception {
    return new S3NFileObject( name, this );
  }

  public S3Service getS3Service() {
    if ( service == null || service.getProviderCredentials() == null
        || service.getProviderCredentials().getAccessKey() == null ) {
      com.amazonaws.auth.AWSCredentials credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
      ProviderCredentials
          awsCredentials =
          new AWSCredentials( credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey() );
      try {
        service = new RestS3Service( awsCredentials );
      } catch ( Throwable t ) {
        System.out.println( "Could not getS3Service() for " + awsCredentials.getLogString() );
        t.printStackTrace();
      }
    }
    return service;
  }
}
