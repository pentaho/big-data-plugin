/*!
* Copyright 2010 - 2019 Hitachi Vantara.  All rights reserved.
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

import java.util.Collection;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.UserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.commons.vfs2.provider.URLFileName;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;
import org.jets3t.service.security.ProviderCredentials;
import org.pentaho.amazon.s3.S3Util;

public class S3FileSystem extends AbstractFileSystem implements FileSystem {

  private S3Service service;

  private String awsAccessKeyCache;

  private String awsSecretKeyCache;


  protected S3FileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    super( rootName, null, fileSystemOptions );
  }

  @SuppressWarnings( "unchecked" )
  protected void addCapabilities( Collection caps ) {
    caps.addAll( S3FileProvider.capabilities );
  }

  protected FileObject createFile( AbstractFileName name ) throws Exception {
    return new S3FileObject( name, this );
  }

  public S3Service getS3Service() {
    String awsAccessKeySystemEnvValue = System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY );
    String awsSecretKeySystemEnvValue = System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY );

    if ( service == null || service.getProviderCredentials() == null
      || service.getProviderCredentials().getAccessKey() == null || ( service != null
        && S3Util.hasChanged( awsAccessKeyCache, awsAccessKeySystemEnvValue )
            && S3Util.hasChanged( awsSecretKeyCache, awsSecretKeySystemEnvValue ) ) ) {

      UserAuthenticator userAuthenticator =
        DefaultFileSystemConfigBuilder.getInstance().getUserAuthenticator( getFileSystemOptions() );

      String awsAccessKey = null;
      String awsSecretKey = null;

      if ( userAuthenticator != null ) {
        UserAuthenticationData data = userAuthenticator.requestAuthentication( S3FileProvider.AUTHENTICATOR_TYPES );
        awsAccessKey = String.valueOf( data.getData( UserAuthenticationData.USERNAME ) );
        awsSecretKey = String.valueOf( data.getData( UserAuthenticationData.PASSWORD ) );
      } else if ( awsAccessKeySystemEnvValue != null && awsSecretKeySystemEnvValue != null ) {
        awsAccessKey = awsAccessKeySystemEnvValue;
        awsSecretKey = awsSecretKeySystemEnvValue;
        awsAccessKeyCache = awsAccessKeySystemEnvValue;
        awsSecretKeyCache = awsSecretKeySystemEnvValue;
      } else {
        awsAccessKey = ( (URLFileName) getRootName() ).getUserName();
        awsSecretKey = ( (URLFileName) getRootName() ).getPassword();
      }
      ProviderCredentials awsCredentials = new AWSCredentials( awsAccessKey, awsSecretKey );
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
