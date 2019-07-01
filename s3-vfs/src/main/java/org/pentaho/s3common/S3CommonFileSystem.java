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

package org.pentaho.s3common;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.pentaho.amazon.s3.S3Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;

public abstract class S3CommonFileSystem extends AbstractFileSystem {

  private String awsAccessKeyCache;
  private String awsSecretKeyCache;
  private AmazonS3 client;

  private static final Logger logger = LoggerFactory.getLogger( S3CommonFileSystem.class );

  protected S3CommonFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    super( rootName, null, fileSystemOptions );
  }

  @SuppressWarnings( "unchecked" )
  protected void addCapabilities( Collection caps ) {
    caps.addAll( S3CommonFileProvider.capabilities );
  }

  protected abstract FileObject createFile( AbstractFileName name ) throws Exception;

  public AmazonS3 getS3Client() {
    if ( client == null && getFileSystemOptions() != null ) {
      S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder =
        new S3CommonFileSystemConfigBuilder( getFileSystemOptions() );
      String accessKey = s3CommonFileSystemConfigBuilder.getAccessKey();
      String secretKey = s3CommonFileSystemConfigBuilder.getSecretKey();
      if ( !S3Util.isEmpty( accessKey ) && !S3Util.isEmpty( secretKey ) ) {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials( accessKey, secretKey );
        client = AmazonS3ClientBuilder.standard()
          .enableForceGlobalBucketAccess()
          .withRegion( Regions.DEFAULT_REGION )
          .withCredentials( new AWSStaticCredentialsProvider( awsCredentials ) )
          .build();
      }
    }
    if ( client == null || hasClientChangedCredentials() ) {
      try {
        if ( isRegionSet() ) {
          client = AmazonS3ClientBuilder.standard()
            .enableForceGlobalBucketAccess()
            .build();
        } else {
          client = AmazonS3ClientBuilder.standard()
            .enableForceGlobalBucketAccess()
            .withRegion( Regions.DEFAULT_REGION )
            .build();
        }
        awsAccessKeyCache = System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY );
        awsSecretKeyCache = System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY );
      } catch ( Throwable t ) {
        logger.error( "Could not get an S3Client", t );
      }
    }
    return client;
  }

  private boolean hasClientChangedCredentials() {
    return client != null
      && ( S3Util.hasChanged( awsAccessKeyCache, System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY ) )
      || S3Util.hasChanged( awsSecretKeyCache, System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY ) ) );
  }

  private boolean isRegionSet() {
    //region is set if explicitly set in env variable or configuration file is explicitly set
    if ( System.getenv( S3Util.AWS_REGION ) != null || System.getenv( S3Util.AWS_CONFIG_FILE ) != null ) {
      return true;
    }
    //check if configuration file exists in default location
    File awsConfigFolder = new File( System.getProperty( "user.home" ) + File.separator + S3Util.AWS_FOLDER + File.separator + S3Util.CONFIG_FILE );
    if ( awsConfigFolder.exists() ) {
      return true;
    }
    return false;
  }
}
