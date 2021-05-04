/*!
 * Copyright 2010 - 2020 Hitachi Vantara.  All rights reserved.
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.pentaho.amazon.s3.S3Util;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.encryption.Encr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public abstract class S3CommonFileSystem extends AbstractFileSystem {

  private static final Logger logger = LoggerFactory.getLogger( S3CommonFileSystem.class );
  private static final String DEFAULT_S3_CONFIG_PROPERTY = "defaultS3Config";
  private String awsAccessKeyCache;
  private String awsSecretKeyCache;
  private AmazonS3 client;
  private Supplier<ConnectionManager> connectionManager = ConnectionManager::getInstance;
  private Map<String, String> currentConnectionProperties;
  private FileSystemOptions currentFileSystemOptions;

  protected S3CommonFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions ) {
    super( rootName, null, fileSystemOptions );
    currentConnectionProperties = new HashMap<>();
  }

  @SuppressWarnings( "unchecked" )
  protected void addCapabilities( Collection caps ) {
    caps.addAll( S3CommonFileProvider.capabilities );
  }

  protected abstract FileObject createFile( AbstractFileName name ) throws Exception;

  public AmazonS3 getS3Client() {
    S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder =
      new S3CommonFileSystemConfigBuilder( getFileSystemOptions() );

    Optional<? extends ConnectionDetails> defaultS3Connection = Optional.empty();
    try {
      defaultS3Connection =
        connectionManager.get().getConnectionDetailsByScheme( "s3" ).stream().filter(
          connectionDetails -> connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ) != null
            && connectionDetails.getProperties().get( DEFAULT_S3_CONFIG_PROPERTY ).equalsIgnoreCase( "true" ) )
          .findFirst();
    } catch ( Exception ignored ) {
      // Ignore the exception, it's OK if we can't find a default S3 connection.
    }

    // If the fileSystemOptions don't contain a name, the originating url is s3:// NOT pvfs://
    // Use a specified default PVFS connection if it's available.
    if ( s3CommonFileSystemConfigBuilder.getName() == null ) {
      // Copy the connection properties
      Map<String, String> newConnectionProperties = new HashMap<>();
      if ( defaultS3Connection.isPresent() ) {
        for ( Map.Entry<String, String> entry : defaultS3Connection.get().getProperties().entrySet() ) {
          newConnectionProperties.put( entry.getKey(), entry.getValue() );
        }
      }

      // Have the default connection properties changed?
      if ( !newConnectionProperties.equals( currentConnectionProperties ) ) {
        // Force a new connection if the default PVFS was changed
        client = null;
        // Track the new connection
        currentConnectionProperties = newConnectionProperties;
        // Clear the file system cache as the credentials have changed and the cache is now invalid.
        this.getFileSystemManager().getFilesCache().clear( this );
      }
    }

    if ( currentFileSystemOptions != null && !currentFileSystemOptions.equals( getFileSystemOptions() ) ) {
      client = null;
      this.getFileSystemManager().getFilesCache().clear( this );
    }

    if ( client == null && getFileSystemOptions() != null ) {
      currentFileSystemOptions = getFileSystemOptions();
      String accessKey = null;
      String secretKey = null;
      String sessionToken = null;
      String region = null;
      String credentialsFilePath = null;
      String profileName = null;
      String endpoint = null;
      String signatureVersion = null;
      String pathStyleAccess = null;

      if ( s3CommonFileSystemConfigBuilder.getName() == null && defaultS3Connection.isPresent() ) {
        accessKey = Encr.decryptPassword( currentConnectionProperties.get( "accessKey" ) );
        secretKey = Encr.decryptPassword( currentConnectionProperties.get( "secretKey" ) );
        sessionToken = Encr.decryptPassword( currentConnectionProperties.get( "sessionToken" ) );
        region = currentConnectionProperties.get( "region" );
        credentialsFilePath = currentConnectionProperties.get( "credentialsFilePath" );
        profileName = currentConnectionProperties.get( "profileName" );
        endpoint = currentConnectionProperties.get( "endpoint" );
        signatureVersion = currentConnectionProperties.get( "signatureVersion" );
        pathStyleAccess = currentConnectionProperties.get( "pathStyleAccess" );
      } else {
        accessKey = s3CommonFileSystemConfigBuilder.getAccessKey();
        secretKey = s3CommonFileSystemConfigBuilder.getSecretKey();
        sessionToken = s3CommonFileSystemConfigBuilder.getSessionToken();
        region = s3CommonFileSystemConfigBuilder.getRegion();
        credentialsFilePath = s3CommonFileSystemConfigBuilder.getCredentialsFile();
        profileName = s3CommonFileSystemConfigBuilder.getProfileName();
        endpoint = s3CommonFileSystemConfigBuilder.getEndpoint();
        signatureVersion = s3CommonFileSystemConfigBuilder.getSignatureVersion();
        pathStyleAccess = s3CommonFileSystemConfigBuilder.getPathStyleAccess();
      }
      boolean access = ( pathStyleAccess == null ) || Boolean.parseBoolean( pathStyleAccess );

      AWSCredentialsProvider awsCredentialsProvider = null;
      Regions regions = Regions.DEFAULT_REGION;

      String keys = S3Util.getKeysFromURI( getRootURI(), S3Util.URI_AWS_CREDENTIALS_REGEX );
      if ( !keys.isEmpty() ) {
        String[] splitKeys = keys.split( ":" );
        accessKey = splitKeys[0];
        secretKey = splitKeys[1];
      }

      if ( !S3Util.isEmpty( accessKey ) && !S3Util.isEmpty( secretKey ) ) {
        AWSCredentials awsCredentials;
        if ( S3Util.isEmpty( sessionToken ) ) {
          awsCredentials = new BasicAWSCredentials( accessKey, secretKey );
        } else {
          awsCredentials = new BasicSessionCredentials( accessKey, secretKey, sessionToken );
        }
        awsCredentialsProvider = new AWSStaticCredentialsProvider( awsCredentials );
        regions = S3Util.isEmpty( region ) ? Regions.DEFAULT_REGION : Regions.fromName( region );
      } else if ( !S3Util.isEmpty( credentialsFilePath ) ) {
        ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile( credentialsFilePath );
        awsCredentialsProvider = new ProfileCredentialsProvider( profilesConfigFile, profileName );
      }

      if ( !S3Util.isEmpty( endpoint ) ) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride(
          S3Util.isEmpty( signatureVersion ) ? S3Util.SIGNATURE_VERSION_SYSTEM_PROPERTY : signatureVersion );
        client = AmazonS3ClientBuilder.standard()
          .withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( endpoint, regions.getName() ) )
          .withPathStyleAccessEnabled( access )
          .withClientConfiguration( clientConfiguration )
          .withCredentials( awsCredentialsProvider )
          .build();
      } else {
        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard()
          .enableForceGlobalBucketAccess()
          .withCredentials( awsCredentialsProvider );
        if ( !isRegionSet() ) {
          clientBuilder.withRegion( regions );
        }
        client = clientBuilder.build();
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
      } catch ( Exception ex ) {
        logger.error( "Could not get an S3Client", ex );
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
    File awsConfigFolder = new File(
      System.getProperty( "user.home" ) + File.separator + S3Util.AWS_FOLDER + File.separator + S3Util.CONFIG_FILE );
    if ( awsConfigFolder.exists() ) {
      return true;
    }
    //When running on an Amazon EC2 instance getCurrentRegion will get its region. Null if not running in an EC2 instance
    return Regions.getCurrentRegion() != null;
  }
}
