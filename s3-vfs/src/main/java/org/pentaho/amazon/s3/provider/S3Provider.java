/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.amazon.s3.provider;

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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.amazon.s3.S3Details;
import org.pentaho.amazon.s3.S3Util;
import org.pentaho.di.connections.ConnectionDetails;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.vfs.BaseVFSConnectionProvider;
import org.pentaho.di.connections.vfs.VFSRoot;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.s3common.S3CommonFileSystemConfigBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Created by bmorrise on 2/5/19.
 */
@SuppressWarnings( "WeakerAccess" )
public class S3Provider extends BaseVFSConnectionProvider<S3Details> {

  private static final String ACCESS_KEY_SECRET_KEY = "0";
  private static final String CREDENTIALS_FILE = "1";
  public static final String NAME = "Amazon S3";
  private final Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private final Supplier<VariableSpace> variableSpace = Variables::getADefaultVariableSpace;
  private final LogChannelInterface log = new LogChannel( this );

  @Override
  public Class<S3Details> getClassType() {
    return S3Details.class;
  }

  @Override
  public FileSystemOptions getOpts( S3Details s3Details ) {
    S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder =
      new S3CommonFileSystemConfigBuilder( new FileSystemOptions() );
    s3CommonFileSystemConfigBuilder.setName( s3Details.getName() );
    s3CommonFileSystemConfigBuilder.setAccessKey( s3Details.getAccessKey() );
    s3CommonFileSystemConfigBuilder.setSecretKey( s3Details.getSecretKey() );
    s3CommonFileSystemConfigBuilder.setSessionToken( s3Details.getSessionToken() );
    s3CommonFileSystemConfigBuilder.setRegion( s3Details.getRegion() );
    s3CommonFileSystemConfigBuilder.setCredentialsFile( s3Details.getCredentialsFilePath() );
    s3CommonFileSystemConfigBuilder.setProfileName( s3Details.getProfileName() );
    s3CommonFileSystemConfigBuilder.setEndpoint( s3Details.getEndpoint() );
    s3CommonFileSystemConfigBuilder.setPathStyleAccess( s3Details.getPathStyleAccess() );
    s3CommonFileSystemConfigBuilder.setSignatureVersion( s3Details.getSignatureVersion() );
    s3CommonFileSystemConfigBuilder.setDefaultS3Config( s3Details.getDefaultS3Config() );
    s3CommonFileSystemConfigBuilder.setConnectionType( s3Details.getConnectionType() );
    return s3CommonFileSystemConfigBuilder.getFileSystemOptions();
  }

  @SuppressWarnings( "unchecked" )
  @Override public List<S3Details> getConnectionDetails() {
    return (List<S3Details>) connectionManagerSupplier.get().getConnectionDetailsByScheme( getKey() );
  }

  @Override public List<VFSRoot> getLocations( S3Details s3Details ) {
    List<VFSRoot> buckets = new ArrayList<>();
    try {
      AmazonS3 s3 = getAmazonS3( s3Details );
      if ( s3 != null ) {
        for ( Bucket bucket : s3.listBuckets() ) {
          buckets.add( new VFSRoot( bucket.getName(), bucket.getCreationDate() ) );
        }
      }
    } catch ( Exception e ) {
      log.logError( e.getMessage(), e );
    }
    return buckets;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getKey() {
    return S3FileProvider.SCHEME;
  }

  @Override public String getProtocol( S3Details s3Details ) {
    return S3FileProvider.SCHEME;
  }

  @Override public boolean test( S3Details s3Details ) {
    s3Details = prepare( s3Details );
    AmazonS3 amazonS3 = getAmazonS3( s3Details );
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // make sure that sax parsing classes are loaded from this cl.
      // TCCL may have been set to a bundle classloader.
      Thread.currentThread().setContextClassLoader( getClass().getClassLoader() );
      Objects.requireNonNull( amazonS3 ).getS3AccountOwner();

      List<? extends ConnectionDetails> connections =
        connectionManagerSupplier.get().getConnectionDetailsByScheme( S3FileProvider.SCHEME );

      if ( s3Details.getDefaultS3Config() != null && s3Details.getDefaultS3Config().equalsIgnoreCase( "true" ) ) {
        for ( ConnectionDetails details : connections ) {
          if ( !s3Details.getName().equalsIgnoreCase( details.getName() ) ) {
            S3Details removeDefault = (S3Details) details;
            removeDefault.setAccessKey( Encr.decryptPasswordOptionallyEncrypted( removeDefault.getAccessKey() ) );
            removeDefault.setSecretKey( Encr.decryptPasswordOptionallyEncrypted( removeDefault.getSecretKey() ) );
            removeDefault.setSessionToken( Encr.decryptPasswordOptionallyEncrypted( removeDefault.getSessionToken() ) );
            removeDefault.setCredentialsFile(
              Encr.decryptPasswordOptionallyEncrypted( removeDefault.getCredentialsFile() ) );
            removeDefault.setDefaultS3Config( "false" );
            connectionManagerSupplier.get().save( removeDefault );
          }
        }
      }
      return true;
    } catch ( AmazonS3Exception e ) {
      // expected exception if credentials are invalid.  Log just the message.
      log.logError( e.getMessage() );
      return false;
    } catch ( Exception e ) {
      // unexpected exception, log the whole stack
      log.logError( e.getMessage(), e );
      return false;
    } finally {
      Thread.currentThread().setContextClassLoader( cl );
    }
  }

  @Override public S3Details prepare( S3Details s3Details ) {
    if ( s3Details.getAuthType().equals( CREDENTIALS_FILE ) ) {
      String credetialsFilePath = getVar( s3Details.getCredentialsFilePath(), variableSpace.get() );
      if ( credetialsFilePath != null ) {
        try ( BufferedReader reader = Files.newBufferedReader( Paths.get( credetialsFilePath ) ) ) {
          StringBuilder builder = new StringBuilder();
          String currentLine;
          while ( ( currentLine = reader.readLine() ) != null ) {
            builder.append( currentLine ).append( "\n" );
          }
          s3Details.setCredentialsFile( builder.toString() );
        } catch ( IOException e ) {
          return null;
        }
      }
    }
    return s3Details;
  }

  private AmazonS3 getAmazonS3( S3Details s3Details ) {
    AWSCredentials awsCredentials;
    AWSCredentialsProvider awsCredentialsProvider = null;

    String accessKey = getVar( s3Details.getAccessKey(), variableSpace.get() );
    String secretKey = getVar( s3Details.getSecretKey(), variableSpace.get() );
    String sessionToken = getVar( s3Details.getSessionToken(), variableSpace.get() );
    String credentialsFilePath = getVar( s3Details.getCredentialsFilePath(), variableSpace.get() );
    String profileName = getVar( s3Details.getProfileName(), variableSpace.get() );

    String endpoint = getVar( s3Details.getEndpoint(), variableSpace.get() );
    String pathStyleAccess =
      getBooleanStringOfVariable( s3Details.getPathStyleAccessVariable(), s3Details.getPathStyleAccess(),
        variableSpace.get() );
    String signatureVersion = getVar( s3Details.getSignatureVersion(), variableSpace.get() );
    boolean access = ( pathStyleAccess == null ) || Boolean.parseBoolean( pathStyleAccess );

    if ( s3Details.getAuthType().equals( ACCESS_KEY_SECRET_KEY ) ) {
      if ( S3Util.isEmpty( s3Details.getSessionToken() ) ) {
        awsCredentials = new BasicAWSCredentials( accessKey, secretKey );
      } else {
        awsCredentials =
          new BasicSessionCredentials( accessKey, secretKey, sessionToken );
      }
      awsCredentialsProvider = new AWSStaticCredentialsProvider( awsCredentials );
    }
    if ( s3Details.getAuthType().equals( CREDENTIALS_FILE ) ) {
      ProfilesConfigFile profilesConfigFile = new ProfilesConfigFile( credentialsFilePath );
      awsCredentialsProvider = new ProfileCredentialsProvider( profilesConfigFile, profileName );
    }
    Regions regions =
      !S3Util.isEmpty( s3Details.getRegion() ) ? Regions.fromName( s3Details.getRegion() ) : Regions.DEFAULT_REGION;
    if ( awsCredentialsProvider != null && S3Util.isEmpty( s3Details.getEndpoint() ) ) {
      return AmazonS3ClientBuilder.standard().withCredentials( awsCredentialsProvider )
        .enableForceGlobalBucketAccess().withRegion( regions ).build();
    } else if ( awsCredentialsProvider != null && !S3Util.isEmpty( s3Details.getEndpoint() ) ) {
      ClientConfiguration clientConfiguration = new ClientConfiguration();
      clientConfiguration.setSignerOverride(
        S3Util.isEmpty( signatureVersion ) ? S3Util.SIGNATURE_VERSION_SYSTEM_PROPERTY : signatureVersion );
      return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( endpoint, regions.getName() ) )
        .withPathStyleAccessEnabled( access )
        .withClientConfiguration( clientConfiguration )
        .withCredentials( awsCredentialsProvider )
        .build();
    }
    return null;
  }

  private String getVar( String value, VariableSpace variableSpace ) {
    if ( variableSpace != null ) {
      return variableSpace.environmentSubstitute( value );
    }
    return value;
  }

  private String getBooleanStringOfVariable( String variableName, String defaultValue, VariableSpace space ) {
    return String.valueOf( getBooleanValueOfVariable( variableName, defaultValue, space ) );
  }

  private boolean getBooleanValueOfVariable( String variableName, String defaultValue, VariableSpace space ) {
    if ( !Utils.isEmpty( variableName ) ) {
      String value = space.environmentSubstitute( variableName );
      if ( !Utils.isEmpty( value ) ) {
        Boolean b = ValueMetaBase.convertStringToBoolean( value );
        return b != null && b;
      }
    }
    return Objects.equals( Boolean.TRUE, ValueMetaBase.convertStringToBoolean( defaultValue ) );
  }

}
