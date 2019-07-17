/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfilesConfigFile;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.amazon.s3.S3Details;
import org.pentaho.amazon.s3.S3Util;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.vfs.BaseVFSConnectionProvider;
import org.pentaho.di.connections.vfs.VFSRoot;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.s3common.S3CommonFileSystemConfigBuilder;
import org.pentaho.s3n.vfs.S3NFileProvider;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by bmorrise on 2/5/19.
 */
public class S3Provider extends BaseVFSConnectionProvider<S3Details> {

  private static final String ACCESS_KEY_SECRET_KEY = "0";
  private static final String CREDENTIALS_FILE = "1";
  public static final String NAME = "Amazon S3";
  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

  private Supplier<VariableSpace> variableSpace = Variables::getADefaultVariableSpace;

  @Override
  public Class<S3Details> getClassType() {
    return S3Details.class;
  }

  @Override
  public FileSystemOptions getOpts( S3Details s3Details ) {
    S3CommonFileSystemConfigBuilder s3CommonFileSystemConfigBuilder =
      new S3CommonFileSystemConfigBuilder( new FileSystemOptions() );
    s3CommonFileSystemConfigBuilder.setAccessKey( s3Details.getAccessKey() );
    s3CommonFileSystemConfigBuilder.setSecretKey( s3Details.getSecretKey() );
    s3CommonFileSystemConfigBuilder.setSessionToken( s3Details.getSessionToken() );
    s3CommonFileSystemConfigBuilder.setRegion( s3Details.getRegion() );
    s3CommonFileSystemConfigBuilder.setCredentialsFile( s3Details.getCredentialsFilePath() );
    s3CommonFileSystemConfigBuilder.setProfileName( s3Details.getProfileName() );
    return s3CommonFileSystemConfigBuilder.getFileSystemOptions();
  }

  @SuppressWarnings( "unchecked" )
  @Override public List<S3Details> getConnectionDetails() {
    return (List<S3Details>) connectionManagerSupplier.get().getConnectionDetailsByScheme( getKey() );
  }

  @Override public List<VFSRoot> getLocations( S3Details s3Details ) {
    return Collections.singletonList( new VFSRoot( S3NFileProvider.SCHEME, new Date() ) );
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getKey() {
    return S3NFileProvider.SCHEME;
  }

  @Override public String getProtocol( S3Details s3Details ) {
    return S3NFileProvider.SCHEME;
  }

  @Override public boolean test( S3Details s3Details ) {
    s3Details = prepare( s3Details );
    AmazonS3 amazonS3 = getAmazonS3( s3Details );
    try {
      amazonS3.getS3AccountOwner();
      return true;
    } catch ( Exception e ) {
      return false;
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
    AWSCredentials awsCredentials = null;
    AWSCredentialsProvider awsCredentialsProvider = null;

    String accessKey = getVar( s3Details.getAccessKey(), variableSpace.get() );
    String secretKey = getVar( s3Details.getSecretKey(), variableSpace.get() );
    String sessionToken = getVar( s3Details.getSessionToken(), variableSpace.get() );
    String credentialsFilePath = getVar( s3Details.getCredentialsFilePath(), variableSpace.get() );
    String profileName = getVar( s3Details.getProfileName(), variableSpace.get() );

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
    if ( awsCredentialsProvider != null ) {
      Regions regions =
        !S3Util.isEmpty( s3Details.getRegion() ) ? Regions.fromName( s3Details.getRegion() ) : Regions.DEFAULT_REGION;
      return AmazonS3ClientBuilder.standard().withCredentials( awsCredentialsProvider )
        .enableForceGlobalBucketAccess().withRegion( regions ).build();
    }
    return null;
  }

  private String getVar( String value, VariableSpace variableSpace ) {
    if ( variableSpace != null ) {
      return variableSpace.environmentSubstitute( value );
    }
    return value;
  }
}
