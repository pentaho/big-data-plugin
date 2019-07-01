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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.amazon.s3.S3Details;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.vfs.BaseVFSConnectionProvider;
import org.pentaho.di.connections.vfs.VFSRoot;
import org.pentaho.s3common.S3CommonFileSystemConfigBuilder;
import org.pentaho.s3n.vfs.S3NFileProvider;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

/**
 * Created by bmorrise on 2/5/19.
 */
public class S3Provider extends BaseVFSConnectionProvider<S3Details> {

  public static final String NAME = "Amazon S3";
  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;

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
    AmazonS3 amazonS3 = getAmazonS3( s3Details );
    try {
      amazonS3.getS3AccountOwner();
      return true;
    } catch ( Exception e ) {
      return false;
    }
  }

  @Override public S3Details prepare( S3Details s3Details ) {
    return s3Details;
  }

  private AmazonS3 getAmazonS3( S3Details s3Details ) {
    BasicAWSCredentials awsCredentials = new BasicAWSCredentials( s3Details.getAccessKey(), s3Details.getSecretKey() );
    return AmazonS3ClientBuilder.standard().withCredentials( new AWSStaticCredentialsProvider( awsCredentials ) )
      .enableForceGlobalBucketAccess().withRegion( Regions.DEFAULT_REGION ).build();
  }
}
