/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.amazon.client.impl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.pentaho.amazon.client.AmazonClientCredentials;
import org.pentaho.amazon.client.AbstractClientFactory;
import org.pentaho.amazon.client.api.S3Client;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class S3ClientFactory extends AbstractClientFactory<S3Client> {

  @Override
  public S3Client createClient( String accessKey, String secretKey, String sessionToken, String region ) {
    AmazonClientCredentials clientCredentials = new AmazonClientCredentials( accessKey, secretKey, sessionToken, region );

    AmazonS3 awsS3Client =
      AmazonS3ClientBuilder.standard().withRegion( clientCredentials.getRegion() )
        .withCredentials( new AWSStaticCredentialsProvider( clientCredentials.getAWSCredentials() ) ).build();

    S3Client s3Client = new S3ClientImpl( awsS3Client );

    return s3Client;
  }
}
