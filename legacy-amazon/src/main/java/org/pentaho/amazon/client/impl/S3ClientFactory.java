/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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
