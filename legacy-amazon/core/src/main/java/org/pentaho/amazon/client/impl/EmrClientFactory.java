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
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import org.pentaho.amazon.client.AmazonClientCredentials;
import org.pentaho.amazon.client.AbstractClientFactory;
import org.pentaho.amazon.client.api.EmrClient;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class EmrClientFactory extends AbstractClientFactory<EmrClient> {

  @Override
  public EmrClient createClient( String accessKey, String secretKey, String sessionToken, String region ) {
    AmazonClientCredentials clientCredentials = new AmazonClientCredentials( accessKey, secretKey, sessionToken, region );

    AmazonElasticMapReduce awsEmrClient =
      AmazonElasticMapReduceClientBuilder.standard().withRegion( clientCredentials.getRegion() )
        .withCredentials( new AWSStaticCredentialsProvider( clientCredentials.getAWSCredentials() ) )
        .build();

    EmrClient emrClient = new EmrClientImpl( awsEmrClient );

    return emrClient;
  }
}
