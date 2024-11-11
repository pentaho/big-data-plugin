/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.amazon.client.impl;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.pricing.AWSPricing;
import com.amazonaws.services.pricing.AWSPricingAsyncClientBuilder;
import com.amazonaws.services.s3.model.Region;
import org.pentaho.amazon.client.AmazonClientCredentials;
import org.pentaho.amazon.client.AbstractClientFactory;
import org.pentaho.amazon.client.api.PricingClient;

/**
 * Created by Aliaksandr_Zhuk on 2/5/2018.
 */
public class PricingClientFactory extends AbstractClientFactory<PricingClient> {

  @Override
  public PricingClient createClient( String accessKey, String secretKey, String sessionToken, String region ) {
    AmazonClientCredentials clientCredentials = new AmazonClientCredentials( accessKey, secretKey, sessionToken, region );

    AWSPricing awsPricingClient =
      AWSPricingAsyncClientBuilder.standard().withRegion( Region.US_Standard.toAWSRegion().getName() )
        .withCredentials( new AWSStaticCredentialsProvider( clientCredentials.getAWSCredentials() ) ).build();

    PricingClient pricingClient = new PricingClientImpl( awsPricingClient, region );

    return pricingClient;
  }
}
