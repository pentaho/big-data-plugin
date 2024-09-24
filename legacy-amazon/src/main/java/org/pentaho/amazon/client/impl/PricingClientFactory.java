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
