/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

import com.amazonaws.services.pricing.AWSPricing;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.amazon.client.api.PricingClient;


import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class PricingClientImplTest {

  private PricingClientImpl pricingClient;

  @Before
  public void setUp() {
    AWSPricing awsPricing = mock( AWSPricing.class );
    pricingClient = spy( new PricingClientImpl( awsPricing, "US East (N. Virginia)" ) );
  }

  @Test
  public void testPopulateInstanceTypesForSelectedRegion_withValidValues() throws Exception {

    List<String> expectedInstanceTypes = new ArrayList<>();
    expectedInstanceTypes.add( "c4.2xlarge" );
    expectedInstanceTypes.add( "c4.4xlarge" );

    List<String> productDescriptions = new ArrayList<>();

    String jsonDescriptionC2xlarge = "{" +
      "\"product\": {" +
      "\"productFamily\": \"Elastic Map Reduce Instance\"," +
      "\"attributes\": {" +
      "\"servicecode\": \"ElasticMapReduce\"," +
      "\"softwareType\": \"Hunk\"," +
      "\"instanceType\": \"c4.2xlarge\"," +
      "\"usagetype\": \"HunkBoxUsage:c4.2xlarge\"," +
      "\"locationType\": \"AWS Region\"," +
      "\"location\": \"US East (N. Virginia)\"," +
      "\"servicename\": \"Amazon Elastic MapReduce\"," +
      "\"instanceFamily\": \"Compute optimized\"," +
      "\"operation\": \"\"" +
      "}," +
      "\"sku\": \"226GQ2CAZYZ8D7MG\"" +
      "}}";

    String jsonDescriptionC4xlarge = "{" +
      "\"product\": {" +
      "\"productFamily\": \"Elastic Map Reduce Instance\"," +
      "\"attributes\": {" +
      "\"servicecode\": \"ElasticMapReduce\"," +
      "\"softwareType\": \"Hunk\"," +
      "\"instanceType\": \"c4.4xlarge\"," +
      "\"usagetype\": \"HunkBoxUsage:c4.4xlarge\"," +
      "\"locationType\": \"AWS Region\"," +
      "\"location\": \"US East (N. Virginia)\"," +
      "\"servicename\": \"Amazon Elastic MapReduce\"," +
      "\"instanceFamily\": \"Compute optimized\"," +
      "\"operation\": \"\"" +
      "}," +
      "\"sku\": \"226GQ2CAZYZ8D7MG\"" +
      "}}";

    productDescriptions.add( jsonDescriptionC2xlarge );
    productDescriptions.add( jsonDescriptionC4xlarge );

    doReturn( productDescriptions ).when( pricingClient ).getProductDescriptions();

    List<String> instanceTypes = pricingClient.populateInstanceTypesForSelectedRegion();

    Assert.assertEquals( expectedInstanceTypes, instanceTypes );
  }

  @Test
  public void testPopulateInstanceTypesForSelectedRegion_whenDescriptionIsNull() throws Exception {

    List<String> productDescriptions = null;

    doReturn( productDescriptions ).when( pricingClient ).getProductDescriptions();

    List<String> instanceTypes = pricingClient.populateInstanceTypesForSelectedRegion();

    Assert.assertNull( instanceTypes );
  }

  @Test
  public void testPopulateInstanceTypesForSelectedRegion_whenDescriptionIsEmpty() throws Exception {

    List<String> productDescriptions = new ArrayList<>();

    doReturn( productDescriptions ).when( pricingClient ).getProductDescriptions();

    List<String> instanceTypes = pricingClient.populateInstanceTypesForSelectedRegion();

    Assert.assertNull( instanceTypes );
  }
}
