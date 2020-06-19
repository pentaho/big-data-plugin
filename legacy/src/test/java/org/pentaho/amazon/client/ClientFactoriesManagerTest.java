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

package org.pentaho.amazon.client;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.amazon.client.api.AimClient;
import org.pentaho.amazon.client.api.EmrClient;
import org.pentaho.amazon.client.api.PricingClient;
import org.pentaho.amazon.client.api.S3Client;
import org.pentaho.di.core.util.Assert;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class ClientFactoriesManagerTest {

  private ClientFactoriesManager factoriesManager;

  @Before
  public void setUp() {
    factoriesManager = Mockito.spy( ClientFactoriesManager.getInstance() );
  }

  @Test
  public void createClient_whenClientTypeIsS3() {

    S3Client s3Client;

    s3Client = factoriesManager.createClient( "accessKey", "secretKey", "", "US East ( N. Virginia)", ClientType.S3 );

    Assert.assertNotNull( s3Client );
  }

  @Test
  public void createClient_whenClientTypeIsEmr() {

    EmrClient emrClient;

    emrClient = factoriesManager.createClient( "accessKey", "secretKey", "", "US East ( N. Virginia)", ClientType.EMR );

    Assert.assertNotNull( emrClient );
  }

  @Test
  public void createClient_whenClientTypeIsPricing() {

    PricingClient pricingClient;

    pricingClient =
      factoriesManager.createClient( "accessKey", "secretKey", "", "US East ( N. Virginia)", ClientType.PRICING );

    Assert.assertNotNull( pricingClient );
  }

  @Test
  public void createClient_whenClientTypeIsAim() {

    AimClient aimClient;

    aimClient = factoriesManager.createClient( "accessKey", "secretKey", "", "US East ( N. Virginia)", ClientType.AIM );

    Assert.assertNotNull( aimClient );
  }
}
