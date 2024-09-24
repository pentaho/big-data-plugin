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

package org.pentaho.amazon.client;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
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
