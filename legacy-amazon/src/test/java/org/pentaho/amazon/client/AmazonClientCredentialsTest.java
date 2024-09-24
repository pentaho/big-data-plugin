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

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.junit.Assert;
import org.junit.Test;

public class AmazonClientCredentialsTest {

  @Test
  public void testGetAWSCredentials_getValidCredentials() {

    String expectedAccesskey = "accessKey";
    String expectedSecretKey = "secretKey";

    AmazonClientCredentials clientInitializer =
      new AmazonClientCredentials( "accessKey", "secretKey", "", "US East (N. Virginia)" );

    AWSCredentials awsCredentials = clientInitializer.getAWSCredentials();

    Assert.assertEquals( expectedAccesskey, awsCredentials.getAWSAccessKeyId() );
    Assert.assertEquals( expectedSecretKey, awsCredentials.getAWSSecretKey() );
  }

  @Test
  public void testGetRegion_getValidRegion() {

    String expectedRegion = "us-east-1";

    AmazonClientCredentials clientInitializer =
      new AmazonClientCredentials( "accessKey", "secretKey", "", "US East (N. Virginia)" );

    Assert.assertEquals( expectedRegion, clientInitializer.getRegion() );
  }

  @Test
  public void testGetRegion_getDefaultRegion() {

    String expectedRegion = "us-east-1";

    AmazonClientCredentials clientInitializer =
      new AmazonClientCredentials( "accessKey", "secretKey", "", null );

    Assert.assertEquals( expectedRegion, clientInitializer.getRegion() );
  }
}
