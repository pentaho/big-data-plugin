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
