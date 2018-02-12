/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.junit.Assert;
import org.junit.Test;

public class AbstractAmazonClientTest {

  @Test public void testInitClientParameters_getValidRegion() {

    String expectedRegion = "us-east-1";
    String expectedHumanReadableRegion = "US East ( N. Virginia)";

    AbstractAmazonClient.initClientParameters( "accessKey", "secretKey", "US East ( N. Virginia)" );

    Assert.assertEquals( expectedRegion, AbstractAmazonClient.getRegion() );
    Assert.assertEquals( expectedHumanReadableRegion, AbstractAmazonClient.getHumanReadableRegion() );
  }

  @Test( expected = NullPointerException.class )
  public void testInitClientParameters_getValidRegion1() {
    AbstractAmazonClient.initClientParameters( "accessKey", "secretKey", null );
  }
}
