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

package org.pentaho.amazon;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
public class AmazonRegionTest {

  @Test
  public void testGetHumanReadableRegion_getValidReadableRegion() {

    String expectedReadableRegion = "US East (N. Virginia)";

    String readableRegion = AmazonRegion.US_EAST_1.getHumanReadableRegion();

    assertEquals( expectedReadableRegion, readableRegion );
  }

  @Test
  public void testExtractRegionFromDescription_getValidRegion() {

    String expectedRegion = "us-east-1";

    String region = AmazonRegion.extractRegionFromDescription( "US East (N. Virginia)" );

    Assert.assertEquals( expectedRegion, region );
  }

  @Test
  public void testExtractRegionFromDescription_getDefaultRegion() {

    String expectedRegion = "us-east-1";

    String region = AmazonRegion.extractRegionFromDescription( null );

    Assert.assertEquals( expectedRegion, region );
  }
}
