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
