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


package org.pentaho.hadoop;

import static org.junit.Assert.*;

import org.junit.Test;

public class PluginPropertiesUtilTest {

  @Test
  public void getVersion() {
    // This test will only success if using classes produced by the ant build
    PluginPropertiesUtil util = new PluginPropertiesUtil();
    assertNotNull(
      "Should never be null",
      util.getVersion() );
  }

  @Test
  public void testGetVersionFromNonDefaultLocation() {
    PluginPropertiesUtil ppu = new PluginPropertiesUtil( "test-version.properties" );
    String version = ppu.getVersion();
    assertEquals( "X.Y.Z-TEST", version );
  }

  @Test
  public void testGetVersionFromNonExistingLocation() {
    PluginPropertiesUtil ppu = new PluginPropertiesUtil( "non-existing-version.properties" );
    String version = ppu.getVersion();
    assertEquals( "@VERSION@", version );
  }

}
