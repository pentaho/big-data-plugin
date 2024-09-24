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

package org.pentaho.di.core.hadoop;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 8/14/15.
 */
public class HadoopConfigurationInfoTest {
  private String id;
  private String name;
  private boolean isActive;
  private boolean willBeActiveAfterRestart;
  private HadoopConfigurationInfo hadoopConfigurationInfo;

  @Before
  public void setup() {
    id = "testId";
    name = "testName";
    isActive = true;
    willBeActiveAfterRestart = true;
    createHadoopConfigurationInfo();
  }

  private void createHadoopConfigurationInfo() {
    hadoopConfigurationInfo = new HadoopConfigurationInfo( id, name, isActive, willBeActiveAfterRestart );
  }

  @Test
  public void testGetId() {
    assertEquals( id, hadoopConfigurationInfo.getId() );
  }

  @Test
  public void testGetName() {
    assertEquals( name, hadoopConfigurationInfo.getName() );
  }

  @Test
  public void testIsActive() {
    assertTrue( hadoopConfigurationInfo.isActive() );
    isActive = false;
    createHadoopConfigurationInfo();
    assertFalse( hadoopConfigurationInfo.isActive() );
  }

  @Test
  public void testWillBeActiveAfterRestart() {
    assertTrue( hadoopConfigurationInfo.isWillBeActiveAfterRestart() );
    willBeActiveAfterRestart = false;
    createHadoopConfigurationInfo();
    assertFalse( hadoopConfigurationInfo.isWillBeActiveAfterRestart() );
  }
}
