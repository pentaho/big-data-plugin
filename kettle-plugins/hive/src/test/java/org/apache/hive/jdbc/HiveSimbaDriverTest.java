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


package org.apache.hive.jdbc;

import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.hive.DummyDriver;

import static org.junit.Assert.assertTrue;

/**
 * Created by bryan on 4/14/16.
 */
public class HiveSimbaDriverTest {
  @Test
  public void testIsInstance() {
    assertTrue( DummyDriver.class.isInstance( new HiveSimbaDriver() ) );
  }
}
