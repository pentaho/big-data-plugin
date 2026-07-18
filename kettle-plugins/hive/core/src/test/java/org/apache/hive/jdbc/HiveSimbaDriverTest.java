/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
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
