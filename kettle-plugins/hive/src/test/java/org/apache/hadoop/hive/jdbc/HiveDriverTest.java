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

package org.apache.hadoop.hive.jdbc;

import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.hive.DummyDriver;

/**
 * Created by bryan on 4/14/16.
 */
public class HiveDriverTest {
  @Test
  public void testSubclass() {
    DummyDriver.class.isInstance( new HiveDriver() );
  }
}
