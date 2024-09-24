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

import org.pentaho.big.data.kettle.plugins.hive.DummyDriver;

/**
 * DummyDriver implementation to avoid CNF exception
 * when Hive2DatabaseMeta is loaded.  See DummyDriver.
 */
public class HiveDriver extends DummyDriver {
}
