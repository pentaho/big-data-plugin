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

import org.pentaho.big.data.kettle.plugins.hive.DummyDriver;

/**
 * DummyDriver implementation to avoid CNF exception
 * when HiveSimbaDatabaseMeta is loaded.  See DummyDriver.
 */
public class HiveSimbaDriver extends DummyDriver {
}
