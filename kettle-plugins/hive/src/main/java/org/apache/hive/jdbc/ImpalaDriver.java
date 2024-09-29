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

import org.pentaho.big.data.kettle.plugins.hive.DummyDriver;

/**
 * DummyDriver implementation to avoid CNF exception
 * when ImpalaDatabaseMeta is loaded.  See DummyDriver.
 */
public class ImpalaDriver extends DummyDriver {
}
