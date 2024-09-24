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

package org.pentaho.big.data.kettle.plugins.hive;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * DummyDriver is a bare Driver implementation used as a way
 * to avoid ClassNotFoundException when kettle attempts to load
 * the class associated with each meta.
 *
 * The classes which extend DummyDriver have the same unique
 * names as the name exposed by .getDriverClass() in the
 * DatabaseMeta implementation.
 *
 * Created by bryan on 3/30/16.
 */
public class DummyDriver implements Driver {
  @Override public Connection connect( String url, Properties info ) throws SQLException {
    return null;
  }

  @Override public boolean acceptsURL( String url ) throws SQLException {
    return false;
  }

  @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return new DriverPropertyInfo[ 0 ];
  }

  @Override public int getMajorVersion() {
    return 0;
  }

  @Override public int getMinorVersion() {
    return 0;
  }

  @Override public boolean jdbcCompliant() {
    return false;
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
