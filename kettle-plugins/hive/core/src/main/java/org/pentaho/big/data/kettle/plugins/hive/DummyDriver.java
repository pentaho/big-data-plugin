/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
