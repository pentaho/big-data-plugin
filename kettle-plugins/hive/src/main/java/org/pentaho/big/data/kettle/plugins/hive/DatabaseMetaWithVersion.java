/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

import org.pentaho.big.data.api.jdbc.DriverLocator;
import org.pentaho.di.core.database.BaseDatabaseMeta;

import java.sql.Driver;

/**
 * Created by bryan on 4/14/16.
 */
public abstract class DatabaseMetaWithVersion extends BaseDatabaseMeta {
  private final DriverLocator driverLocator;

  protected DatabaseMetaWithVersion( DriverLocator driverLocator ) {
    this.driverLocator = driverLocator;
  }

  @Override public abstract String getURL( String hostname, String port, String databaseName );

  /**
   * Check that the version of the driver being used is at least the driver you want. If you do not care about the minor
   * version, pass in a 0 (The assumption being that the minor version will ALWAYS be 0 or greater)
   *
   * @return true: the version being used is equal to or newer than the one you requested false: the version being used
   * is older than the one you requested
   */
  protected boolean isDriverVersion( int majorVersion, int minorVersion ) {
    String url = getURL( "localhost", "10000", "default" );
    Driver driver = driverLocator.getDriver( url );
    int driverMajorVersion = driver.getMajorVersion();
    return driverMajorVersion > majorVersion || ( driverMajorVersion == majorVersion
      && driver.getMinorVersion() >= minorVersion );
  }
}
