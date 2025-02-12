/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hive;

import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.pentaho.di.core.database.BaseDatabaseMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.platform.api.data.DBDatasourceServiceException;
import org.pentaho.platform.api.data.IDBDatasourceService;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by bryan on 4/14/16.
 */
public abstract class DatabaseMetaWithVersion extends BaseDatabaseMeta {
  private static final Logger logger = LoggerFactory.getLogger( DatabaseMetaWithVersion.class );
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
    int driverMajorVersion;
    int driverMinorVersion;

    // If it is a JNDI connection
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_JNDI ) {
      IDBDatasourceService dss = PentahoSystem.get( IDBDatasourceService.class );

      DataSource dataSource = null;
      try {
        dataSource = dss.getDataSource( this.getDatabaseName() );
      } catch ( DBDatasourceServiceException e ) {
        logger.error( e.getMessage(), e );
      }

      DatabaseMetaData meta = null;

      try ( Connection connection = dataSource.getConnection() ) {
        meta = connection.getMetaData();
      } catch ( SQLException e ) {
        logger.error( e.getMessage(), e );
      }

      driverMajorVersion = meta.getDriverMajorVersion();
      driverMinorVersion = meta.getDriverMinorVersion();
    // if it is a JDBC or ODBC connection
    } else {
      String url = getURL( "localhost", "10000", "default" );

      Driver driver = driverLocator.getDriver( url );
      driverMajorVersion = driver.getMajorVersion();
      driverMinorVersion = driver.getMinorVersion();
    }

    return driverMajorVersion > majorVersion || ( driverMajorVersion == majorVersion
      && driverMinorVersion >= minorVersion );
  }
}
