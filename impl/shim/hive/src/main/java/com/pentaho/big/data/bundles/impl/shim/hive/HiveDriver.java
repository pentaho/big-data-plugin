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

package com.pentaho.big.data.bundles.impl.shim.hive;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.jdbc.JdbcUrl;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by bryan on 3/29/16.
 */
public class HiveDriver implements Driver {
  protected static final String SIMBA_SPECIFIC_URL_PARAMETER = "AuthMech=";
  /**
   * SQL State "feature not supported" with no subclass specified
   */
  public static final String SQL_STATE_NOT_SUPPORTED = "0A000";
  protected final Driver delegate;
  private final boolean defaultConfiguration;
  private final JdbcUrlParser jdbcUrlParser;
  private final String hadoopConfigurationId;

  public HiveDriver( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                     JdbcUrlParser jdbcUrlParser ) {
    this.delegate = delegate;
    this.hadoopConfigurationId = hadoopConfigurationId;
    this.defaultConfiguration = defaultConfiguration;
    this.jdbcUrlParser = jdbcUrlParser;
  }

  @Override public Connection connect( String url, Properties info ) throws SQLException {
    Driver driver = checkBeforeCallActiveDriver( url );
    JdbcUrl jdbcUrl;
    try {
      jdbcUrl = jdbcUrlParser.parse( url );
    } catch ( URISyntaxException e1 ) {
      throw new SQLException( "Unable to parse jdbc url: " + url, e1 );
    }
    NamedCluster namedCluster;
    try {
      namedCluster = jdbcUrl.getNamedCluster();
    } catch ( Exception e ) {
      return null;
    }
    if ( !acceptsURL( url, driver, namedCluster ) ) {
      return null;
    }
    try {
      return doConnect( driver, jdbcUrl, info );
    } catch ( Exception ex ) {
      Throwable cause = ex;
      do {
        // BACKLOG-6547
        if ( cause instanceof SQLException
          && SQL_STATE_NOT_SUPPORTED.equals( ( (SQLException) cause ).getSQLState() ) ) {
          // this means that either driver can't be obtained or does not support connect().
          // In both cases signal to DriverManager we can't process the URL
          return null;
        }
        cause = cause.getCause();
      } while ( cause != null );

      throw ex;
    }
  }

  public Connection doConnect( Driver driver, JdbcUrl url, Properties info ) throws SQLException {
    return driver.connect( url.toString(), info );
  }

  @Override public final boolean acceptsURL( String url ) throws SQLException {
    try {
      return acceptsURL( url, checkBeforeCallActiveDriver( url ), null );
    } catch ( Exception e ) {
      return false;
    }
  }

  private final boolean acceptsURL( String url, Driver driver, NamedCluster namedCluster ) throws SQLException {

    if ( !defaultConfiguration ) {
      return false;
    }

    if ( driver == null ) {
      return false;
    }
    try {
      return driver.acceptsURL( url );
    } catch ( Throwable e ) {
      // This should not have happened. If there was an error during processing, assume this driver can't
      // handle the URL and thus return false
      return false;
    }
  }

  protected Driver checkBeforeCallActiveDriver( String url ) throws SQLException {
    if ( url.contains( SIMBA_SPECIFIC_URL_PARAMETER ) ) {
      // BAD-215 check required to distinguish Simba driver
      return null;
    }
    return delegate;
  }

  @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    Driver delegate = this.delegate;
    if ( delegate == null ) {
      return null;
    }
    return delegate.getPropertyInfo( url, info );
  }

  @Override public int getMajorVersion() {
    Driver delegate = this.delegate;
    if ( delegate == null ) {
      return -1;
    }
    return delegate.getMajorVersion();
  }

  @Override public int getMinorVersion() {
    Driver delegate = this.delegate;
    if ( delegate == null ) {
      return -1;
    }
    return delegate.getMinorVersion();
  }

  @Override public boolean jdbcCompliant() {
    Driver delegate = this.delegate;
    if ( delegate == null ) {
      return false;
    }
    try {
      return delegate.jdbcCompliant();
    } catch ( Throwable e ) {
      return false;
    }
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    Driver delegate = this.delegate;
    if ( delegate == null ) {
      return null;
    }
    try {
      return delegate.getParentLogger();
    } catch ( Throwable e ) {
      if ( e instanceof SQLFeatureNotSupportedException ) {
        throw e;
      } else {
        throw new SQLFeatureNotSupportedException( e );
      }
    }
  }
}
