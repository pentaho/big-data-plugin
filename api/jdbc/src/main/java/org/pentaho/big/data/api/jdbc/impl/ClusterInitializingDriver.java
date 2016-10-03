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

package org.pentaho.big.data.api.jdbc.impl;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;
import org.pentaho.di.core.database.DelegatingDriver;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by bryan on 4/27/16.
 */
public class ClusterInitializingDriver implements Driver {

  private static final List<String> BIG_DATA_DRIVER_URL_PATTERNS = new ArrayList<>();

  @VisibleForTesting
  protected static org.slf4j.Logger logger = LoggerFactory.getLogger( ClusterInitializingDriver.class );

  private final ClusterInitializer clusterInitializer;
  private final JdbcUrlParser jdbcUrlParser;

  static {
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:hive:.*" );
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:hive2:.*" );
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:impala:.*" );
    BIG_DATA_DRIVER_URL_PATTERNS.add( ".+:spark:.*" );
  }

  public ClusterInitializingDriver( ClusterInitializer clusterInitializer, JdbcUrlParser jdbcUrlParser,
      DriverLocatorImpl driverRegistry ) {
    this( clusterInitializer, jdbcUrlParser, driverRegistry, null );
  }

  public ClusterInitializingDriver( ClusterInitializer clusterInitializer, JdbcUrlParser jdbcUrlParser,
      DriverLocatorImpl driverRegistry, Integer numLazyProxies ) {
    this( clusterInitializer, jdbcUrlParser, driverRegistry, numLazyProxies, DriverManager::registerDriver );
  }

  public ClusterInitializingDriver( ClusterInitializer clusterInitializer, JdbcUrlParser jdbcUrlParser,
      DriverLocatorImpl driverRegistry, Integer numLazyProxies, HasRegisterDriver hasRegisterDriver ) {
    this.clusterInitializer = clusterInitializer;
    this.jdbcUrlParser = jdbcUrlParser;
    int lazyProxies = Optional.ofNullable( numLazyProxies ).orElse( 5 );
    try {
      hasRegisterDriver.registerDriver( new DelegatingDriver( this ) );
    } catch ( SQLException e ) {
      logger.warn( "Unable to register cluster initializing driver", e );
    }
    for ( int i = 0; i < lazyProxies; i++ ) {
      try {
        new LazyDelegatingDriver( driverRegistry, hasRegisterDriver );
      } catch ( SQLException e ) {
        logger.warn( "Failed to register " + LazyDelegatingDriver.class.getName(), e );
      }
    }
  }

  @Override
  public Connection connect( String url, Properties info ) throws SQLException {
    if ( checkIfUsesBigDataDriver( url ) ) {
      initializeCluster( url );
    }
    return null;
  }

  @Override
  public boolean acceptsURL( String url ) throws SQLException {
    if ( checkIfUsesBigDataDriver( url ) ) {
      initializeCluster( url );
    }
    return false;
  }

  boolean checkIfUsesBigDataDriver( String url ) {
    List<String> urlPatterns = getUrlPatternsForBigDataDrivers();
    for ( String pattern : urlPatterns ) {
      if ( url.matches( pattern ) ) {
        return true;
      }
    }
    return false;
  }

  List<String> getUrlPatternsForBigDataDrivers() {
    return BIG_DATA_DRIVER_URL_PATTERNS;
  }

  private void initializeCluster( String url ) {
    try {
      // Initialize with a null namedCluster, since jdbc connections are not
      // associated with namedClusters.
      // Formerly this method used the following to determine cluster:
      // jdbcUrlParser.parse( url ).getNamedCluster() );
      // But this had the potential to create a block. BACKLOG-10983
      clusterInitializer.initialize( null );
    } catch ( Exception e ) {
      // Don't want to depend on legacy, so can't directly
      // check for NoShimSpecifiedException
      if ( e.getCause() != null && e.getCause().getClass().getName().contains( "NoShimSpecifiedException" ) ) {
        logger.debug( "No shim specified", e );
      } else {
        logger.error( "Failed to initialize cluster", e );
      }
    }
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
