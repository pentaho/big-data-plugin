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

import org.osgi.framework.ServiceReference;
import org.pentaho.di.core.database.DelegatingDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * Created by bryan on 4/27/16.
 */
public class LazyDelegatingDriver implements Driver {
  private final DriverLocatorImpl driverLocator;
  private final HasRegisterDriver hasRegisterDriver;
  private Driver delegate;
  private DelegatingDriver delegatingDriver;

  public LazyDelegatingDriver( DriverLocatorImpl driverLocator ) throws SQLException {
    this( driverLocator, DriverManager::registerDriver );
  }

  public LazyDelegatingDriver( DriverLocatorImpl driverLocator, HasRegisterDriver hasRegisterDriver )
    throws SQLException {
    this.driverLocator = driverLocator;
    this.hasRegisterDriver = hasRegisterDriver;
    this.delegatingDriver = new DelegatingDriver( this );
    hasRegisterDriver.registerDriver( delegatingDriver );
  }

  private synchronized <T> T findAndProcess( FunctionWithSQLException<Driver, T> attempt, Predicate<T> success,
                                             T defaultVal )
    throws SQLException {
    if ( delegate == null ) {
      Iterator<Map.Entry<ServiceReference<Driver>, Driver>> drivers = driverLocator.getDrivers();
      while ( drivers.hasNext() ) {
        Map.Entry<ServiceReference<Driver>, Driver> driverEntry = drivers.next();
        ServiceReference<Driver> serviceReference = driverEntry.getKey();
        Driver driver = driverEntry.getValue();
        T result = attempt.apply( driver );
        if ( success.test( result ) ) {
          delegate = driver;
          new LazyDelegatingDriver( driverLocator, hasRegisterDriver );
          driverLocator.registerDriverServiceReferencePair( serviceReference, delegatingDriver, false );
          return result;
        }
      }
    } else {
      T result = attempt.apply( delegate );
      if ( success.test( result ) ) {
        return result;
      }
    }
    return defaultVal;
  }

  private synchronized <T> T process( Function<Driver, T> function, T defaultVal ) {
    if ( delegate == null ) {
      return defaultVal;
    }
    return function.apply( delegate );
  }

  private synchronized <T> T processSQLException( FunctionWithSQLException<Driver, T> function, T defaultVal )
    throws SQLException {
    if ( delegate == null ) {
      return defaultVal;
    }
    return function.apply( delegate );
  }

  private synchronized <T> T processSQLFeatureNotSupportedException(
    FunctionWithSQLFeatureNotSupportedException<Driver, T> function, T defaultVal )
    throws SQLFeatureNotSupportedException {
    if ( delegate == null ) {
      return defaultVal;
    }
    return function.apply( delegate );
  }

  @Override public Connection connect( String url, Properties info ) throws SQLException {
    return findAndProcess( driver -> driver.connect( url, info ), Objects::nonNull, null );
  }

  @Override public boolean acceptsURL( String url ) throws SQLException {
    return findAndProcess( driver -> driver.acceptsURL( url ), bool -> bool, false );
  }

  @Override public DriverPropertyInfo[] getPropertyInfo( String url, Properties info ) throws SQLException {
    return processSQLException( driver -> driver.getPropertyInfo( url, info ), new DriverPropertyInfo[ 0 ] );
  }

  @Override public int getMajorVersion() {
    return process( driver -> driver.getMajorVersion(), 0 );
  }

  @Override public int getMinorVersion() {
    return process( driver -> driver.getMinorVersion(), 0 );
  }

  @Override public boolean jdbcCompliant() {
    return process( driver -> driver.jdbcCompliant(), false );
  }

  @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return processSQLFeatureNotSupportedException( driver -> driver.getParentLogger(), null );
  }

  private interface FunctionWithSQLException<T, R> {
    R apply( T t ) throws SQLException;
  }

  private interface FunctionWithSQLFeatureNotSupportedException<T, R> {
    R apply( T t ) throws SQLFeatureNotSupportedException;
  }
}
