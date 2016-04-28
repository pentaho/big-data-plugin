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

import org.pentaho.big.data.api.jdbc.DriverLocator;
import org.pentaho.big.data.api.jdbc.DriverRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by bryan on 4/18/16.
 */
public class DriverRegistryImpl implements DriverRegistry, DriverLocator {
  private static final Logger logger = LoggerFactory.getLogger( DriverRegistryImpl.class );
  private final List<Driver> drivers;

  public DriverRegistryImpl() {
    this( new CopyOnWriteArrayList<>() );
  }

  public DriverRegistryImpl( List<Driver> drivers ) {
    this.drivers = drivers;
  }

  @Override public void registerDriver( Driver driver ) throws SQLException {
    drivers.add( driver );
  }

  @Override public void deregisterDriver( Driver driver ) throws SQLException {
    drivers.remove( driver );
  }


  public List<Driver> getDrivers() {
    return Collections.unmodifiableList( drivers );
  }

  @Override public Driver getDriver( String url ) {
    for ( Driver driver : drivers ) {
      try {
        if ( driver.acceptsURL( url ) ) {
          return driver;
        }
      } catch ( SQLException e ) {
        logger.error( "Unable to see if driver " + driver + " acceptsURL " + url );
      }
    }
    return null;
  }
}
