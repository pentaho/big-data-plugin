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

import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.pentaho.big.data.api.jdbc.DriverLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by bryan on 4/18/16.
 */
public class DriverLocatorImpl implements DriverLocator {
  private static final Logger logger = LoggerFactory.getLogger( DriverLocatorImpl.class );
  public static final String DATA_SOURCE_TYPE_BIGDATA = "(dataSourceType=bigdata)";
  private final BundleContext bundleContext;
  private final HasRegisterDriver hasRegisterDriver;
  private final Map<ServiceReference<Driver>, List<Driver>> registeredDrivers;

  public DriverLocatorImpl( BundleContext bundleContext ) {
    this( bundleContext, DriverManager::registerDriver, DriverManager::deregisterDriver, new HashMap<>() );
  }

  public DriverLocatorImpl( BundleContext bundleContext, HasRegisterDriver hasRegisterDriver,
                            HasDeregisterDriver hasDeregisterDriver,
                            Map<ServiceReference<Driver>, List<Driver>> registeredDrivers ) {
    this.bundleContext = bundleContext;
    this.hasRegisterDriver = hasRegisterDriver;
    this.registeredDrivers = registeredDrivers;
    this.bundleContext.addServiceListener( event -> {
      ServiceReference<?> serviceReference = event.getServiceReference();
      if ( serviceReference != null ) {
        List<Driver> drivers = registeredDrivers.remove( serviceReference );
        if ( drivers != null ) {
          for ( Driver driver : drivers ) {
            try {
              hasDeregisterDriver.deregisterDriver( driver );
            } catch ( SQLException e ) {
              logger.error( "Unable to deregister driver " + driver, e );
            }
          }
        }
      }
    } );
  }

  public Iterator<Map.Entry<ServiceReference<Driver>, Driver>> getDrivers() {
    try {
      return bundleContext.getServiceReferences( Driver.class, DATA_SOURCE_TYPE_BIGDATA ).stream()
        .<Map.Entry<ServiceReference<Driver>, Driver>>map(
          driverServiceReference -> new Map.Entry<ServiceReference<Driver>, Driver>() {
            @Override public ServiceReference<Driver> getKey() {
              return driverServiceReference;
            }

            @Override public Driver getValue() {
              return bundleContext.getService( driverServiceReference );
            }

            @Override public Driver setValue( Driver value ) {
              throw new UnsupportedOperationException();
            }
          } ).iterator();
    } catch ( InvalidSyntaxException e ) {
      // Shouldn't happen with null filter
      logger.error( e.getFilter(), e );
      return Collections.<Map.Entry<ServiceReference<Driver>, Driver>>emptyList().iterator();
    }
  }

  @Override public Driver getDriver( String url ) {
    Iterator<Map.Entry<ServiceReference<Driver>, Driver>> drivers = getDrivers();
    while ( drivers.hasNext() ) {
      Driver driver = drivers.next().getValue();
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

  public synchronized void registerDriverServiceReferencePair( ServiceReference<Driver> serviceReference, Driver driver,
                                                               boolean shouldRegisterExternally ) {
    try {
      if ( shouldRegisterExternally ) {
        hasRegisterDriver.registerDriver( driver );
      }
      registeredDrivers.compute( serviceReference, ( serviceReference1, drivers ) -> {
        if ( drivers == null ) {
          return Collections.singletonList( driver );
        } else {
          List<Driver> result = new ArrayList<>( drivers );
          result.add( driver );
          return Collections.unmodifiableList( result );
        }
      } );
    } catch ( SQLException e ) {
      logger.error( "Unable to register driver " + driver, e );
    }
  }
}
