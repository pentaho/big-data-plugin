/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.common;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by bryan on 7/7/15.
 */
public class ShimBridgingServiceTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger( ShimBridgingServiceTracker.class );
  private final Map<Object, ShimRef> serviceRegistrationMap = new HashMap<>();

  public boolean registerWithClassloader( Object serviceKey, Class iface, String className, BundleContext bundleContext,
                                          ClassLoader parentClassloader, Class<?>[] argTypes,
                                          Object[] args )
    throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
    InstantiationException {
    if ( serviceKey == null ) {
      LOGGER.warn( "Skipped registering " + serviceKey + " as " + iface.getCanonicalName()
        + " because it was null." );
      return false;
    }
    if ( serviceRegistrationMap.containsKey( serviceKey ) ) {
      LOGGER.warn( "Skipped registering " + serviceKey + " as " + iface.getCanonicalName()
        + " because it was already registered." );
      return false;
    }
    serviceRegistrationMap.put( serviceKey, new ShimRef( bundleContext.registerService( iface,
      Class.forName( className, true, new ShimBridgingClassloader( parentClassloader, bundleContext ) )
        .getConstructor( argTypes ).newInstance( args ), new Hashtable<String, Object>() ), iface ) );
    LOGGER.debug( "Registered " + serviceKey + " as " + iface.getCanonicalName() + " successfully!!" );
    return true;
  }

  public boolean unregister( Object serviceKey ) {
    if ( serviceKey == null ) {
      LOGGER.warn( "Skipped unregistering " + serviceKey + " because it was null." );
      return false;
    }
    ShimRef shimRef = serviceRegistrationMap.remove( serviceKey );
    if ( shimRef != null ) {
      shimRef.serviceRegistration.unregister();
      LOGGER.debug( "Unregistered " + serviceKey + " as " + shimRef.iface + " successfully!!" );
      return true;
    } else {
      LOGGER.warn( "Skipped unregistering " + serviceKey + " because it was already registered." );
      return false;
    }
  }

  private static final class ShimRef {
    private final ServiceRegistration serviceRegistration;
    private final Class<?> iface;

    private ShimRef( ServiceRegistration reference, Class<?> iface ) {
      this.serviceRegistration = reference;
      this.iface = iface;
    }
  }
}
