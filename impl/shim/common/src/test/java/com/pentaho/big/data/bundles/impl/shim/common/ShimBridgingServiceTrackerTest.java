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

import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.framework.wiring.BundleWiring;

import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/5/15.
 */
public class ShimBridgingServiceTrackerTest {
  private ShimBridgingServiceTracker shimBridgingServiceTracker;
  private BundleContext bundleContext;
  private BundleWiring bundleWiring;
  private Bundle bundle;
  private ShimBridgingClassloader.PublicLoadResolveClassLoader bundleWiringClassloader;

  @Before
  public void setup() {
    shimBridgingServiceTracker = new ShimBridgingServiceTracker();
    bundleContext = mock( BundleContext.class );
    bundle = mock( Bundle.class );
    bundleWiring = mock( BundleWiring.class );
    bundleWiringClassloader = mock( ShimBridgingClassloader.PublicLoadResolveClassLoader.class );
    when( bundleWiring.getClassLoader() ).thenReturn( bundleWiringClassloader );
    when( bundleContext.getBundle() ).thenReturn( bundle );
    when( bundle.adapt( BundleWiring.class ) ).thenReturn( bundleWiring );
  }

  @Test
  public void testRegisterNullServiceKey()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    assertFalse(
      shimBridgingServiceTracker.registerWithClassloader( null, Object.class, null, null, null, null, null ) );
  }

  @Test
  public void testRegisterTwice()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    String key = "testKey";
    String className = Object.class.getCanonicalName();
    ClassLoader parentClassloader = getClass().getClassLoader();
    Class[] argTypes = {};
    Object[] objects = {};
    assertTrue( shimBridgingServiceTracker
      .registerWithClassloader( key, Object.class, className, bundleContext, parentClassloader, argTypes, objects ) );
    assertFalse( shimBridgingServiceTracker
      .registerWithClassloader( key, Object.class, className, bundleContext, parentClassloader, argTypes, objects ) );
  }

  @Test
  public void testUnregisterNullServiceKey() {
    assertFalse( shimBridgingServiceTracker.unregister( null ) );
  }

  @Test
  public void testUnregisterNotRegisteredServiceKey() {
    assertFalse( shimBridgingServiceTracker.unregister( "testKey" ) );
  }

  @Test
  public void testRegisterUnregister()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    String key = "testKey";
    String className = Object.class.getCanonicalName();
    ClassLoader parentClassloader = getClass().getClassLoader();
    Class[] argTypes = {};
    Object[] objects = {};
    ServiceRegistration serviceRegistration = mock( ServiceRegistration.class );
    when(
      bundleContext.registerService( eq( Object.class ), any( Object.class ), eq( new Hashtable<String, Object>() ) ) )
      .thenReturn( serviceRegistration );
    assertTrue( shimBridgingServiceTracker
      .registerWithClassloader( key, Object.class, className, bundleContext, parentClassloader, argTypes, objects ) );
    assertTrue( shimBridgingServiceTracker.unregister( key ) );
    verify( serviceRegistration ).unregister();
  }
}
