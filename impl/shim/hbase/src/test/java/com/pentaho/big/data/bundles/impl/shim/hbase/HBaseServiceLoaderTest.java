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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.eclipse.core.runtime.AssertionFailedException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/2/16.
 */
public class HBaseServiceLoaderTest {
  private BundleContext bundleContext;
  private ShimBridgingServiceTracker shimBridgingServiceTracker;
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private HBaseServiceLoader hBaseServiceLoader;
  private HadoopConfiguration hadoopConfiguration;

  @Before
  public void setup() throws ConfigurationException {
    bundleContext = mock( BundleContext.class );
    shimBridgingServiceTracker = mock( ShimBridgingServiceTracker.class );
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    hadoopConfiguration = mock( HadoopConfiguration.class );
    hBaseServiceLoader =
      new HBaseServiceLoader( bundleContext, shimBridgingServiceTracker, hadoopConfigurationBootstrap );
  }

  @Test
  public void testTwoArgConstructor() throws ConfigurationException {
    assertNotNull( new HBaseServiceLoader( bundleContext, shimBridgingServiceTracker ) );
  }

  @Test
  public void testOnClassLoaderAvailable() {
    ClassLoader classLoader = mock( ClassLoader.class );
    hBaseServiceLoader.onClassLoaderAvailable( classLoader );
    verifyNoMoreInteractions( classLoader );
  }

  @Test
  public void testOnConfigurationClose() {
    hBaseServiceLoader.onConfigurationClose( hadoopConfiguration );
    verify( shimBridgingServiceTracker ).unregister( hadoopConfiguration );
  }

  @Test
  public void testOnConfigurationOpen()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( mock( HadoopShim.class ) );
    final AtomicBoolean validated = new AtomicBoolean( false );
    final AtomicReference<AssertionFailedException> assertionFailedExceptionAtomicReference =
      new AtomicReference<>( null );
    doAnswer( new Answer() {
      @Override public Object answer( InvocationOnMock invocation ) throws Throwable {
        try {
          Object[] arguments = invocation.getArguments();
          assertEquals( hadoopConfiguration, arguments[ 0 ] );

          assertEquals( NamedClusterServiceFactory.class, arguments[ 1 ] );

          String implName = (String) arguments[ 2 ];
          assertEquals( HBaseServiceLoader.HBASE_SERVICE_FACTORY_CANONICAL_NAME, implName );

          Class<?> implClass = Class.forName( implName );
          assertNotNull( implClass );

          assertEquals( bundleContext, arguments[ 3 ] );

          Constructor<?> constructor = implClass.getConstructor( (Class<?>[]) arguments[ 5 ] );
          assertNotNull( constructor );

          assertArrayEquals( new Object[] { true, hadoopConfiguration }, (Object[]) arguments[ 6 ] );
        } catch ( AssertionFailedException e ) {
          assertionFailedExceptionAtomicReference.set( e );
        }
        validated.set( true );
        return null;
      }
    } ).when( shimBridgingServiceTracker )
      .registerWithClassloader( any(), any( Class.class ), anyString(), any( BundleContext.class ),
        any( ClassLoader.class ), any( Class[].class ), any( Object[].class ) );

    hBaseServiceLoader.onConfigurationOpen( hadoopConfiguration, true );
    AssertionFailedException assertionFailedException = assertionFailedExceptionAtomicReference.get();
    if ( assertionFailedException != null ) {
      throw assertionFailedException;
    }
    assertTrue( validated.get() );
  }

  @Test
  public void testOnConfigurationOpenFailure()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    hBaseServiceLoader.onConfigurationOpen( hadoopConfiguration, true );
    verify( shimBridgingServiceTracker, never() )
      .registerWithClassloader( any(), any( Class.class ), anyString(), any( BundleContext.class ),
        any( ClassLoader.class ), any( Class[].class ), any( Object[].class ) );
  }
}
