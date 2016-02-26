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

package org.pentaho.big.data.impl.shim.sqoop;

import com.google.common.primitives.Primitives;
import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/26/16.
 */
public class SqoopServiceFactoryLoaderTest {
  private BundleContext bundleContext;
  private ShimBridgingServiceTracker shimBridgingServiceTracker;
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private SqoopServiceFactoryLoader sqoopServiceFactoryLoader;

  @Before
  public void setup() throws ConfigurationException {
    bundleContext = mock( BundleContext.class );
    shimBridgingServiceTracker = mock( ShimBridgingServiceTracker.class );
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    sqoopServiceFactoryLoader =
      new SqoopServiceFactoryLoader( bundleContext, shimBridgingServiceTracker, hadoopConfigurationBootstrap );
    verify( hadoopConfigurationBootstrap ).registerHadoopConfigurationListener( sqoopServiceFactoryLoader );
  }

  @Test
  public void testTwoArgConstructor() throws ConfigurationException {
    assertNotNull( new SqoopServiceFactoryLoader( bundleContext, shimBridgingServiceTracker ) );
  }

  @Test
  public void testOnConfigurationOpenNull() {
    sqoopServiceFactoryLoader.onConfigurationOpen( null, false );
    verifyNoMoreInteractions( shimBridgingServiceTracker );
  }

  @Test
  public void testOnConfigurationOpenSuccess()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    HadoopConfiguration hadoopConfiguration = mock( HadoopConfiguration.class );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( mock( HadoopShim.class ) );
    ClassLoader classLoader = hadoopConfiguration.getHadoopShim().getClass().getClassLoader();
    ArgumentCaptor<Class[]> clazzArgumentCaptor = ArgumentCaptor.forClass( Class[].class );
    ArgumentCaptor<Object[]> argsArgumentCaptor = ArgumentCaptor.forClass( Object[].class );

    sqoopServiceFactoryLoader.onConfigurationOpen( hadoopConfiguration, true );
    verify( shimBridgingServiceTracker ).registerWithClassloader( eq( hadoopConfiguration ), eq(
      NamedClusterServiceFactory.class ), eq( SqoopServiceFactoryImpl.class.getCanonicalName() ), eq( bundleContext ),
      eq( classLoader ), clazzArgumentCaptor.capture(), argsArgumentCaptor.capture() );

    Class[] parameterTypes = clazzArgumentCaptor.getValue();
    Constructor<SqoopServiceFactoryImpl> constructor =
      SqoopServiceFactoryImpl.class.getConstructor( parameterTypes );
    assertNotNull( constructor );
    Object[] objects = argsArgumentCaptor.getValue();
    assertEquals( parameterTypes.length, objects.length );
    for ( int i = 0; i < objects.length; i++ ) {
      assertTrue( Primitives.wrap( parameterTypes[ i ] ).isInstance( objects[ i ] ) );
    }
  }

  @Test
  public void testOnConfigurationFailure() {
    sqoopServiceFactoryLoader.onConfigurationOpen( mock( HadoopConfiguration.class ), false );
    verifyNoMoreInteractions( shimBridgingServiceTracker );
  }

  @Test
  public void testOnConfigurationClose() {
    HadoopConfiguration hadoopConfiguration = mock( HadoopConfiguration.class );
    sqoopServiceFactoryLoader.onConfigurationClose( hadoopConfiguration );
    verify( shimBridgingServiceTracker ).unregister( hadoopConfiguration );
  }

  @Test
  public void testOnClassLoaderAvailable() {
    sqoopServiceFactoryLoader.onClassLoaderAvailable( getClass().getClassLoader() );
    verifyNoMoreInteractions( shimBridgingServiceTracker, hadoopConfigurationBootstrap );
  }
}
