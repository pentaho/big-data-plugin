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

package org.pentaho.big.data.impl.shim.pig;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/1/15.
 */
public class PigServiceFactoryLoaderTest {
  private BundleContext bundleContext;
  private ShimBridgingServiceTracker shimBridgingServiceTracker;
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private PigServiceFactoryLoader pigServiceFactoryLoader;
  private HadoopConfiguration hadoopConfiguration;

  @Before
  public void setup() throws ConfigurationException {
    bundleContext = mock( BundleContext.class );
    shimBridgingServiceTracker = mock( ShimBridgingServiceTracker.class );
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    hadoopConfiguration = mock( HadoopConfiguration.class );
    pigServiceFactoryLoader =
      new PigServiceFactoryLoader( bundleContext, shimBridgingServiceTracker, hadoopConfigurationBootstrap );
  }

  @Test
  public void testTwoArgConstructor() throws ConfigurationException {
    assertNotNull( new PigServiceFactoryLoader( bundleContext, shimBridgingServiceTracker ) );
  }

  @Test
  public void testConstructorRegistersItself() throws ConfigurationException {
    verify( hadoopConfigurationBootstrap ).registerHadoopConfigurationListener( pigServiceFactoryLoader );
  }

  @Test
  public void testOnConfigurationOpenNull() {
    pigServiceFactoryLoader.onConfigurationOpen( null, false );
    verifyNoMoreInteractions( shimBridgingServiceTracker );
  }

  @Test
  public void testOnConfigurationOpenNoExceptions()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    HadoopShim hadoopShim = mock( HadoopShim.class );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( hadoopShim );
    pigServiceFactoryLoader.onConfigurationOpen( hadoopConfiguration, true );
    verify( shimBridgingServiceTracker )
      .registerWithClassloader( eq( hadoopConfiguration ), eq( NamedClusterServiceFactory.class ),
        eq( PigServiceFactoryImpl.class.getCanonicalName() ), eq( bundleContext ),
        eq( hadoopShim.getClass().getClassLoader() ),
        eq( new Class<?>[] { boolean.class, HadoopConfiguration.class } ),
        eq( new Object[] { true, hadoopConfiguration } ) );
  }

  @Test
  public void testOnConfigurationOpenException()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    pigServiceFactoryLoader.onConfigurationOpen( hadoopConfiguration, true );
    verifyNoMoreInteractions( shimBridgingServiceTracker );
  }

  @Test
  public void testOnConfigurationClose() {
    pigServiceFactoryLoader.onConfigurationClose( hadoopConfiguration );
    verify( shimBridgingServiceTracker ).unregister( hadoopConfiguration );
  }

  @Test
  public void testOnClassLoaderAvailable() {
    pigServiceFactoryLoader.onClassLoaderAvailable( getClass().getClassLoader() );
    verifyNoMoreInteractions( bundleContext, shimBridgingServiceTracker );
  }
}
