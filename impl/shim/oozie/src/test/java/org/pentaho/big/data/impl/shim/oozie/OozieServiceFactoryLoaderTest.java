/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.oozie;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.InvocationTargetException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

@RunWith( MockitoJUnitRunner.class )
public class OozieServiceFactoryLoaderTest {

  @Mock private BundleContext bundleContext;
  @Mock private ShimBridgingServiceTracker shimBridgingServiceTracker;
  @Mock private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  @Mock private HadoopConfiguration hadoopConfiguration;
  @Mock private HadoopShim shim;

  @InjectMocks private OozieServiceFactoryLoader oozieServiceFactoryLoader;

  @Before
  public void before() {

  }

  @After
  public void after() {

  }

  @Test
  public void testOnConfigurationOpen() throws Exception {
    verify( hadoopConfigurationBootstrap )
      .registerHadoopConfigurationListener( oozieServiceFactoryLoader );

    when( hadoopConfiguration.getHadoopShim() )
      .thenReturn( shim );
    oozieServiceFactoryLoader
      .onConfigurationOpen( hadoopConfiguration, true );
    verifyRegisterWithClassloader();
  }

  @Test
  public void testOnConfigurationOpenNullConfig() throws Exception {
    verify( hadoopConfigurationBootstrap )
      .registerHadoopConfigurationListener( oozieServiceFactoryLoader );
    oozieServiceFactoryLoader
      .onConfigurationOpen( null, true );
    verifyNoMoreInteractions( shimBridgingServiceTracker );
  }

  @Test
  public void testOnConfigurationTrackerThrows() throws Exception {
    verify( hadoopConfigurationBootstrap )
      .registerHadoopConfigurationListener( oozieServiceFactoryLoader );
    RuntimeException ex = mock( RuntimeException.class );
    doThrow( ex ).when( shimBridgingServiceTracker )
      .registerWithClassloader( hadoopConfiguration,
        NamedClusterServiceFactory.class,
        OozieServiceFactoryImpl.class.getCanonicalName(),
        bundleContext,
        shim.getClass().getClassLoader(),
        new Class<?>[] { boolean.class, HadoopConfiguration.class },
        new Object[] { true, hadoopConfiguration }  );
    oozieServiceFactoryLoader
      .onConfigurationOpen( hadoopConfiguration, true );
  }

  private void verifyRegisterWithClassloader()
    throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
    InstantiationException {
    verify( shimBridgingServiceTracker )
      .registerWithClassloader( hadoopConfiguration,
        NamedClusterServiceFactory.class,
        OozieServiceFactoryImpl.class.getCanonicalName(),
        bundleContext,
        shim.getClass().getClassLoader(),
        new Class<?>[] { boolean.class, HadoopConfiguration.class },
        new Object[] { true, hadoopConfiguration } );
  }

  @Test
  public void testOnConfigurationClose() throws Exception {
    oozieServiceFactoryLoader.onConfigurationClose( hadoopConfiguration );
    verify( shimBridgingServiceTracker )
      .unregister( hadoopConfiguration );
  }

  @Test
  public void test2ArgConstructor()
    throws ConfigurationException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
    InstantiationException, IllegalAccessException {
    final HadoopConfigurationBootstrap bootstrap =
      mock( HadoopConfigurationBootstrap.class );
    // swap the HCB.instance with a mock
    class HadoopConfigBootstrapTester extends HadoopConfigurationBootstrap {
      { setInstance( bootstrap ); }
    }
    new HadoopConfigBootstrapTester();
    OozieServiceFactoryLoader loader = new OozieServiceFactoryLoader( bundleContext, shimBridgingServiceTracker );
    verify( bootstrap ).registerHadoopConfigurationListener( loader );
  }

  @Test
  public void testOnClassLoaderAvailable() {
    oozieServiceFactoryLoader.onClassLoaderAvailable( getClass().getClassLoader() );
    verifyNoMoreInteractions( bundleContext, shimBridgingServiceTracker );
  }
}
