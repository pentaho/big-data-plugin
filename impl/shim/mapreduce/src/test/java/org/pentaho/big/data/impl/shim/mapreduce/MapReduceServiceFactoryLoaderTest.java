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

package org.pentaho.big.data.impl.shim.mapreduce;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.mapreduce.TransformationVisitorService;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 12/8/15.
 */
public class MapReduceServiceFactoryLoaderTest {

  private BundleContext bundleContext;
  private ShimBridgingServiceTracker shimBridgingServiceTracker;
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private ExecutorService executorService;
  private MapReduceServiceFactoryLoader mapReduceServiceFactoryLoader;
  private HadoopConfiguration hadoopConfiguration;
  private List<TransformationVisitorService> visitorServices = new ArrayList<>();

  @Before
  public void setup() throws ConfigurationException {
    bundleContext = mock( BundleContext.class );
    shimBridgingServiceTracker = mock( ShimBridgingServiceTracker.class );
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    executorService = mock( ExecutorService.class );
    mapReduceServiceFactoryLoader =
      new MapReduceServiceFactoryLoader( bundleContext, shimBridgingServiceTracker, hadoopConfigurationBootstrap,
        executorService, visitorServices );
    hadoopConfiguration = mock( HadoopConfiguration.class );
  }

  @Test
  public void testSimpleConstructor() throws ConfigurationException {
    assertNotNull( new MapReduceServiceFactoryLoader( bundleContext, shimBridgingServiceTracker, executorService, visitorServices ) );
  }

  @Test
  public void testOnConfigurationOpenNull() {
    mapReduceServiceFactoryLoader.onConfigurationOpen( null, true );
    verifyNoMoreInteractions( shimBridgingServiceTracker, bundleContext );
  }

  @Test
  public void testOnConfigurationOpenSuccess()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    HadoopShim hadoopShim = mock( HadoopShim.class );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( hadoopShim );
    mapReduceServiceFactoryLoader.onConfigurationOpen( hadoopConfiguration, true );

    ArgumentCaptor<Class[]> classArgumentCaptor = ArgumentCaptor.forClass( Class[].class );
    ArgumentCaptor<Object[]> objectArgumentCaptor = ArgumentCaptor.forClass( Object[].class );

    Class<MapReduceServiceFactoryImpl> mapReduceServiceFactoryClass = MapReduceServiceFactoryImpl.class;
    verify( shimBridgingServiceTracker ).registerWithClassloader( eq( hadoopConfiguration ), eq(
      NamedClusterServiceFactory.class ), eq( mapReduceServiceFactoryClass.getCanonicalName() ),
      eq( bundleContext ), eq( hadoopShim.getClass().getClassLoader() ), classArgumentCaptor.capture(),
      objectArgumentCaptor.capture() );

    Class[] parameterTypes = classArgumentCaptor.getValue();
    Object[] objects = objectArgumentCaptor.getValue();
    assertNotNull( mapReduceServiceFactoryClass.getConstructor( parameterTypes ) );
    assertEquals( 4, parameterTypes.length );
    assertEquals( 4, objects.length );
    for ( int i = 0; i < parameterTypes.length; i++ ) {
      assertTrue( parameterTypes[ i ].isInstance( objects[ i ] ) || ( parameterTypes[ i ] == boolean.class && (
        (boolean) objects[ i ] || !(boolean) objects[ i ] ) ) );
    }
    assertEquals( true, objects[ 0 ] );
    assertEquals( hadoopConfiguration, objects[ 1 ] );
    assertEquals( executorService, objects[ 2 ] );
  }

  @Test
  public void testOnConfigurationOpenFailure()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    when( shimBridgingServiceTracker
      .registerWithClassloader( any(), any( Class.class ), any( String.class ), any( BundleContext.class ),
        any( ClassLoader.class ), any( Class[].class ), any( Object[].class ) ) ).thenThrow( new RuntimeException() );
    mapReduceServiceFactoryLoader.onConfigurationOpen( hadoopConfiguration, true );
    verifyNoMoreInteractions( bundleContext );
  }

  @Test
  public void testOnConfigurationClose() {
    mapReduceServiceFactoryLoader.onConfigurationClose( hadoopConfiguration );
    verify( shimBridgingServiceTracker ).unregister( hadoopConfiguration );
  }

  @Test
  public void testOnClassLoaderAvailable() {
    mapReduceServiceFactoryLoader.onClassLoaderAvailable( getClass().getClassLoader() );
    verifyNoMoreInteractions( shimBridgingServiceTracker );
  }
}
