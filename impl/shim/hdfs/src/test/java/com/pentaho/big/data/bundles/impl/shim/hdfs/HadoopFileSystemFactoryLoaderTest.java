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

package com.pentaho.big.data.bundles.impl.shim.hdfs;

import com.pentaho.big.data.bundles.impl.shim.common.ShimBridgingServiceTracker;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.osgi.framework.BundleContext;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.lang.reflect.InvocationTargetException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/3/15.
 */
public class HadoopFileSystemFactoryLoaderTest {
  private BundleContext bundleContext;
  private ShimBridgingServiceTracker shimBridgingServiceTracker;
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private HadoopFileSystemFactoryLoader hadoopFileSystemFactoryLoader;
  private HadoopConfiguration hadoopConfiguration;
  private HadoopShim hadoopShim;

  @Before
  public void setup() throws ConfigurationException {
    bundleContext = mock( BundleContext.class );
    shimBridgingServiceTracker = mock( ShimBridgingServiceTracker.class );
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    hadoopConfiguration = mock( HadoopConfiguration.class );
    hadoopShim = mock( HadoopShim.class );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( hadoopShim );
    hadoopFileSystemFactoryLoader =
      new HadoopFileSystemFactoryLoader( bundleContext, shimBridgingServiceTracker, hadoopConfigurationBootstrap );
    verify( hadoopConfigurationBootstrap ).registerHadoopConfigurationListener( hadoopFileSystemFactoryLoader );
  }

  @Test
  public void testOnConfigurationOpenDefault()
    throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
    IllegalAccessException {
    hadoopFileSystemFactoryLoader.onConfigurationOpen( hadoopConfiguration, true );
    ArgumentCaptor<Class[]> classCaptor = ArgumentCaptor.forClass( Class[].class );
    ArgumentCaptor<Object[]> objectCaptor = ArgumentCaptor.forClass( Object[].class );
    verify( shimBridgingServiceTracker )
      .registerWithClassloader( eq( hadoopConfiguration ), eq( HadoopFileSystemFactory.class ),
        eq( HadoopFileSystemFactoryLoader.HADOOP_FILESYSTEM_FACTORY_IMPL_CANONICAL_NAME ), eq( bundleContext ),
        eq( hadoopShim.getClass().getClassLoader() ), classCaptor.capture(), objectCaptor.capture() );

    Class[] classCaptorValue = classCaptor.getValue();
    assertEquals( 3, classCaptorValue.length );
    assertEquals( boolean.class, classCaptorValue[0] );
    assertEquals( HadoopConfiguration.class, classCaptorValue[ 1 ] );
    assertEquals( String.class, classCaptorValue[ 2 ] );

    // Important check to ensure that the constructor can actually be called
    assertNotNull( HadoopFileSystemFactoryImpl.class.getConstructor( classCaptorValue ) );

    Object[] objectCaptorValue = objectCaptor.getValue();
    assertEquals( 3, objectCaptorValue.length );
    assertEquals( true, objectCaptorValue[0] );
    assertEquals( hadoopConfiguration, objectCaptorValue[ 1 ] );
    assertEquals( "hdfs", objectCaptorValue[ 2 ] );
  }

  @Test
  public void testOnConfigurationClose() {
    hadoopFileSystemFactoryLoader.onConfigurationClose( hadoopConfiguration );
    verify( shimBridgingServiceTracker ).unregister( hadoopConfiguration );
  }
}
