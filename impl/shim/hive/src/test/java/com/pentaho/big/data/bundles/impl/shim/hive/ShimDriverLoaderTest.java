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

package com.pentaho.big.data.bundles.impl.shim.hive;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.BundleContext;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.sql.Driver;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/18/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class ShimDriverLoaderTest {
  @Mock JdbcUrlParser jdbcUrlParser;
  @Mock HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  @Mock HadoopConfiguration hadoopConfiguration;
  @Mock HadoopShim hadoopShim;
  @Mock Driver driver;
  @Mock BundleContext bundleContext;
  private ShimDriverLoader shimDriverLoader;

  @Before
  public void setup() {
    shimDriverLoader = new ShimDriverLoader( jdbcUrlParser, bundleContext, hadoopConfigurationBootstrap );
    when( hadoopConfiguration.getHadoopShim() ).thenReturn( hadoopShim );
  }

  @Test
  public void testOnClassLoaderAvailableNoop() throws ConfigurationException {
    ClassLoader classLoader = mock( ClassLoader.class );
    shimDriverLoader.onClassLoaderAvailable( classLoader );
    verify( hadoopConfigurationBootstrap ).registerHadoopConfigurationListener( shimDriverLoader );
    verifyNoMoreInteractions( jdbcUrlParser, hadoopConfiguration, hadoopConfigurationBootstrap, classLoader );
  }

  @Test
  public void testOnConfigurationOpenNull() throws ConfigurationException {
    shimDriverLoader.onConfigurationOpen( null, false );
    verify( hadoopConfigurationBootstrap ).registerHadoopConfigurationListener( shimDriverLoader );
    verifyNoMoreInteractions( jdbcUrlParser, hadoopConfiguration, hadoopConfigurationBootstrap );
  }

  @Test
  public void testOnConfigurationOpen() {
    when( hadoopShim.getJdbcDriver( ShimDriverLoader.HIVE ) ).thenReturn( driver );
  }
}
