/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.core.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.VFS;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.plugins.KettleLifecyclePluginType;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.Plugin;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginMainClassType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.MockHadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

public class HadoopConfigurationBootstrapTest {
  /**
   * 
   */
  private static final String TEST_MESSAGE = "Test message";
  private static Plugin plugin = new Plugin( new String[] { HadoopSpoonPlugin.PLUGIN_ID }, StepPluginType.class,
      LifecyclePluginType.class.getAnnotation( PluginMainClassType.class ).value(), "", "", "", null, false, false,
      new HashMap<Class<?>, String>(), new ArrayList<String>(), null, getPluginURL() );

  @SuppressWarnings( "deprecation" )
  private static URL getPluginURL() {
    try {
      return new File( "package-res" ).toURL();
    } catch ( MalformedURLException e ) {
      return null;
    }
  }

  @BeforeClass
  public static void before() throws KettlePluginException {
    PluginRegistry.getInstance().registerPlugin( LifecyclePluginType.class, plugin );
  }

  @AfterClass
  public static void after() throws NoSuchFieldException, SecurityException, IllegalArgumentException,
    IllegalAccessException {
    Field pluginMapField = PluginRegistry.class.getDeclaredField( "pluginMap" );
    pluginMapField.setAccessible( true );
    @SuppressWarnings( "unchecked" )
    Map<Class<? extends PluginTypeInterface>, List<PluginInterface>> pluginMap =
        (Map<Class<? extends PluginTypeInterface>, List<PluginInterface>>) pluginMapField.get( PluginRegistry
            .getInstance() );
    pluginMap.get( LifecyclePluginType.class ).remove( plugin );
  }

  @Test
  public void getActiveConfigurationId_missing_property() {

    try {
      ( new HadoopConfigurationBootstrap() {
        public java.util.Properties getPluginProperties() throws ConfigurationException {
          return new Properties();
        };
      } ).getProvider();
      fail( "Expected exception" );
    } catch ( ConfigurationException ex ) {
      assertEquals( "Active configuration property is not set in plugin.properties: \""
          + HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION + "\".", ex.getMessage() );
      assertNull( ex.getCause() );
    }
  }

  @Test
  public void getActiveConfigurationId_exception_getting_properties() throws ConfigurationException {
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap() {
      public java.util.Properties getPluginProperties() throws ConfigurationException {
        throw new NullPointerException();
      };
    };

    try {
      b.getActiveConfigurationId();
      fail( "Expected exception" );
    } catch ( ConfigurationException ex ) {
      assertEquals( "Unable to determine active Hadoop configuration.", ex.getMessage() );
      assertNotNull( ex.getCause() );
    }
  }

  @Test
  public void getPluginInterface_not_registered() throws ConfigurationException {
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap();

    try {
      b.getPluginInterface();
    } catch ( KettleException ex ) {
      assertEquals( "\nError locating plugin. Please make sure the Plugin Registry has been initialized.\n", ex
          .getMessage() );
    }
  }

  @Test
  public void getPluginInterface() throws Exception {
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap();
    PluginInterface retrieved = b.getPluginInterface();
    assertEquals( plugin, retrieved );
  }

  @Test
  public void resolveHadoopConfigurationsDirectory() throws Exception {
    final FileObject ramRoot = VFS.getManager().resolveFile( "ram://" );
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap() {
      public FileObject locatePluginDirectory() throws ConfigurationException {
        return ramRoot;
      };

      @Override
      public Properties getPluginProperties() throws ConfigurationException {
        Properties p = new Properties();
        p.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, "hadoop-configs-go-here" );
        return p;
      }
    };

    FileObject hadoopConfigsDir = b.resolveHadoopConfigurationsDirectory();
    assertNotNull( hadoopConfigsDir );

    assertEquals( ramRoot.resolveFile( "hadoop-configs-go-here" ).getURL(), hadoopConfigsDir.getURL() );
  }

  @Test
  public void getHadoopConfigurationProvider() throws Exception {
    final FileObject ramRoot = VFS.getManager().resolveFile( "ram://" );
    final String CONFIGS_PATH = "hadoop-configs-go-here";
    ramRoot.resolveFile( CONFIGS_PATH ).createFolder();

    HadoopConfiguration c = new HadoopConfiguration( ramRoot, "test", "test", new MockHadoopShim() );
    final HadoopConfigurationProvider provider = new MockHadoopConfigurationProvider( Arrays.asList( c ), "test" );

    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap() {
      public FileObject locatePluginDirectory() throws ConfigurationException {
        return ramRoot;
      };

      @Override
      public Properties getPluginProperties() throws ConfigurationException {
        Properties p = new Properties();
        p.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, CONFIGS_PATH );
        p.setProperty( HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION, "test" );
        return p;
      }

      @Override
      protected HadoopConfigurationProvider initializeHadoopConfigurationProvider( FileObject hadoopConfigurationsDir )
        throws ConfigurationException {
        return provider;
      }
    };

    assertEquals( provider, b.getProvider() );
  }

  @Test
  public void getHadoopConfigurationProvider_active_invalid() throws Exception {
    final FileObject ramRoot = VFS.getManager().resolveFile( "ram://" );
    final String CONFIGS_PATH = "hadoop-configs-go-here";
    ramRoot.resolveFile( CONFIGS_PATH ).createFolder();

    HadoopConfiguration c = new HadoopConfiguration( ramRoot, "test", "test", new MockHadoopShim() );
    final HadoopConfigurationProvider provider = new MockHadoopConfigurationProvider( Arrays.asList( c ), "invalid" );

    try {
      ( new HadoopConfigurationBootstrap() {
        public FileObject locatePluginDirectory() throws ConfigurationException {
          return ramRoot;
        };

        @Override
        public Properties getPluginProperties() throws ConfigurationException {
          Properties p = new Properties();
          p.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, CONFIGS_PATH );
          p.setProperty( HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION, "invalid" );
          return p;
        }

        @Override
        protected HadoopConfigurationProvider initializeHadoopConfigurationProvider( FileObject hadoopConfigurationsDir )
          throws ConfigurationException {
          return provider;
        }
      } ).getProvider();
      fail( "Expected exception" );
    } catch ( ConfigurationException ex ) {
      assertEquals( "Invalid active Hadoop configuration: \"invalid\".", ex.getMessage() );
    }
  }

  @Test
  public void getHadoopConfigurationProvider_getActiveException() throws Exception {
    final FileObject ramRoot = VFS.getManager().resolveFile( "ram://" );
    final String CONFIGS_PATH = "hadoop-configs-go-here";
    ramRoot.resolveFile( CONFIGS_PATH ).createFolder();

    HadoopConfiguration c = new HadoopConfiguration( ramRoot, "test", "test", new MockHadoopShim() );

    try {
      final HadoopConfigurationProvider provider = new MockHadoopConfigurationProvider( Arrays.asList( c ), "test" ) {
        public HadoopConfiguration getActiveConfiguration() throws ConfigurationException {
          throw new NullPointerException();
        };
      };

      ( new HadoopConfigurationBootstrap() {
        public FileObject locatePluginDirectory() throws ConfigurationException {
          return ramRoot;
        };

        @Override
        public Properties getPluginProperties() throws ConfigurationException {
          Properties p = new Properties();
          p.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, CONFIGS_PATH );
          p.setProperty( HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION, "test" );
          return p;
        }

        @Override
        protected HadoopConfigurationProvider initializeHadoopConfigurationProvider( FileObject hadoopConfigurationsDir )
          throws ConfigurationException {
          return provider;
        }
      } ).getProvider();
      fail( "Expected exception" );
    } catch ( ConfigurationException ex ) {
      assertEquals( "Invalid active Hadoop configuration: \"test\".", ex.getMessage() );
    }
  }

  @Test
  public void initializeHadoopConfigurationProvider() throws Exception {
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap();

    HadoopConfigurationProvider provider =
        b.initializeHadoopConfigurationProvider( VFS.getManager().resolveFile( "ram://" ) );
    assertNotNull( provider );
  }

  @Test
  public void locatePluginDirectory() throws Exception {
    FileObject ramRoot = VFS.getManager().resolveFile( "ram:///" );
    final URL folderURL = ramRoot.getURL();
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap() {
      protected PluginInterface getPluginInterface() throws KettleException {
        return new Plugin( new String[] { "id" }, KettleLifecyclePluginType.class, null, null, null, null, null, false,
            false, null, null, null, folderURL );
      };
    };

    assertEquals( ramRoot.getURL(), b.locatePluginDirectory().getURL() );
  }

  @Test
  public void locatePluginDirectory_null_plugin_folder() throws Exception {
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap() {
      protected PluginInterface getPluginInterface() throws KettleException {
        return new Plugin( new String[] { "id" }, KettleLifecyclePluginType.class, null, null, null, null, null, false,
            false, null, null, null, null );
      };
    };

    try {
      b.locatePluginDirectory();
      fail( "Expected exception" );
    } catch ( Exception ex ) {
      assertEquals( "Hadoop configuration directory could not be located. Hadoop functionality will not work.", ex
          .getMessage() );
    }
  }

  @Test
  public void locatePluginDirectory_invalid_path() throws Exception {
    FileObject ramRoot = VFS.getManager().resolveFile( "ram:///does-not-exist" );
    final URL folderURL = ramRoot.getURL();
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap() {
      protected PluginInterface getPluginInterface() throws KettleException {
        return new Plugin( new String[] { "id" }, KettleLifecyclePluginType.class, null, null, null, null, null, false,
            false, null, null, null, folderURL );
      };
    };

    try {
      b.locatePluginDirectory();
      fail( "Expected exception" );
    } catch ( Exception ex ) {
      assertEquals( "Hadoop configuration directory could not be located. Hadoop functionality will not work.", ex
          .getMessage() );
    }
  }

  @Test
  public void testLifecycleExceptionWithSevereTrueThrows_WhenConfigurationExceptionOccursOnEnvInit() throws Exception {
    HadoopConfigurationBootstrap hadoopConfigurationBootstrap = new HadoopConfigurationBootstrap();
    HadoopConfigurationBootstrap hadoopConfigurationBootstrapSpy = spy( hadoopConfigurationBootstrap );
    doThrow( new ConfigurationException( TEST_MESSAGE ) ).when( hadoopConfigurationBootstrapSpy ).getProvider();
    try {
      hadoopConfigurationBootstrapSpy.onEnvironmentInit();
      fail( "Expected LifecycleException exception but wasn't" );
    } catch ( LifecycleException lcExc ) {
      assertTrue( lcExc.isSevere() );
    }
  }


}
