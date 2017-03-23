/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Matchers.any;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.VFS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.KettleLifecyclePluginType;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.Plugin;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginMainClassType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.MockHadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.MockHadoopShim;

public class HadoopConfigurationBootstrapTest {

  private Plugin plugin;

  private java.util.Properties prop = new Properties();

  @After
  public void tearDown() {
    PluginRegistry.getInstance().removePlugin( LifecyclePluginType.class, plugin );
  }

  @Before
  public void setUp() throws MalformedURLException, KettlePluginException {
    URL url = Paths.get( "src/test/resources" ).toUri().toURL();
    plugin = new Plugin( new String[] { HadoopSpoonPlugin.PLUGIN_ID },
        StepPluginType.class,
        LifecyclePluginType.class.getAnnotation( PluginMainClassType.class ).value(),
        "", "", "", null, false, false,
        new HashMap<Class<?>, String>(), new ArrayList<String>(), null, url );

    PluginRegistry.getInstance().registerPlugin( LifecyclePluginType.class, plugin );
    prop.setProperty( HadoopConfigurationBootstrap.MAX_TIMEOUT_BEFORE_LOADING_SHIM, "1" );
  }

  @Test
  public void getActiveConfigurationId_missing_property() {
    try {
      HadoopConfigurationBootstrap boot = spy( new HadoopConfigurationBootstrap() );
      doReturn( prop ).when( boot ).getPluginProperties();
      doReturn( prop ).when( boot ).getMergedPmrAndPluginProperties();
      boot.getProvider();
      fail( "Expected exception" );
    } catch ( ConfigurationException ex ) {
      assertEquals( "Active configuration property is not set in plugin.properties: \""
          + HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION + "\".", ex.getMessage() );
      assertNull( ex.getCause() );
    }
  }

  @Test
  public void getActiveConfigurationId_exception_getting_properties() throws ConfigurationException {
    HadoopConfigurationBootstrap b = spy( new HadoopConfigurationBootstrap() );
    doThrow( NullPointerException.class ).when( b ).getPluginProperties();
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
    PluginRegistry.getInstance().removePlugin( LifecyclePluginType.class, plugin );
    HadoopConfigurationBootstrap b = new HadoopConfigurationBootstrap();
    try {
      b.getPluginInterface();
      fail( "Expected error" );
    } catch ( KettleException ex ) {
      assertTrue( ex.getMessage().contains( "Error locating plugin. Please make sure the Plugin Registry has been initialized." ) );
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
    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, "hadoop-configs-go-here" );
    HadoopConfigurationBootstrap b = spy( new HadoopConfigurationBootstrap() );
    doReturn( ramRoot ).when( b ).locatePluginDirectory();
    doReturn( prop ).when( b ).getPluginProperties();

    FileObject hadoopConfigsDir = b.resolveHadoopConfigurationsDirectory();
    assertNotNull( hadoopConfigsDir );
    assertEquals( ramRoot.resolveFile( "hadoop-configs-go-here" ).getURL(), hadoopConfigsDir.getURL() );
  }

  @Test
  public void getHadoopConfigurationProvider() throws Exception {
    final FileObject ramRoot = VFS.getManager().resolveFile( "ram://" );
    String CONFIGS_PATH = "hadoop-configs-go-here";
    ramRoot.resolveFile( CONFIGS_PATH ).createFolder();

    HadoopConfiguration c = new HadoopConfiguration( ramRoot, "test", "test", new MockHadoopShim() );
    HadoopConfigurationProvider provider = new MockHadoopConfigurationProvider( Arrays.asList( c ), "test" );

    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, CONFIGS_PATH );
    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION, "test" );

    HadoopConfigurationBootstrap boot = spy( new HadoopConfigurationBootstrap() );
    boot.setPrompter( mock( HadoopConfigurationPrompter.class ) );
    doReturn( ramRoot ).when( boot ).locatePluginDirectory();
    doReturn( prop ).when( boot ).getPluginProperties();
    doReturn( prop ).when( boot ).getMergedPmrAndPluginProperties();
    doReturn( provider ).when( boot ).initializeHadoopConfigurationProvider( any( FileObject.class ) );
    assertEquals( provider, boot.getProvider() );
  }

  @Test
  public void getHadoopConfigurationProvider_active_invalid() throws Exception {
    final FileObject ramRoot = VFS.getManager().resolveFile( "ram://" );
    final String CONFIGS_PATH = "hadoop-configs-go-here";
    ramRoot.resolveFile( CONFIGS_PATH ).createFolder();

    HadoopConfiguration c = new HadoopConfiguration( ramRoot, "test", "test", new MockHadoopShim() );
    HadoopConfigurationProvider provider = new MockHadoopConfigurationProvider( Arrays.asList( c ), "invalid" );

    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, CONFIGS_PATH );
    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION, "invalid" );

    HadoopConfigurationBootstrap b = spy( new HadoopConfigurationBootstrap() );
    doReturn( ramRoot ).when( b ).locatePluginDirectory();
    doReturn( prop ).when( b ).getPluginProperties();
    doReturn( prop ).when( b ).getMergedPmrAndPluginProperties();
    doReturn( provider ).when( b ).initializeHadoopConfigurationProvider( any( FileObject.class ) );
    try {
      b.getProvider();
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

    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_HADOOP_CONFIGURATIONS_PATH, CONFIGS_PATH );
    prop.setProperty( HadoopConfigurationBootstrap.PROPERTY_ACTIVE_HADOOP_CONFIGURATION, "test" );

    HadoopConfiguration c = new HadoopConfiguration( ramRoot, "test", "test", new MockHadoopShim() );
    try {
      HadoopConfigurationProvider provider = new MockHadoopConfigurationProvider( Arrays.asList( c ), "test" ) {
        public HadoopConfiguration getActiveConfiguration() throws ConfigurationException {
          throw new NullPointerException();
        };
      };
      HadoopConfigurationBootstrap boot = spy( new HadoopConfigurationBootstrap() );
      doReturn( ramRoot ).when( boot ).locatePluginDirectory();
      doReturn( prop ).when( boot ).getPluginProperties();
      doReturn( prop ).when( boot ).getMergedPmrAndPluginProperties();
      doReturn( provider ).when( boot ).initializeHadoopConfigurationProvider( any( FileObject.class ) );

      boot.getProvider();
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
    Plugin plug = new Plugin( new String[] { "id" }, KettleLifecyclePluginType.class,
        null, null, null, null, null, false,
        false, null, null, null, folderURL );
    HadoopConfigurationBootstrap b = spy( new HadoopConfigurationBootstrap() );
    doReturn( plug ).when( b ).getPluginInterface();
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
    HadoopConfigurationBootstrap boot = spy( new HadoopConfigurationBootstrap() );
    //add this to avoid long wait during load shim
    doReturn( prop ).when( boot ).getMergedPmrAndPluginProperties();
    doThrow( new ConfigurationException( null ) ).when( boot ).resolveHadoopConfigurationsDirectory();
    try {
      boot.getProvider();
      fail( "Expected LifecycleException exception but wasn't" );
    } catch ( ConfigurationException lcExc ) {
      // Ignore
    }
  }


}
