/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.hadoop;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.pentaho.di.core.plugins.PluginInterface;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class PluginPropertiesUtilTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void getVersion() {
    // This test will only success if using classes produced by the ant build
    PluginPropertiesUtil util = new PluginPropertiesUtil();
    assertNotNull(
      "Should never be null",
      util.getVersion() );
  }

  @Test
  public void testGetVersionFromNonDefaultLocation() {
    PluginPropertiesUtil ppu = new PluginPropertiesUtil( "test-version.properties" );
    String version = ppu.getVersion();
    assertEquals( "X.Y.Z-TEST", version );
  }

  @Test
  public void testGetVersionFromNonExistingLocation() {
    PluginPropertiesUtil ppu = new PluginPropertiesUtil( "non-existing-version.properties" );
    String version = ppu.getVersion();
    assertEquals( "@VERSION@", version );
  }

  @Test
  public void testLoadPluginProperties() throws Exception {
    File pluginDir = tempFolder.newFolder( "pluginDir" );
    File propsFile = new File( pluginDir, PluginPropertiesUtil.PLUGIN_PROPERTIES_FILE );

    Properties written = new Properties();
    written.setProperty( "active.hadoop.configuration", "test-shim" );
    written.setProperty( "hadoop.configurations.path", "hadoop-configurations" );
    try ( FileOutputStream fos = new FileOutputStream( propsFile ) ) {
      written.store( fos, null );
    }

    PluginInterface mockPlugin = mock( PluginInterface.class );
    when( mockPlugin.getPluginDirectory() ).thenReturn( pluginDir.toURI().toURL() );

    PluginPropertiesUtil util = new PluginPropertiesUtil();
    Properties loaded = util.loadPluginProperties( mockPlugin );

    assertNotNull( loaded );
    assertEquals( "test-shim", loaded.getProperty( "active.hadoop.configuration" ) );
    assertEquals( "hadoop-configurations", loaded.getProperty( "hadoop.configurations.path" ) );
  }

  @Test
  public void testSavePluginProperties() throws Exception {
    File pluginDir = tempFolder.newFolder( "pluginDir" );
    File propsFile = new File( pluginDir, PluginPropertiesUtil.PLUGIN_PROPERTIES_FILE );
    propsFile.createNewFile();

    PluginInterface mockPlugin = mock( PluginInterface.class );
    when( mockPlugin.getPluginDirectory() ).thenReturn( pluginDir.toURI().toURL() );

    Properties toSave = new Properties();
    toSave.setProperty( "active.hadoop.configuration", "new-shim" );

    PluginPropertiesUtil util = new PluginPropertiesUtil();
    util.savePluginProperties( mockPlugin, toSave );

    Properties reloaded = util.loadPluginProperties( mockPlugin );
    assertNotNull( reloaded );
    assertEquals( "new-shim", reloaded.getProperty( "active.hadoop.configuration" ) );
  }

  @Test
  public void testLoadAndSaveRoundTrip() throws Exception {
    File pluginDir = tempFolder.newFolder( "pluginDir" );
    File propsFile = new File( pluginDir, PluginPropertiesUtil.PLUGIN_PROPERTIES_FILE );

    Properties original = new Properties();
    original.setProperty( "active.hadoop.configuration", "shim-v1" );
    original.setProperty( "hadoop.configurations.path", "hadoop-configurations" );
    try ( FileOutputStream fos = new FileOutputStream( propsFile ) ) {
      original.store( fos, null );
    }

    PluginInterface mockPlugin = mock( PluginInterface.class );
    when( mockPlugin.getPluginDirectory() ).thenReturn( pluginDir.toURI().toURL() );

    PluginPropertiesUtil util = new PluginPropertiesUtil();
    Properties loaded = util.loadPluginProperties( mockPlugin );
    loaded.setProperty( "active.hadoop.configuration", "shim-v2" );
    util.savePluginProperties( mockPlugin, loaded );

    Properties reloaded = util.loadPluginProperties( mockPlugin );
    assertEquals( "shim-v2", reloaded.getProperty( "active.hadoop.configuration" ) );
    assertEquals( "hadoop-configurations", reloaded.getProperty( "hadoop.configurations.path" ) );
  }

  @Test( expected = NullPointerException.class )
  public void testLoadPluginPropertiesNullPlugin() throws Exception {
    new PluginPropertiesUtil().loadPluginProperties( null );
  }

  @Test( expected = NullPointerException.class )
  public void testSavePluginPropertiesNullPlugin() throws Exception {
    new PluginPropertiesUtil().savePluginProperties( null, new Properties() );
  }

  @Test( expected = IOException.class )
  public void testLoadPluginPropertiesFileNotFound() throws Exception {
    File pluginDir = tempFolder.newFolder( "emptyPluginDir" );
    PluginInterface mockPlugin = mock( PluginInterface.class );
    when( mockPlugin.getPluginDirectory() ).thenReturn( pluginDir.toURI().toURL() );

    new PluginPropertiesUtil().loadPluginProperties( mockPlugin );
  }

}
