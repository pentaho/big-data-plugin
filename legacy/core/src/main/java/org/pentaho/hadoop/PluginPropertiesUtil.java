/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.hadoop;

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.vfs.KettleVFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility for working with plugin.properties
 */
public class PluginPropertiesUtil {
  public static final String PLUGIN_PROPERTIES_FILE = "plugin.properties";
  public static final String VERSION_PROPERTIES_FILE = "META-INF/version.properties";
  private static final String VERSION_REPLACE_STR = "@VERSION@";

  private final String VERSION_PLACEHOLDER;

  public PluginPropertiesUtil() {
    this( VERSION_PROPERTIES_FILE );
  }

  public PluginPropertiesUtil( String versionPropertiesFile ) {
    VERSION_PLACEHOLDER = getVersionPlaceholder( versionPropertiesFile );
  }

  private static String getVersionPlaceholder( String versionPropertiesFile ) {
    try ( InputStream propertiesStream = PluginPropertiesUtil.class.getClassLoader().getResourceAsStream(
        versionPropertiesFile ) ) {
      Properties properties = new Properties();
      properties.load( propertiesStream );
      return properties.getProperty( "version", VERSION_REPLACE_STR );
    } catch ( Exception e ) {
      return VERSION_REPLACE_STR;
    }
  }

  /**
   * Loads a properties file from the plugin directory for the plugin interface provided
   *
   * @param plugin
   * @return
   * @throws KettleFileException
   * @throws IOException
   */
  protected Properties loadProperties( PluginInterface plugin, String relativeName ) throws KettleFileException,
    IOException {
    if ( plugin == null ) {
      throw new NullPointerException();
    }
    FileObject propFile =
      KettleVFS.getFileObject( plugin.getPluginDirectory().getPath() + Const.FILE_SEPARATOR + relativeName );
    if ( !propFile.exists() ) {
      throw new FileNotFoundException( propFile.toString() );
    }
    try {
      return new PropertiesConfigurationProperties( propFile );
    } catch ( Exception e ) {
      // Do not catch ConfigurationException. Different shims will use different
      // packages for this exception.
      throw new IOException( e );
    }
  }

  /**
   * Loads the plugin properties file for the plugin interface provided
   *
   * @param plugin
   * @return Properties file for plugin
   * @throws KettleFileException
   * @throws IOException
   */
  public Properties loadPluginProperties( PluginInterface plugin ) throws KettleFileException, IOException {
    return loadProperties( plugin, PLUGIN_PROPERTIES_FILE );
  }

  /**
   * @return the version of this plugin
   */
  public String getVersion() {
    // This value is replaced during the ant build process (task: compile.pre)
    return VERSION_PLACEHOLDER;
  }
}
