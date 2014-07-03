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

package org.pentaho.hadoop;

import org.apache.commons.vfs.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.vfs.KettleVFS;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Utility for working with plugin.properties
 */
public class PluginPropertiesUtil {
  public static final String PLUGIN_PROPERTIES_FILE = "plugin.properties";

  /**
   * Placeholder for the version string that is replaced during the ant build process. This is mangled here so we have
   * something to compare against to determine if the replacement has occured.
   */
  private static final String VERSION_PLACEHOLDER = "@" + "VERSION@";

  /**
   * Loads a properties file from the plugin directory for the plugin interface provided
   * 
   * @param plugin
   * @return
   * @throws KettleFileException
   * @throws FileSystemException
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
    Properties p = new Properties();
    p.load( KettleVFS.getInputStream( propFile ) );
    return p;
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
   * @return the version of this plugin or {@code null} if not set during the ant build process
   */
  public String getVersion() {
    // This value is replaced during the ant build process (task: compile.pre)
    String version = "@VERSION@";
    return VERSION_PLACEHOLDER.equals( version ) ? null : version;
  }
}
