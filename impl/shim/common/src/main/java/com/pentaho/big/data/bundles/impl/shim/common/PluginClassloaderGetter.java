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

package com.pentaho.big.data.bundles.impl.shim.common;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;

/**
 * Created by bryan on 10/5/15.
 *
 * @deprecated
 */
@Deprecated
public class PluginClassloaderGetter {
  private final PluginRegistry pluginRegistry;

  public PluginClassloaderGetter() {
    this( PluginRegistry.getInstance() );
  }

  public PluginClassloaderGetter( PluginRegistry pluginRegistry ) {
    this.pluginRegistry = pluginRegistry;
  }

  /**
   * Gets the classloader for the specified plugin, blocking until the plugin becomes available the feature watcher will
   * kill us after a while anyway
   *
   * @param pluginType the plugin type (Specified as a string so that we can get the classloader for plugin types OSGi
   *                   doesn't know about)
   * @param pluginId   the plugin id
   * @return
   * @throws KettlePluginException
   * @throws InterruptedException
   */
  public ClassLoader getPluginClassloader( String pluginType, String pluginId )
    throws KettlePluginException {
    Class<? extends PluginTypeInterface> pluginTypeInterface = null;
    PluginRegistry pluginRegistry = this.pluginRegistry;
    while ( true ) {
      synchronized ( pluginRegistry ) {
        if ( pluginTypeInterface == null ) {
          for ( Class<? extends PluginTypeInterface> potentialPluginTypeInterface : pluginRegistry.getPluginTypes() ) {
            if ( pluginType.equals( potentialPluginTypeInterface.getCanonicalName() ) ) {
              pluginTypeInterface = potentialPluginTypeInterface;
            }
          }
        }
        PluginInterface plugin = pluginRegistry.getPlugin( pluginTypeInterface, pluginId );
        if ( plugin != null ) {
          return pluginRegistry.getClassLoader( plugin );
        }
        try {
          pluginRegistry.wait();
        } catch ( InterruptedException e ) {
          throw new KettlePluginException( e );
        }
      }
    }
  }
}
