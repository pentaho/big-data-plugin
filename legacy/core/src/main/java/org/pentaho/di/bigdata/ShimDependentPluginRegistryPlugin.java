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

package org.pentaho.di.bigdata;

import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginRegistryExtension;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.RegistryPlugin;

@RegistryPlugin(
    id = "ShimDependentPluginRegistryPlugin",
    name = "ShimDependentPluginRegistryPlugin",
    description = "Registers sub plugins of the big data plugin that depend on the shim jars in their classpath" )
    //classLoaderGroup = "big-data" )

public class ShimDependentPluginRegistryPlugin implements PluginRegistryExtension {

  @Override
  public String getPluginId( Class<? extends PluginTypeInterface> arg0, Object arg1 ) {
    return null;
  }

  @Override
  public void init( PluginRegistry pluginRegistry ) {
    if ( KettleClientEnvironment.isInitialized() ) {
      PluginRegistry.addPluginType( ShimDependentJobEntryPluginType.getInstance() );
    }
  }

  @Override
  public void searchForType( PluginTypeInterface pluginTypeInterface ) {
  }
}
