/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
