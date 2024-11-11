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


package org.pentaho.di.ui.hadoop.configuration;

import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.ui.xul.XulDomContainer;

/**
 * Created by bryan on 8/10/15.
 */
@SpoonPluginCategories( { "spoon" } )
@SpoonPlugin( id = "HadoopConfigurationsSpoonPlugin", image = "" )
public class HadoopConfigurationsSpoonPlugin implements SpoonPluginInterface {

  public void applyToContainer( String category, XulDomContainer container ) {
    HadoopConfigurationsController controller = new HadoopConfigurationsController();
    container.addEventHandler( controller );
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }

}
