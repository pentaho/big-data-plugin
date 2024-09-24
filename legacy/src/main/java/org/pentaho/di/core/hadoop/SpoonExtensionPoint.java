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

package org.pentaho.di.core.hadoop;

import java.util.ResourceBundle;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.ui.repository.repositoryexplorer.ControllerInitializationException;
import org.pentaho.di.ui.repository.repositoryexplorer.controllers.NamedClustersController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

@SpoonPluginCategories( { "repository-explorer" } ) @SpoonPlugin( id = "SpoonExtensionPoint", image = "" )
public class SpoonExtensionPoint implements SpoonPluginInterface {

  private static Class<?> PKG = SpoonExtensionPoint.class;

  private LogChannelInterface log = new LogChannel( SpoonExtensionPoint.class.getName() );

  private ResourceBundle resourceBundle = new XulSpoonResourceBundle( PKG );

  public void applyToContainer( String category, XulDomContainer container ) throws XulException {
    try {
      container.registerClassLoader( getClass().getClassLoader() );
      container.loadOverlay( "org/pentaho/di/core/hadoop/explorer-layout-overlay.xul", resourceBundle );
      NamedClustersController controller = new NamedClustersController();
      controller.init( Spoon.getInstance().rep );
      container.addEventHandler( controller );
    } catch ( XulException e ) {
      log.logError( e.getMessage() );
    } catch ( ControllerInitializationException e ) {
      log.logError( e.getMessage() );
    }
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }
}
