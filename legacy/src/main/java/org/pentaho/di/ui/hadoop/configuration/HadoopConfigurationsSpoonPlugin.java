/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.hadoop.configuration;

import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationInfo;
import org.pentaho.di.core.hadoop.HadoopConfigurationPrompter;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by bryan on 8/10/15.
 */
@SpoonPluginCategories( { "spoon" } ) @SpoonPlugin( id = "HadoopConfigurationsSpoonPlugin", image = "" )
public class HadoopConfigurationsSpoonPlugin implements SpoonPluginInterface {

  private static Class<?> PKG = HadoopConfigurationsSpoonPlugin.class;

  private LogChannelInterface log = new LogChannel( HadoopConfigurationsSpoonPlugin.class.getName() );

  private ResourceBundle resourceBundle = new XulSpoonResourceBundle( PKG );

  public void applyToContainer( String category, XulDomContainer container ) throws XulException {
    try {
      container.registerClassLoader( getClass().getClassLoader() );
      container.loadOverlay( "org/pentaho/di/ui/hadoop/configuration/toolbar-overlay.xul", resourceBundle );
      HadoopConfigurationsController controller = new HadoopConfigurationsController();
      container.addEventHandler( controller );
      controller.init();
    } catch ( XulException e ) {
      log.logError( e.getMessage() );
    }
    HadoopConfigurationBootstrap.getInstance().setPrompter( new HadoopConfigurationPrompter() {
      @Override
      public String getConfigurationSelection( final List<HadoopConfigurationInfo> hadoopConfigurationInfos ) {
        final Spoon spoon = Spoon.getInstance();
        final AtomicReference<String> atomicReference = new AtomicReference<>();
        spoon.getDisplay().syncExec( new Runnable() {
          @Override public void run() {
            atomicReference
              .set( new HadoopConfigurationsXulDialog( spoon.getShell(), hadoopConfigurationInfos ).open() );
          }
        } );
        return atomicReference.get();
      }

      @Override public boolean promptForRestart() {
        final AtomicReference<Boolean> atomicReference = new AtomicReference<>();
        final Spoon spoon = Spoon.getInstance();
        spoon.getDisplay().syncExec( new Runnable() {
          @Override public void run() {
            atomicReference.set( new HadoopConfigurationRestartXulDialog( spoon.getShell() ).open() );
          }
        } );
        return atomicReference.get();
      }
    } );
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }

}
