/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.di.ui.hadoop.configuration;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;

import java.util.ResourceBundle;

/**
 * Created by bryan on 8/10/15.
 */
@SpoonPluginCategories( { "spoon" } )
@SpoonPlugin( id = "HadoopConfigurationsSpoonPlugin", image = "" )
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
    } catch ( XulException e ) {
      log.logError( e.getMessage() );
    }
    //todo:no more active shim require needed - here ui rethink when select shim for step
//    HadoopConfigurationBootstrap.getInstance().setPrompter( new HadoopConfigurationPrompter() {
//      @Override
//      public String getConfigurationSelection( final List<HadoopConfigurationInfo> hadoopConfigurationInfos ) {
//        final Spoon spoon = Spoon.getInstance();
//        final AtomicReference<String> atomicReference = new AtomicReference<>();
//        spoon.getDisplay().syncExec( new Runnable() {
//          @Override
//          public void run() {
//            // If there are no shims, bring up the "no shims" dialog, otherwise bring up the shim select dialog
//            atomicReference
//              .set( Const.isEmpty( hadoopConfigurationInfos )
//                ? new NoHadoopConfigurationsXulDialog( spoon.getShell() ).open()
//                : new HadoopConfigurationsXulDialog( spoon.getShell(), hadoopConfigurationInfos ).open() );
//          }
//        } );
//        return atomicReference.get();
//      }
//
//      @Override
//      public void promptForRestart() {
//        final Spoon spoon = Spoon.getInstance();
//        spoon.getDisplay().syncExec( new Runnable() {
//          @Override
//          public void run() {
//            new HadoopConfigurationRestartXulDialog( spoon.getShell() ).open();
//          }
//        } );
//      }
//    } );
  }

  public SpoonLifecycleListener getLifecycleListener() {
    return null;
  }

  public SpoonPerspective getPerspective() {
    return null;
  }

}
