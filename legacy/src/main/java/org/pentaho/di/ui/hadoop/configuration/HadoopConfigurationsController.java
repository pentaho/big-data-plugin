/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

/**
 * Created by bryan on 8/10/15.
 */
public class HadoopConfigurationsController extends AbstractXulEventHandler {
  public static final String HADOOP_CONFIGURATIONS_CONTROLLER = "hadoopConfigurationsController";
  private static final Log logger = LogFactory.getLog( HadoopConfigurationRestartXulDialog.class );

  public HadoopConfigurationsController() {
    setName( HADOOP_CONFIGURATIONS_CONTROLLER );
  }

  public void promptForShim() {
    final Spoon spoon = Spoon.getInstance();
    spoon.getDisplay().asyncExec( new Runnable() {
      @Override public void run() {
        try {
          //todo:no more active shim require needed - here ui rethink when select shim for step
//          List<HadoopConfigurationInfo> hadoopConfigurationInfos =
//            HadoopConfigurationBootstrap.getInstance().getHadoopConfigurationInfos();
//          Shell shell = spoon.getShell();
//          if ( hadoopConfigurationInfos.size() == 0 ) {
//            new NoHadoopConfigurationsXulDialog( shell ).open();
//          } else {
//            String shimId = new HadoopConfigurationsXulDialog( shell, hadoopConfigurationInfos ).open();
//            if ( !Const.isEmpty( shimId ) ) {
//              try {
//                HadoopConfigurationBootstrap.getInstance().setActiveShim( shimId );
//              } catch ( ConfigurationException e ) {
//                logger.error( e.getMessage(), e );
//              }
//            }
//          }
        } catch ( Exception e ) {
          logger.error( e.getMessage(), e );
        }
      }
    } );
  }
}
