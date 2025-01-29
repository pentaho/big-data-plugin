/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
