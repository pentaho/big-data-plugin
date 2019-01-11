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
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.core.Const;
import org.pentaho.di.ui.core.dialog.ShowHelpDialog;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.di.ui.spoon.XulSpoonSettingsManager;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

public class HadoopConfigurationRestartXulDialog extends AbstractXulEventHandler {
  private static final Class<?> PKG = HadoopConfigurationsXulDialog.class;
  private static final String CONTROLLER_NAME = "hadoopConfigurationRestartXulDialog";
  private static final String XUL = "org/pentaho/di/ui/hadoop/configuration/restart-prompt.xul";
  private static final Log logger = LogFactory.getLog( HadoopConfigurationRestartXulDialog.class );
  private final Shell shell;
  private XulDialog promptDialog;

  public HadoopConfigurationRestartXulDialog( Shell aShell ) {
    this.shell = aShell;
    setName( CONTROLLER_NAME );
  }

  public void open() {
    try {
      KettleXulLoader e = new KettleXulLoader();
      e.setOuterContext( shell );
      e.setSettingsManager( XulSpoonSettingsManager.getInstance() );
      e.registerClassLoader( getClass().getClassLoader() );
      XulDomContainer container = e.loadXul( XUL, new XulSpoonResourceBundle( PKG ) );
      container.addEventHandler( this );
      XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();
      promptDialog = (XulDialog) container.getDocumentRoot().getElementById( "hadoopConfigurationRestartDialog" );
      promptDialog.show();
      ( (SwtDialog) promptDialog ).dispose();
    } catch ( Exception var4 ) {
      logger.info( var4 );
    }
  }

  public void close() {
    promptDialog.hide();
  }

  public void showHelp() {
    String docUrl =
        Const.getDocUrl( BaseMessages.getString( PKG, "HadoopConfigurationRestartXulDialog.Help.Url" ) );

    ShowHelpDialog showHelpDialog = new ShowHelpDialog( shell,
        BaseMessages.getString( PKG, "HadoopConfigurationRestartXulDialog.Help.Title" ),
        docUrl,
        BaseMessages.getString( PKG, "HadoopConfigurationRestartXulDialog.Help.Header" ) ) {

      // Parent is modal so we have to be as well
      @Override
      protected Shell createShell( Shell parent ) {
        return new Shell( parent, SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
      }
    };

    showHelpDialog.open();
    showHelpDialog.dispose();
  }
}
