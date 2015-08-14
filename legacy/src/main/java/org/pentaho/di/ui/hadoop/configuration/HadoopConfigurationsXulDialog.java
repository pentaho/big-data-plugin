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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.hadoop.HadoopConfigurationInfo;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ShowHelpDialog;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.di.ui.spoon.XulSpoonSettingsManager;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.jface.tags.JfaceMenuList;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 8/13/15.
 */
public class HadoopConfigurationsXulDialog extends AbstractXulEventHandler {
  private static final Class<?> PKG = HadoopConfigurationsXulDialog.class;
  private static final String CONTROLLER_NAME = "hadoopConfigurationsXulDialog";
  private static final String XUL = "org/pentaho/di/ui/hadoop/configuration/select-config.xul";
  private static final Log logger = LogFactory.getLog( HadoopConfigurationsXulDialog.class );
  private final Shell shell;
  private final List<HadoopConfigurationInfo> hadoopConfigurationInfos;
  private XulDialog selectDialog;
  private boolean accept = false;

  public HadoopConfigurationsXulDialog( Shell aShell, List<HadoopConfigurationInfo> hadoopConfigurationInfos ) {
    this.shell = new Shell( aShell, SWT.SHELL_TRIM | SWT.APPLICATION_MODAL );
    this.hadoopConfigurationInfos = hadoopConfigurationInfos;
    setName( CONTROLLER_NAME );
  }

  public String open() {
    String selectedShim = "";
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
      selectDialog = (XulDialog) container.getDocumentRoot().getElementById( "hadoopConfigurationSelectionDialog" );
      JfaceMenuList<String> hadoopConfigurationSelectionDialogMenuList =
        (JfaceMenuList<String>) container.getDocumentRoot()
          .getElementById( "hadoopConfigurationSelectionDialogMenuList" );
      List<String> configurations = new ArrayList<>();
      for ( HadoopConfigurationInfo hadoopConfigurationInfo : hadoopConfigurationInfos ) {
        configurations.add( hadoopConfigurationInfo.getName() );
      }
      hadoopConfigurationSelectionDialogMenuList.setElements( configurations );
      selectDialog.show();
      if ( accept ) {
        selectedShim =
          hadoopConfigurationInfos.get( hadoopConfigurationSelectionDialogMenuList.getSelectedIndex() ).getId();
      }
      ( (SwtDialog) selectDialog ).dispose();
    } catch ( Exception var4 ) {
      logger.info( var4 );
    }
    return selectedShim;
  }

  public void cancel() {
    selectDialog.hide();
  }

  public void accept() {
    accept = true;
    selectDialog.hide();
  }

  public void showHelp() {
    ShowHelpDialog showHelpDialog =
      new ShowHelpDialog( shell,
        BaseMessages.getString( PKG, "HadoopConfigurationSelectionDialog.Help.Title" ),
        BaseMessages.getString( PKG, "HadoopConfigurationSelectionDialog.Help.Url" ),
        BaseMessages.getString( PKG, "HadoopConfigurationSelectionDialog.Help.Header" ) ) {

        // Parent is modal so we have to be as well
        @Override protected Shell createShell( Shell parent ) {
          return new Shell( parent, SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
        }
      };
    showHelpDialog.open();
    showHelpDialog.dispose();
  }
}
