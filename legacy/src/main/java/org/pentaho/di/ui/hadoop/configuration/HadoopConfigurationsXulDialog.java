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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.hadoop.HadoopConfigurationInfo;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ShowHelpDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.XulSpoonResourceBundle;
import org.pentaho.di.ui.spoon.XulSpoonSettingsManager;
import org.pentaho.di.ui.xul.KettleXulLoader;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulRunner;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.containers.XulListbox;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.swt.SwtXulRunner;
import org.pentaho.ui.xul.swt.tags.SwtButton;
import org.pentaho.ui.xul.swt.tags.SwtDialog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
    this.shell = aShell;
    this.hadoopConfigurationInfos = new ArrayList<>( hadoopConfigurationInfos );
    Collections.sort( this.hadoopConfigurationInfos, new Comparator<HadoopConfigurationInfo>() {
      @Override public int compare( HadoopConfigurationInfo o1, HadoopConfigurationInfo o2 ) {
        int result = o1.getName().compareTo( o2.getName() );
        if ( result == 0 ) {
          result = o1.getId().compareTo( o2.getId() );
        }
        return result;
      }
    } );
    setName( CONTROLLER_NAME );
  }

  public String open() {
    String selectedShim = "";
    try {
      KettleXulLoader e = new KettleXulLoader();
      e.setOuterContext( shell );
      e.setIconsSize( 24, 24 );
      e.setSettingsManager( XulSpoonSettingsManager.getInstance() );
      e.registerClassLoader( getClass().getClassLoader() );
      e.registerClassLoader( KettleXulLoader.class.getClassLoader() );
      XulDomContainer container = e.loadXul( XUL, new XulSpoonResourceBundle( PKG ) );
      container.addEventHandler( this );
      XulRunner runner = new SwtXulRunner();
      runner.addContainer( container );
      runner.initialize();

      SwtButton helpButton = (SwtButton) container.getDocumentRoot().getElementById( "helpButton" );
      Button managedObject = (Button) helpButton.getManagedObject();
      managedObject.setImage( GUIResource.getInstance().getImageHelpWeb() );

      selectDialog = (XulDialog) container.getDocumentRoot().getElementById( "hadoopConfigurationSelectionDialog" );
      XulListbox hadoopConfigurationSelectionDialogMenuListBox =
        (XulListbox) container.getDocumentRoot().getElementById(
          "hadoopConfigurationSelectionDialogMenuListBox" );
      int selectedIndex = -1;
      List<String> configs = new ArrayList<>();
      boolean previousConfigHadSameName = false;
      for ( int i = 0; i < hadoopConfigurationInfos.size(); i++ ) {
        HadoopConfigurationInfo hadoopConfigurationInfo = hadoopConfigurationInfos.get( i );
        if ( hadoopConfigurationInfo.isWillBeActiveAfterRestart() ) {
          selectedIndex = i;
        }
        boolean nextConfigHasSameName =
          i < hadoopConfigurationInfos.size() - 1 && hadoopConfigurationInfos.get( i + 1 ).getName()
            .equals( hadoopConfigurationInfo.getName() );
        if ( previousConfigHadSameName || nextConfigHasSameName ) {
          configs.add( hadoopConfigurationInfo.getName() + " (" + hadoopConfigurationInfo.getId() + ")" );
        } else {
          configs.add( hadoopConfigurationInfo.getName() );
        }
        previousConfigHadSameName = nextConfigHasSameName;
      }
      hadoopConfigurationSelectionDialogMenuListBox.setElements( configs );
      if ( selectedIndex >= 0 ) {
        hadoopConfigurationSelectionDialogMenuListBox.setSelectedIndex( selectedIndex );
      }
      hadoopConfigurationSelectionDialogMenuListBox.setRows( 6 );
      selectDialog.show();
      if ( accept ) {
        selectedShim =
          hadoopConfigurationInfos.get( hadoopConfigurationSelectionDialogMenuListBox.getSelectedIndex() ).getId();
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
    String docUrl =
        Const.getDocUrl( BaseMessages.getString( PKG, "HadoopConfigurationSelectionDialog.Help.Url" ) );

    ShowHelpDialog showHelpDialog = new ShowHelpDialog( shell,
        BaseMessages.getString( PKG, "HadoopConfigurationSelectionDialog.Help.Title" ),
        docUrl,
        BaseMessages.getString( PKG, "HadoopConfigurationSelectionDialog.Help.Header" ) ) {

      // Parent is modal so we have to be as well
      @Override
      protected Shell createShell( Shell parent ) {
        return new Shell( parent, SWT.CLOSE | SWT.RESIZE | SWT.MAX | SWT.MIN | SWT.APPLICATION_MODAL );
      }
    };

    showHelpDialog.open();
    showHelpDialog.dispose();
  }
}
