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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard;

import org.eclipse.jface.wizard.Wizard;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
//import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.AddDriverPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.AddDriverResultPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.ClusterSettingsPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.CustomWizardDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;

import java.util.function.Supplier;

public class AddDriverDialog extends Wizard {

  private AddDriverPage addDriverPage;
  private AddDriverResultPage addDriverResultPage;
  private final VariableSpace variableSpace;
  private final HadoopClusterManager hadoopClusterManager;
  private final Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private static final Class<?> PKG = ClusterSettingsPage.class;
  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "AddDriverDialog" );

  public AddDriverDialog( VariableSpace variables, NamedClusterService namedClusterService, IMetaStore metastore ) {
    setWindowTitle( BaseMessages.getString( PKG, "NamedClusterDialog.newCluster" ) );
    variableSpace = variables;
    hadoopClusterManager = new HadoopClusterManager( spoonSupplier.get(), namedClusterService, metastore, "" );
  }

  public void addPages() {
    addDriverPage = new AddDriverPage( variableSpace );
    addPage( addDriverPage );
    addDriverResultPage = new AddDriverResultPage( variableSpace );
    addPage( addDriverResultPage );
  }

  public boolean importDriver( String driverFile ) {
    boolean result = false;
    try {
      result = NamedClusterHelper.processDriverFile( driverFile, hadoopClusterManager );
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
    return result;
  }

  public boolean canFinish() {
    // Hack to style the CustomWizardDialog.
    ( (CustomWizardDialog) getContainer() ).style();
    // Couldn't be done elsewhere because the "TestResultsPage" was not initialized by the wizard.

    String currentPage = super.getContainer().getCurrentPage().getName();
    if ( currentPage.equals( addDriverPage.getClass().getSimpleName() ) ) {
      ( (CustomWizardDialog) getContainer() ).enableCancelButton( true );
    }
    if ( currentPage.equals( addDriverResultPage.getClass().getSimpleName() ) ) {
      ( (CustomWizardDialog) getContainer() ).enableCancelButton( false );
    }
    return addDriverPage.isPageComplete();
  }

  public boolean performFinish() {
    String currentPage = super.getContainer().getCurrentPage().getName();
    if ( currentPage.equals( addDriverPage.getClass().getSimpleName() ) ) {
      boolean result = importDriver( addDriverPage.getFileName() );
      addDriverResultPage.setImportResult( result );
      getContainer().showPage( addDriverResultPage );
    }
    return currentPage.equals( addDriverResultPage.getClass().getSimpleName() );
  }

  /*
  public static void main( String[] args ) {
    try {
      KettleLogStore.init();
      Display display = new Display();
      Shell shell = new Shell( display );
      PropsUI.init( display, Props.TYPE_PROPERTIES_SPOON );
      AddDriverDialog addDriverDialog =
        new AddDriverDialog( new Variables(), NamedClusterManager.getInstance(),
          MetaStoreConst.openLocalPentahoMetaStore() );
      CustomWizardDialog wizardDialog = new CustomWizardDialog( shell, addDriverDialog );
      wizardDialog.open();
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
  }
  */
}
