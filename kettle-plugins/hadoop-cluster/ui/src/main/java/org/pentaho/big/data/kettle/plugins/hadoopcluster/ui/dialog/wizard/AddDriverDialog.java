/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard;

import org.eclipse.jface.wizard.Wizard;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
//import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.AddDriverPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.AddDriverResultPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.CustomWizardDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
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
  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "AddDriverDialog" );

  public AddDriverDialog( VariableSpace variables, NamedClusterService namedClusterService, IMetaStore metastore ) {
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
