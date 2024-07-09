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
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.ClusterSettingsPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.KerberosSettingsPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.KnoxSettingsPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.ReportPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.SecuritySettingsPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.TestResultsPage;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.BadSiteFilesException;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.CustomWizardDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.CachedFileItemStream;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.big.data.plugins.common.ui.ClusterTestDialog;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages.SecuritySettingsPage.NamedClusterSecurityType.NONE;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.processSiteFiles;

/*
 * To run this dialog as stand alone for development purposes under UBUNTU do the following:
 * 1.Look for the following comment in the module:
 *   FOR UI EXECUTION AS A STANDALONE
 *   And either comment or uncomment the referred section as requested
 * 2.Execute running the following command at the root of the "ui" submodule:
 *   mvn clean compile exec:java
 *
 * TO DEBUG
 * mvn clean compile exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath -Xdebug
 * -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 org.pentaho.big.data.kettle.plugins.hadoopcluster.ui
 * .dialog.wizard.NamedClusterDialog"
 * */

public class NamedClusterDialog extends Wizard {

  private String dialogState;
  private boolean isEditMode;
  private boolean isDuplicating;
  private ClusterSettingsPage clusterSettingsPage;
  private SecuritySettingsPage securitySettingsPage;
  private KerberosSettingsPage kerberosSettingsPage;
  private KnoxSettingsPage knoxSettingsPage;
  private ReportPage reportPage;
  private TestResultsPage testResultsPage;
  private final HadoopClusterManager hadoopClusterManager;
  private ThinNameClusterModel thinNameClusterModel;
  private boolean isDevMode = false;
  private final RuntimeTester runtimeTester;
  private final VariableSpace variableSpace;
  private final Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private static final Class<?> PKG = ClusterSettingsPage.class;
  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "NamedClusterDialog" );

  public NamedClusterDialog( NamedClusterService namedClusterService, IMetaStore metastore, VariableSpace variables,
                             RuntimeTester tester, Map<String, String> params, String dialogState ) {
    setWindowTitle( BaseMessages.getString( PKG, "NamedClusterDialog.newCluster" )  );
    variableSpace = variables;
    runtimeTester = tester;
    hadoopClusterManager = new HadoopClusterManager( spoonSupplier.get(), namedClusterService, metastore, "" );
    String namedClusterNameParam = params.get( "name" );
    isEditMode = namedClusterNameParam != null;
    thinNameClusterModel = createModel( hadoopClusterManager.getNamedCluster( namedClusterNameParam ) );
    String duplicateNamedClusterParam = params.get( "duplicateName" );
    if ( duplicateNamedClusterParam != null ) {
      thinNameClusterModel.setOldName( thinNameClusterModel.getName() );
      thinNameClusterModel.setName( "copy-of-" + thinNameClusterModel.getName() );
      isEditMode = false;
      isDuplicating = true;
    }
    this.dialogState = dialogState;
  }

  public boolean isConnectedToRepo() {
    boolean isConnectedToRepo = false;
    if ( spoonSupplier.get() != null ) {
      Repository repo = spoonSupplier.get().getRepository();
      isConnectedToRepo = repo != null && repo.getUri().isPresent();
    }
    return isConnectedToRepo;
  }

  private ThinNameClusterModel createModel( ThinNameClusterModel model ) {
    if ( model == null ) {
      model = new ThinNameClusterModel();
    }
    model.setName( model.getName() == null ? "" : model.getName() );
    model.setShimVendor( model.getShimVendor() == null ? "" : model.getShimVendor() );
    model.setShimVersion( model.getShimVersion() == null ? "" : model.getShimVersion() );
    model.setHdfsHost( model.getHdfsHost() == null ? "" : model.getHdfsHost() );
    model.setHdfsPort( model.getHdfsPort() == null ? "8020" : model.getHdfsPort() );
    model.setHdfsUsername( model.getHdfsUsername() == null ? "" : model.getHdfsUsername() );
    model.setHdfsPassword( model.getHdfsPassword() == null ? "" : model.getHdfsPassword() );
    model.setJobTrackerHost( model.getJobTrackerHost() == null ? "" : model.getJobTrackerHost() );
    model.setJobTrackerPort( model.getJobTrackerPort() == null ? "8032" : model.getJobTrackerPort() );
    model.setZooKeeperHost( model.getZooKeeperHost() == null ? "" : model.getZooKeeperHost() );
    model.setZooKeeperPort( model.getZooKeeperPort() == null ? "2181" : model.getZooKeeperPort() );
    model.setOozieUrl( model.getOozieUrl() == null ? "" : model.getOozieUrl() );
    model.setKafkaBootstrapServers( model.getKafkaBootstrapServers() == null ? "" : model.getKafkaBootstrapServers() );
    model.setOldName( model.getName() );
    model.setSecurityType( model.getSecurityType() == null ? "None" : model.getSecurityType() );
    model.setKerberosSubType( model.getKerberosSubType() == null ? "Password" : model.getKerberosSubType() );
    model.setKerberosAuthenticationUsername(
      model.getKerberosAuthenticationUsername() == null ? "" : model.getKerberosAuthenticationUsername() );
    model.setKerberosAuthenticationPassword(
      model.getKerberosAuthenticationPassword() == null ? "" : model.getKerberosAuthenticationPassword() );
    model.setKerberosImpersonationUsername(
      model.getKerberosImpersonationUsername() == null ? "" : model.getKerberosImpersonationUsername() );
    model.setKerberosImpersonationPassword(
      model.getKerberosImpersonationPassword() == null ? "" : model.getKerberosImpersonationPassword() );
    model.setGatewayUrl( model.getGatewayUrl() == null ? "" : model.getGatewayUrl() );
    model.setGatewayUsername( model.getGatewayUsername() == null ? "" : model.getGatewayUsername() );
    model.setGatewayPassword( model.getGatewayPassword() == null ? "" : model.getGatewayPassword() );
    model.setKeytabImpFile( model.getKeytabImpFile() == null ? "" : model.getKeytabImpFile() );
    model.setKeytabAuthFile(
      model.getKeytabAuthFile() == null ? BaseMessages.getString( PKG, "NamedClusterDialog.noFileSelected" ) :
        model.getKeytabAuthFile() );
    model.setSiteFiles( model.getSiteFiles() == null ? new ArrayList<>() : model.getSiteFiles() );
    return model;
  }

  public void initialize( ThinNameClusterModel model ) {
    if ( !dialogState.equals( "testing" ) ) {
      thinNameClusterModel = model == null ? createModel( null ) : createModel( model );
      clusterSettingsPage.initialize( thinNameClusterModel );
      securitySettingsPage.initialize( thinNameClusterModel );
      knoxSettingsPage.initialize( thinNameClusterModel );
      kerberosSettingsPage.initialize( thinNameClusterModel );
      reportPage.initialize( thinNameClusterModel );
      testResultsPage.initialize( thinNameClusterModel );
    } else {
      try {
        testResultsPage.initialize( model );
        testResultsPage.setTestResults( getTestResults() );
      } catch ( KettleException e ) {
        log.logError( e.getMessage() );
      }
    }
  }

  public void addPages() {
    if ( !dialogState.equals( "testing" ) ) {
      clusterSettingsPage =
        new ClusterSettingsPage( variableSpace, thinNameClusterModel );
      addPage( clusterSettingsPage );
      securitySettingsPage = new SecuritySettingsPage( thinNameClusterModel );
      addPage( securitySettingsPage );
      knoxSettingsPage = new KnoxSettingsPage( variableSpace, thinNameClusterModel );
      addPage( knoxSettingsPage );
      kerberosSettingsPage = new KerberosSettingsPage( variableSpace, thinNameClusterModel );
      addPage( kerberosSettingsPage );
      reportPage = new ReportPage( thinNameClusterModel );
      addPage( reportPage );
      testResultsPage = new TestResultsPage( variableSpace, thinNameClusterModel );
      addPage( testResultsPage );
    } else {
      testResultsPage = new TestResultsPage( variableSpace, thinNameClusterModel );
      addPage( testResultsPage );
    }
  }

  public void editCluster() {
    dialogState = "new-edit";
    ThinNameClusterModel model = hadoopClusterManager.getNamedCluster( thinNameClusterModel.getName() );
    if ( model != null ) {
      isEditMode = true;
      isDuplicating = false;
      initialize( model );
    } else {
      isEditMode = false;
      isDuplicating = false;
    }
    getContainer().showPage( getPage( ClusterSettingsPage.class.getSimpleName() ) );
  }

  public void createNewCluster() {
    dialogState = "new-edit";
    isEditMode = false;
    isDuplicating = false;
    initialize( null );
    getContainer().showPage( getPage( ClusterSettingsPage.class.getSimpleName() ) );
  }

  public boolean performFinish() {
    boolean finish = false;
    String currentPage = super.getContainer().getCurrentPage().getName();
    if ( !currentPage.equals( reportPage.getClass().getSimpleName() ) ) {
      if ( isEditMode || isDuplicating ) {
        saveEditedNamedCluster();
      } else {
        saveNewNamedCluster();
      }
    } else {
      finish = true;
    }
    if ( spoonSupplier.get() != null ) {
      spoonSupplier.get().refreshTree( BaseMessages.getString( PKG, "HadoopClusterTree.Title" ) );
    }
    return finish;
  }

  private void saveNewNamedCluster() {
    try {
      Map<String, CachedFileItemStream> siteFiles = processSiteFiles( thinNameClusterModel, hadoopClusterManager );
      if ( dialogState.equals( "new-edit" ) ) {
        hadoopClusterManager.createNamedCluster( thinNameClusterModel, siteFiles );
      }
      if ( dialogState.equals( "import" ) ) {
        hadoopClusterManager.importNamedCluster( thinNameClusterModel, siteFiles );
      }
      reportPage.setTestResults( getTestResults() );
    } catch ( BadSiteFilesException e ) {
      reportPage.setTestResult( BaseMessages.getString( PKG, "NamedClusterDialog.test.importFailed" ) );
    } catch ( IOException | KettleException e ) {
      log.logError( e.getMessage() );
    }
    getContainer().showPage( reportPage );
  }

  private void saveEditedNamedCluster() {
    try {
      Map<String, CachedFileItemStream> siteFiles = processSiteFiles( thinNameClusterModel, hadoopClusterManager );
      hadoopClusterManager.editNamedCluster( thinNameClusterModel, isEditMode, siteFiles );
      reportPage.setTestResults( getTestResults() );
    } catch ( BadSiteFilesException e ) {
      reportPage.setTestResult( BaseMessages.getString( PKG, "NamedClusterDialog.test.importFailed" ) );
    } catch ( IOException | KettleException e ) {
      log.logError( e.getMessage() );
    }
    getContainer().showPage( reportPage );
  }

  private Object[] getTestResults() throws KettleException {
    NamedCluster namedCluster = hadoopClusterManager.getNamedClusterByName( thinNameClusterModel.getName() );
    if ( isDevMode() ) {
      if ( !dialogState.equals( "testing" ) ) {
        return (Object[]) hadoopClusterManager.runTests( runtimeTester, thinNameClusterModel.getName() );
      } else {
        return new Object[] {};
      }
    } else {
      RuntimeTestStatus runtimeTestStatus =
        ClusterTestDialog.create( spoonSupplier.get().getShell(), namedCluster, runtimeTester ).open();
      return hadoopClusterManager.produceTestCategories( runtimeTestStatus, namedCluster );
    }
  }

  public boolean canFinish() {
    // Hack to style the CustomWizardDialog.
    ( (CustomWizardDialog) getContainer() ).style();
    // Couldn't be done elsewhere because the "TestResultsPage" was not initialized by the wizard.

    String currentPage = super.getContainer().getCurrentPage().getName();
    if ( !dialogState.equals( "testing" ) ) {
      if ( currentPage.equals( clusterSettingsPage.getClass().getSimpleName() ) ) {
        ( (CustomWizardDialog) getContainer() ).enableCancelButton( true );
      }
      if ( currentPage.equals( reportPage.getClass().getSimpleName() ) ) {
        ( (CustomWizardDialog) getContainer() ).enableCancelButton( false );
      }
      return
        ( currentPage.equals( securitySettingsPage.getClass().getSimpleName() )
          && securitySettingsPage.getSecurityType()
          .equals( NONE ) ) || ( currentPage.equals( kerberosSettingsPage.getClass().getSimpleName() )
          && kerberosSettingsPage.isPageComplete() || (
          currentPage.equals( knoxSettingsPage.getClass().getSimpleName() )
            && knoxSettingsPage.isPageComplete() ) || currentPage.equals( reportPage.getClass().getSimpleName() ) );
    } else {
      // Set to Initialize "TestResultsPage" when "dialogState" is "testing" and disable its "Finish" button.
      // Couldn't be done elsewhere because the "TestResultsPage" was not initialized by the wizard.
      initialize( thinNameClusterModel );
      return false;
    }
  }

  public String getDialogState() {
    return dialogState;
  }

  public void setDevMode( boolean devMode ) {
    this.isDevMode = devMode;
  }

  public boolean isDevMode() {
    return isDevMode;
  }

  public boolean isEditMode() {
    return isEditMode;
  }

  public static void main( String[] args ) {
    try {
      PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
      PluginRegistry.init( false );
      Encr.init( "Kettle" );
      KettleLogStore.init();
      Display display = new Display();
      Shell shell = new Shell( display );
      PropsUI.init( display, Props.TYPE_PROPERTIES_SPOON );
      NamedClusterDialog namedClusterDialog =
        new NamedClusterDialog( NamedClusterManager.getInstance(), MetaStoreConst.openLocalPentahoMetaStore(),
          new Variables(), RuntimeTesterImpl.getInstance(), new HashMap<String, String>(), "new-edit" );
      namedClusterDialog.setDevMode( true );
      CustomWizardDialog namedClusterWizardDialog = new CustomWizardDialog( shell, namedClusterDialog );
      namedClusterWizardDialog.open();
    } catch ( Exception e ) {
      log.logError( e.getMessage() );
    }
  }
}
