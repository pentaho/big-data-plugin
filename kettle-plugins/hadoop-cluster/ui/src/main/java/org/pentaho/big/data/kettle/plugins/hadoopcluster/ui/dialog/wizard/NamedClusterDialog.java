/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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
 * To run this dialog as stand alone for development purposes do the following:
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
 *
 * */

public class NamedClusterDialog extends Wizard {

  private boolean isEditMode = false;
  private ClusterSettingsPage clusterSettingsPage;
  private SecuritySettingsPage securitySettingsPage;
  private KerberosSettingsPage kerberosSettingsPage;
  private KnoxSettingsPage knoxSettingsPage;
  private ReportPage reportPage;
  private TestResultsPage testResultsPage;
  private final HadoopClusterManager hadoopClusterManager;
  private ThinNameClusterModel thinNameClusterModel;
  private final RuntimeTester runtimeTester;
  private final VariableSpace variableSpace;
  private final Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private static final Class<?> PKG = ClusterSettingsPage.class;
  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "NamedClusterDialog" );

  private boolean isDevMode = false;

  public NamedClusterDialog( NamedClusterService namedClusterService, IMetaStore metastore, VariableSpace variables,
                             RuntimeTester tester, Map<String, String> params ) {
    variableSpace = variables;
    runtimeTester = tester;
    hadoopClusterManager = new HadoopClusterManager( spoonSupplier.get(), namedClusterService, metastore, "" );
    thinNameClusterModel = createModel( hadoopClusterManager.getNamedCluster( params.get( "name" ) ) );
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
    model.setOldName( model.getOldName() == null ? "" : model.getOldName() );
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
    thinNameClusterModel = model == null ? createModel( null ) : createModel( model );
    clusterSettingsPage.initialize( thinNameClusterModel );
    securitySettingsPage.initialize( thinNameClusterModel );
    knoxSettingsPage.initialize( thinNameClusterModel );
    kerberosSettingsPage.initialize( thinNameClusterModel );
    reportPage.initialize( thinNameClusterModel );
    testResultsPage.initialize( thinNameClusterModel );
  }

  public void addPages() {
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
  }

  public void editCluster() {
    ThinNameClusterModel model = hadoopClusterManager.getNamedCluster( thinNameClusterModel.getName() );
    if ( model != null ) {
      isEditMode = true;
      model.setOldName( thinNameClusterModel.getName() );
      initialize( model );
    } else {
      isEditMode = false;
    }
    getContainer().showPage( getPage( ClusterSettingsPage.class.getSimpleName() ) );
  }

  public void createNewCluster() {
    isEditMode = false;
    initialize( null );
    getContainer().showPage( getPage( ClusterSettingsPage.class.getSimpleName() ) );
  }

  public boolean performFinish() {
    if ( isEditMode ) {
      saveEditedNamedCluster();
    } else {
      saveNewNamedCluster();
    }
    if ( spoonSupplier.get() != null ) {
      spoonSupplier.get().refreshTree( BaseMessages.getString( PKG, "HadoopClusterTree.Title" ) );
    }
    return false;
  }

  private void saveNewNamedCluster() {
    try {
      Map<String, CachedFileItemStream> siteFiles = processSiteFiles( thinNameClusterModel, hadoopClusterManager );
      hadoopClusterManager.createNamedCluster( thinNameClusterModel, siteFiles );
      processTestResults();
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
      hadoopClusterManager.editNamedCluster( thinNameClusterModel, true, siteFiles );
      processTestResults();
    } catch ( BadSiteFilesException e ) {
      reportPage.setTestResult( BaseMessages.getString( PKG, "NamedClusterDialog.test.importFailed" ) );
    } catch ( IOException | KettleException e ) {
      log.logError( e.getMessage() );
    }
    getContainer().showPage( reportPage );
  }

  private void processTestResults() throws KettleException {
    NamedCluster namedCluster = hadoopClusterManager.getNamedClusterByName( thinNameClusterModel.getName() );
    RuntimeTestStatus runtimeTestStatus = ClusterTestDialog.create( getShell(), namedCluster, runtimeTester ).open();
    Object[] testResults = hadoopClusterManager.produceTestCategories( runtimeTestStatus, namedCluster );
    reportPage.processTestResults( testResults );
  }

  public boolean canFinish() {
    ( (CustomWizardDialog) getContainer() ).style();
    String currentPage = super.getContainer().getCurrentPage().getName();
    return
      ( currentPage.equals( securitySettingsPage.getClass().getSimpleName() ) && securitySettingsPage.getSecurityType()
        .equals( NONE ) ) || ( currentPage.equals( kerberosSettingsPage.getClass().getSimpleName() )
        && kerberosSettingsPage.isPageComplete() || ( currentPage.equals( knoxSettingsPage.getClass().getSimpleName() )
        && knoxSettingsPage.isPageComplete() ) );
  }

  public void setDevMode( boolean devMode ) {
    this.isDevMode = devMode;
  }

  public boolean isDevMode() {
    return isDevMode;
  }
}
