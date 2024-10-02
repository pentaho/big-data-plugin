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
package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages;

import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.NamedClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.Test;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.TestCategory;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.util.HelpUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabel;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabelWithStyle;

public class ReportPage extends WizardPage {

  private PropsUI props;
  private Composite basePanel;
  private Composite parent;
  private Composite mainPanel;
  private Label statusLabel;
  private Label statusDescriptionLabel;
  private Label iconLabel;
  private Button viewTestResultsButton;
  private Object[] testResults;
  private ThinNameClusterModel thinNameClusterModel;
  private static final Class<?> PKG = ReportPage.class;
  private static final String SUCCESS_IMG = "images/success.svg";
  private static final String FAIL_IMG = "images/fail.svg";

  public ReportPage( ThinNameClusterModel model ) {
    super( ReportPage.class.getSimpleName() );
    thinNameClusterModel = model;
  }

  public void createControl( Composite composite ) {
    parent = new Composite( composite, SWT.NONE );
    props = PropsUI.getInstance();
    props.setLook( parent );
    GridLayout gridLayout = new GridLayout( ONE_COLUMN, false );
    parent.setLayout( gridLayout );
    basePanel = new Composite( parent, SWT.NONE );

    //START OF MAIN LAYOUT
    GridLayout baseGridLayout = new GridLayout( ONE_COLUMN, false );
    baseGridLayout.marginWidth = 60; //TO CENTER CONTENTS
    baseGridLayout.marginTop = 10; //TO CENTER CONTENTS
    baseGridLayout.marginBottom = 30;
    baseGridLayout.marginLeft = 20;
    basePanel.setLayout( baseGridLayout );
    GridData basePanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    basePanel.setLayoutData( basePanelGridData );
    props.setLook( basePanel );
    //END OF MAIN LAYOUT

    mainPanel = new Composite( basePanel, SWT.NONE );
    mainPanel.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData mainPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    mainPanelGridData.heightHint = 510; //Height of the panel (WILL NEED TO ADJUST)
    mainPanel.setLayoutData( mainPanelGridData );
    props.setLook( mainPanel );

    GridData iconGridData = new GridData();
    iconGridData.widthHint = 400; // Label width
    iconGridData.heightHint = 100; // Label height
    iconLabel = createLabelWithStyle( mainPanel, "", iconGridData, props, SWT.NONE );
    iconLabel.setAlignment( SWT.CENTER );

    GridData statusGridData = new GridData();
    statusGridData.widthHint = 400; // Label width
    statusGridData.heightHint = 50; // Label height
    statusLabel = createLabelWithStyle( mainPanel, "", statusGridData, props, SWT.NONE );
    statusLabel.setFont( new Font( statusLabel.getDisplay(), new FontData( "Arial", 20, SWT.NONE ) ) );
    statusLabel.setAlignment( SWT.CENTER );

    GridData statusDescriptionGridData = new GridData();
    statusDescriptionGridData.widthHint = 400; // Label width
    statusDescriptionGridData.heightHint = 100; // Label height
    statusDescriptionLabel = createLabelWithStyle( mainPanel, "", statusDescriptionGridData, props, SWT.WRAP );
    statusDescriptionLabel.setAlignment( SWT.CENTER );

    GridData questonLabelGridData = new GridData();
    questonLabelGridData.widthHint = 400; // Label width
    questonLabelGridData.heightHint = 50; // Label height
    createLabel( mainPanel, BaseMessages.getString( PKG, "NamedClusterDialog.question" ), questonLabelGridData,
      props ).setAlignment( SWT.CENTER );

    Button editClusterButton = new Button( mainPanel, SWT.PUSH );
    GridData editButtonGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    editButtonGridData.widthHint = 150;
    editButtonGridData.horizontalAlignment = SWT.CENTER;
    editClusterButton.setLayoutData( editButtonGridData );
    editClusterButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.editCluster" ) );
    Listener editClusterListener = e -> editCluster();
    editClusterButton.addListener( SWT.Selection, editClusterListener );
    props.setLook( editClusterButton );

    Button newClusterButton = new Button( mainPanel, SWT.PUSH );
    GridData newButtonGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    newButtonGridData.widthHint = 150;
    newButtonGridData.horizontalAlignment = SWT.CENTER;
    newClusterButton.setLayoutData( newButtonGridData );
    newClusterButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.createNewCluster" ) );
    Listener newClusterListener = e -> createNewCluster();
    newClusterButton.addListener( SWT.Selection, newClusterListener );
    props.setLook( newClusterButton );

    viewTestResultsButton = new Button( mainPanel, SWT.PUSH );
    GridData viewTestResultsButtonGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    viewTestResultsButtonGridData.widthHint = 150;
    viewTestResultsButtonGridData.horizontalAlignment = SWT.CENTER;
    viewTestResultsButton.setLayoutData( viewTestResultsButtonGridData );
    viewTestResultsButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.viewTestResults" ) );
    Listener viewTestResultsListener = e -> viewTestResults();
    viewTestResultsButton.addListener( SWT.Selection, viewTestResultsListener );
    props.setLook( viewTestResultsButton );

    setControl( parent );
    initialize( thinNameClusterModel );
  }

  public void setTestResult( String status ) {
    if ( status.equals( BaseMessages.getString( PKG, "NamedClusterDialog.test.pass" ) ) ) {
      statusLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.pass" ) );
      statusDescriptionLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.description.pass" ) );
      iconLabel.setImage(
        GUIResource.getInstance().getImage( SUCCESS_IMG, getClass().getClassLoader(), 70, 70 ) );
      viewTestResultsButton.setVisible( true );
    } else if ( status.equals( BaseMessages.getString( PKG, "NamedClusterDialog.test.importFailed" ) ) ) {
      statusLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.import.fail" ) );
      statusDescriptionLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.import.fail.description" ) );
      iconLabel.setImage(
        GUIResource.getInstance().getImage( FAIL_IMG, getClass().getClassLoader(), 70, 70 ) );
      viewTestResultsButton.setVisible( false );

    } else {
      statusLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.fail" ) );
      statusDescriptionLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.fail.description" ) );
      iconLabel.setImage(
        GUIResource.getInstance().getImage( FAIL_IMG, getClass().getClassLoader(), 70, 70 ) );
      viewTestResultsButton.setVisible( true );
    }
    mainPanel.pack();
  }

  public void setTestResults( Object[] categories ) {
    testResults = categories;
    String status = "";
    for ( Object category : testResults ) {
      TestCategory testCategory = (TestCategory) category;
      List<Test> tests =
        testCategory.getTests().stream().filter( test -> test.getTestName()
            .equals( BaseMessages.getString( PKG, "NamedClusterDialog.test.hadoopFileSystemConnection" ) ) )
          .collect( Collectors.toList() );
      if ( !tests.isEmpty() ) {
        status = tests.get( 0 ).getTestStatus();
      }
    }
    setTestResult( status );
  }

  // FOR DEV MODE ONLY
  private boolean isDevMode() {
    NamedClusterDialog namedClusterDialog = (NamedClusterDialog) getWizard();
    return namedClusterDialog.isDevMode();
  }
  // FOR DEV MODE ONLY

  private boolean isConnectedToRepo() {
    NamedClusterDialog namedClusterDialog = (NamedClusterDialog) getWizard();
    boolean isConnectedToRepo = namedClusterDialog.isConnectedToRepo();
    if ( isDevMode() ) {
      isConnectedToRepo = true;
    }
    return isConnectedToRepo;
  }

  public void initialize( ThinNameClusterModel model ) {
    setTitle( ( (NamedClusterDialog) getWizard() ).isEditMode() ?
      BaseMessages.getString( PKG, "NamedClusterDialog.editCluster.title" ) :
      BaseMessages.getString( PKG, "NamedClusterDialog.newCluster.title" ) );

    if ( isConnectedToRepo() ) {
      setDescription( BaseMessages.getString( PKG, "NamedClusterDialog.repositoryNotification" ) );
    }

    thinNameClusterModel = model;
  }

  private void viewTestResults() {
    TestResultsPage testResultsPage = (TestResultsPage) getWizard().getPage( TestResultsPage.class.getSimpleName() );
    testResultsPage.setTestResults( testResults );
    getContainer().showPage( testResultsPage );
  }

  private void editCluster() {
    NamedClusterDialog namedClusterDialog = (NamedClusterDialog) getWizard();
    namedClusterDialog.editCluster();
  }

  private void createNewCluster() {
    NamedClusterDialog namedClusterDialog = (NamedClusterDialog) getWizard();
    namedClusterDialog.createNewCluster();
  }

  public IWizardPage getPreviousPage() {
    return null;
  }

  public IWizardPage getNextPage() {
    return null;
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", BaseMessages.getString( PKG, "NamedClusterDialog.help" ), "" );
  }
}