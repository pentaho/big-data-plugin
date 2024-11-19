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
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.NamedClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.util.HelpUtils;

import java.io.File;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.stream.Collectors;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.TWO_COLUMNS;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabel;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createText;

public class KerberosSettingsPage extends WizardPage {

  private PropsUI props;
  private Composite parent;
  private Composite mainPanel;
  private Composite passwordAuthenticationPanel;
  private Composite keytabAuthenticationPanel;
  private CCombo securityMethodCombo;
  private TextVar authenticationUserNameTextField;
  private TextVar authenticationPasswordTextField;
  private Text authenticationKeytabText;
  private TextVar impersonationUserNameTextField;
  private TextVar impersonationPasswordTextField;
  private Text impersonationKeytabText;
  private ThinNameClusterModel thinNameClusterModel;
  private final Listener clusterListener = e -> validate();
  private final VariableSpace variableSpace;
  private final String password = "Password";
  private final String keytab = "Keytab";
  private final String NO_FILE_SELECTED = BaseMessages.getString( PKG, "NamedClusterDialog.noFileSelected" );

  private static final Class<?> PKG = KerberosSettingsPage.class;

  public KerberosSettingsPage( VariableSpace variables, ThinNameClusterModel model ) {
    super( KerberosSettingsPage.class.getSimpleName() );
    variableSpace = variables;
    thinNameClusterModel = model;
    setPageComplete( false );
  }

  public void createControl( Composite composite ) {
    parent = new Composite( composite, SWT.NONE );
    props = PropsUI.getInstance();
    props.setLook( parent );
    GridLayout gridLayout = new GridLayout( ONE_COLUMN, false );
    parent.setLayout( gridLayout );

    Composite basePanel = new Composite( parent, SWT.NONE );

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

    GridData securityMethodLableGridData = new GridData();
    securityMethodLableGridData.widthHint = 400; // Label width
    createLabel( mainPanel, BaseMessages.getString( PKG, "NamedClusterDialog.securityMethod" ),
      securityMethodLableGridData,
      props );

    GridData securityMethodComboGridData = new GridData();
    securityMethodComboGridData.widthHint = 400; // TextField width
    securityMethodCombo = new CCombo( mainPanel, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    securityMethodCombo.setLayoutData( securityMethodComboGridData );
    securityMethodCombo.add( password );
    securityMethodCombo.add( keytab );

    Listener securityMethodComboListener = e -> displaySecurityMethodFields();
    securityMethodCombo.addListener( SWT.Selection, securityMethodComboListener );
    props.setLook( securityMethodCombo );
    setControl( parent );

    initialize( thinNameClusterModel );
  }

  private void displaySecurityMethodFields() {
    if ( securityMethodCombo.getText().equals( password ) ) {
      createPasswordAuthenticationFields();
      updatePasswordFields( thinNameClusterModel );
    }
    if ( securityMethodCombo.getText().equals( keytab ) ) {
      createKeytabAuthenticationFields();
      updateKeytabFields( thinNameClusterModel );
    }
    validate();
  }

  private void disposeComponents() {
    if ( passwordAuthenticationPanel != null ) {
      passwordAuthenticationPanel.dispose();
      passwordAuthenticationPanel = null;
    }
    if ( keytabAuthenticationPanel != null ) {
      keytabAuthenticationPanel.dispose();
      keytabAuthenticationPanel = null;
    }
    mainPanel.pack();
  }

  private void createPasswordAuthenticationFields() {
    disposeComponents();
    passwordAuthenticationPanel = new Composite( mainPanel, SWT.NONE );
    GridLayout authenticationPanelGridLayout = new GridLayout( TWO_COLUMNS, true );
    authenticationPanelGridLayout.marginWidth = 0;
    passwordAuthenticationPanel.setLayout( authenticationPanelGridLayout );
    GridData authenticationPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    passwordAuthenticationPanel.setLayoutData( authenticationPanelGridData );
    props.setLook( passwordAuthenticationPanel );

    GridData authenticationUsernameGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    createLabel( passwordAuthenticationPanel,
      BaseMessages.getString( PKG, "NamedClusterDialog.authenticationUsername" ),
      authenticationUsernameGridData, props );

    GridData authenticationPasswordGridData = new GridData();
    createLabel( passwordAuthenticationPanel, BaseMessages.getString( PKG, "NamedClusterDialog.password" ),
      authenticationPasswordGridData, props );

    GridData authenticationUserNameTextFieldGridData = new GridData();
    authenticationUserNameTextFieldGridData.widthHint = Const.isLinux() ? 197 : 200; // TextField width
    authenticationUserNameTextField =
      createText( passwordAuthenticationPanel, "", authenticationUserNameTextFieldGridData, props, variableSpace,
        clusterListener );

    GridData authenticationPasswordTextFieldGroupGridData = new GridData();
    authenticationPasswordTextFieldGroupGridData.widthHint = Const.isLinux() ? 197 : 200; // TextField width
    authenticationPasswordTextField =
      createText( passwordAuthenticationPanel, "", authenticationPasswordTextFieldGroupGridData, props, variableSpace,
        clusterListener );
    authenticationPasswordTextField.setEchoChar( '*' );

    if ( isConnectedToRepo() ) {
      GridData impersonationUsernameGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
      createLabel( passwordAuthenticationPanel,
        BaseMessages.getString( PKG, "NamedClusterDialog.impersonationUsername" ),
        impersonationUsernameGridData, props );

      GridData impersonationPasswordGridData = new GridData();
      createLabel( passwordAuthenticationPanel, BaseMessages.getString( PKG, "NamedClusterDialog.password" ),
        impersonationPasswordGridData, props );

      GridData impersonationUserNameTextFieldGridData = new GridData();
      impersonationUserNameTextFieldGridData.widthHint = Const.isLinux() ? 197 : 200; // TextField width
      impersonationUserNameTextField =
        createText( passwordAuthenticationPanel, "", impersonationUserNameTextFieldGridData, props, variableSpace,
          clusterListener );

      GridData impersonationPasswordTextFieldGroupGridData = new GridData();
      impersonationPasswordTextFieldGroupGridData.widthHint = Const.isLinux() ? 197 : 200; // TextField width
      impersonationPasswordTextField =
        createText( passwordAuthenticationPanel, "", impersonationPasswordTextFieldGroupGridData, props, variableSpace,
          clusterListener );
      impersonationPasswordTextField.setEchoChar( '*' );
    }

    mainPanel.pack();
  }

  private void createKeytabAuthenticationFields() {
    disposeComponents();
    keytabAuthenticationPanel = new Composite( mainPanel, SWT.NONE );
    GridLayout authenticationPanelGridLayout = new GridLayout( ONE_COLUMN, true );
    authenticationPanelGridLayout.marginWidth = 0;
    keytabAuthenticationPanel.setLayout( authenticationPanelGridLayout );
    GridData authenticationPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    keytabAuthenticationPanel.setLayoutData( authenticationPanelGridData );
    props.setLook( keytabAuthenticationPanel );

    GridData authenticationUsernameGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    createLabel( keytabAuthenticationPanel,
      BaseMessages.getString( PKG, "NamedClusterDialog.authenticationUsername" ),
      authenticationUsernameGridData, props );

    GridData authenticationUserNameTextFieldGridData = new GridData();
    authenticationUserNameTextFieldGridData.widthHint = Const.isLinux() ? 400 : 405; // TextField width
    authenticationUserNameTextField =
      createText( keytabAuthenticationPanel, "", authenticationUserNameTextFieldGridData, props, variableSpace,
        clusterListener );

    GridData authenticationPasswordGridData = new GridData();
    createLabel( keytabAuthenticationPanel, BaseMessages.getString( PKG, "NamedClusterDialog.authenticationKeytab" ),
      authenticationPasswordGridData, props );

    Composite authenticationKeytabPanel = new Composite( keytabAuthenticationPanel, SWT.NONE );
    GridLayout authenticationKeytabPanelGridLayout = new GridLayout( TWO_COLUMNS, true );
    authenticationKeytabPanelGridLayout.marginWidth = 0;
    authenticationKeytabPanel.setLayout( authenticationKeytabPanelGridLayout );
    GridData authenticationKeytabTextGridData = new GridData(); //new GridData(GridData.FILL_BOTH);
    props.setLook( authenticationKeytabPanel );

    authenticationKeytabText = new Text( authenticationKeytabPanel, SWT.BORDER );
    authenticationKeytabText.setEditable( false );
    authenticationKeytabTextGridData.widthHint = Const.isLinux() ? 310 : 341;
    authenticationKeytabText.setLayoutData( authenticationKeytabTextGridData );
    props.setLook( authenticationKeytabText );

    Button authenticationBrowseButton = new Button( authenticationKeytabPanel, SWT.PUSH );
    authenticationBrowseButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.browse" ) );
    props.setLook( authenticationBrowseButton );
    Listener authenticationBrowseListener = e -> authenticationBrowse();
    authenticationBrowseButton.addListener( SWT.Selection, authenticationBrowseListener );

    if ( isConnectedToRepo() ) {
      GridData impersonationUsernameGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
      createLabel( keytabAuthenticationPanel,
        BaseMessages.getString( PKG, "NamedClusterDialog.impersonationUsername" ),
        impersonationUsernameGridData, props );

      GridData impersonationUserNameTextFieldGridData = new GridData();
      impersonationUserNameTextFieldGridData.widthHint = Const.isLinux() ? 400 : 405; // TextField width
      impersonationUserNameTextField =
        createText( keytabAuthenticationPanel, "", impersonationUserNameTextFieldGridData, props, variableSpace,
          clusterListener );

      GridData impersonationPasswordGridData = new GridData();
      createLabel( keytabAuthenticationPanel, BaseMessages.getString( PKG, "NamedClusterDialog.impersonationKeytab" ),
        impersonationPasswordGridData, props );

      Composite impersonationKeytabPanel = new Composite( keytabAuthenticationPanel, SWT.NONE );
      GridLayout impersonationKeytabPanelGridLayout = new GridLayout( TWO_COLUMNS, true );
      impersonationKeytabPanelGridLayout.marginWidth = 0;
      impersonationKeytabPanel.setLayout( impersonationKeytabPanelGridLayout );
      GridData impersonationKeytabTextGridData = new GridData();
      props.setLook( impersonationKeytabPanel );

      impersonationKeytabText = new Text( impersonationKeytabPanel, SWT.BORDER );
      impersonationKeytabText.setEditable( false );
      impersonationKeytabTextGridData.widthHint = Const.isLinux() ? 310 : 341;
      impersonationKeytabText.setLayoutData( impersonationKeytabTextGridData );
      props.setLook( impersonationKeytabText );

      Button impersonationBrowseButton = new Button( impersonationKeytabPanel, SWT.PUSH );
      impersonationBrowseButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.browse" ) );
      props.setLook( impersonationBrowseButton );
      Listener impersonationBrowseListener = e -> impersonationBrowse();
      impersonationBrowseButton.addListener( SWT.Selection, impersonationBrowseListener );

      Button clearImpersonationButton = new Button( impersonationKeytabPanel, SWT.PUSH );
      clearImpersonationButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.clear" ) );
      props.setLook( clearImpersonationButton );
      Listener clearImpersonationListener = e -> clearImpersonation();
      clearImpersonationButton.addListener( SWT.Selection, clearImpersonationListener );
    }
    mainPanel.pack();
  }

  private void validate() {
    if ( securityMethodCombo.getText().equals( password ) ) {
      thinNameClusterModel.setKerberosSubType( password );
      thinNameClusterModel.setKerberosAuthenticationUsername( authenticationUserNameTextField.getText() );
      thinNameClusterModel.setKerberosAuthenticationPassword( authenticationPasswordTextField.getText() );
      if ( isConnectedToRepo() ) {
        thinNameClusterModel.setKerberosImpersonationUsername( impersonationUserNameTextField.getText() );
        thinNameClusterModel.setKerberosImpersonationPassword( impersonationPasswordTextField.getText() );
        setPageComplete( ( !thinNameClusterModel.getKerberosAuthenticationUsername().isBlank()
          && !thinNameClusterModel.getKerberosAuthenticationPassword().isBlank() )
          ||
          ( !thinNameClusterModel.getKerberosImpersonationUsername().isBlank()
            && !thinNameClusterModel.getKerberosImpersonationPassword().isBlank() ) );
      } else {
        setPageComplete( !thinNameClusterModel.getKerberosAuthenticationUsername().isBlank()
          && !thinNameClusterModel.getKerberosAuthenticationPassword().isBlank() );
      }
    }
    if ( securityMethodCombo.getText().equals( keytab ) ) {
      thinNameClusterModel.setKerberosSubType( keytab );
      thinNameClusterModel.setKerberosAuthenticationUsername( authenticationUserNameTextField.getText() );
      thinNameClusterModel.setKeytabAuthFile( authenticationKeytabText.getText()
        .equals( NO_FILE_SELECTED ) ? "" :
        authenticationKeytabText.getText() );
      if ( !thinNameClusterModel.getKeytabAuthFile().isBlank() ) {
        List<SimpleImmutableEntry<String, String>> siteFiles = thinNameClusterModel.getSiteFiles();
        List<SimpleImmutableEntry<String, String>> result =
          siteFiles.stream().filter( siteFile -> siteFile.getValue().equals( "keytabAuthFile" ) ).collect(
            Collectors.toList() );
        if ( !result.isEmpty() ) {
          siteFiles.remove( result.get( 0 ) );
        }
        siteFiles.add( new SimpleImmutableEntry<>( thinNameClusterModel.getKeytabAuthFile(), "keytabAuthFile" ) );
      }
      if ( isConnectedToRepo() ) {
        thinNameClusterModel.setKerberosImpersonationUsername( impersonationUserNameTextField.getText() );
        thinNameClusterModel.setKeytabImpFile( impersonationKeytabText.getText()
          .equals( NO_FILE_SELECTED ) ? "" :
          impersonationKeytabText.getText() );
        if ( !thinNameClusterModel.getKeytabImpFile().isBlank() ) {
          List<SimpleImmutableEntry<String, String>> siteFiles = thinNameClusterModel.getSiteFiles();
          List<SimpleImmutableEntry<String, String>> result =
            siteFiles.stream().filter( siteFile -> siteFile.getValue().equals( "keytabImpFile" ) ).collect(
              Collectors.toList() );
          if ( !result.isEmpty() ) {
            siteFiles.remove( result.get( 0 ) );
          }
          siteFiles.add( new SimpleImmutableEntry<>( thinNameClusterModel.getKeytabImpFile(), "keytabImpFile" ) );
        }
        setPageComplete( !thinNameClusterModel.getKeytabAuthFile().isBlank() );
      } else {
        setPageComplete( !thinNameClusterModel.getKeytabAuthFile().isBlank() );
      }
    }
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
      ( (NamedClusterDialog) getWizard() ).isImporting() ?
        BaseMessages.getString( PKG, "NamedClusterDialog.importCluster.title" ) :
        BaseMessages.getString( PKG, "NamedClusterDialog.newCluster.title" ) );

    if ( isConnectedToRepo() ) {
      setDescription( BaseMessages.getString( PKG, "NamedClusterDialog.repositoryNotification" ) );
    }

    thinNameClusterModel = model;
    securityMethodCombo.setText( model.getKerberosSubType() );
    if ( securityMethodCombo.getText().equals( password ) ) {
      createPasswordAuthenticationFields();
      updatePasswordFields( model );
    }
    if ( securityMethodCombo.getText().equals( keytab ) ) {
      createKeytabAuthenticationFields();
      updateKeytabFields( model );
    }
    validate();
  }

  private void updatePasswordFields( ThinNameClusterModel model ) {
    authenticationUserNameTextField.setText( model.getKerberosAuthenticationUsername() );
    authenticationPasswordTextField.setText( model.getKerberosAuthenticationPassword() );
    if ( isConnectedToRepo() ) {
      impersonationUserNameTextField.setText( model.getKerberosImpersonationUsername() );
      impersonationPasswordTextField.setText( model.getKerberosImpersonationPassword() );
    }
  }

  private void updateKeytabFields( ThinNameClusterModel model ) {
    authenticationKeytabText.setText(
      model.getKeytabAuthFile().isBlank() ? NO_FILE_SELECTED : model.getKeytabAuthFile() );
    authenticationUserNameTextField.setText( model.getKerberosAuthenticationUsername() );
    if ( isConnectedToRepo() ) {
      impersonationKeytabText.setText(
        model.getKeytabImpFile().isBlank() ? NO_FILE_SELECTED : model.getKeytabImpFile() );
      impersonationUserNameTextField.setText( model.getKerberosImpersonationUsername() );
    }
  }

  private void authenticationBrowse() {
    FileDialog dialog = new FileDialog( mainPanel.getShell(), SWT.OPEN );
    String path = dialog.open();
    if ( path != null ) {
      File file = new File( path );
      if ( file.isFile() ) {
        authenticationKeytabText.setText( file.toString() );
        validate();
      }
    }
  }

  private void impersonationBrowse() {
    FileDialog dialog = new FileDialog( mainPanel.getShell(), SWT.OPEN );
    String path = dialog.open();
    if ( path != null ) {
      File file = new File( path );
      if ( file.isFile() ) {
        impersonationKeytabText.setText( file.toString() );
        validate();
      }
    }
  }

  private void clearImpersonation() {
    impersonationKeytabText.setText( "" );
    validate();
  }

  public IWizardPage getNextPage() {
    return null;
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", BaseMessages.getString( PKG, "NamedClusterDialog.help" ), "" );
  }
}
