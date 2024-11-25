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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages;

import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
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

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.TWO_COLUMNS;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabel;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createText;

public class KnoxSettingsPage extends WizardPage {

  private PropsUI props;
  private Composite basePanel;
  private Composite parent;
  private Composite mainPanel;
  private Text gatewayURLTextField;
  private TextVar gatewayUsernameTextfield;
  private TextVar gatewayPasswordTextField;
  private final VariableSpace variableSpace;
  private final ThinNameClusterModel thinNameClusterModel;
  private final Listener clusterListener = e -> validate();

  private static final Class<?> PKG = KnoxSettingsPage.class;

  public KnoxSettingsPage( VariableSpace variables, ThinNameClusterModel model ) {
    super( KnoxSettingsPage.class.getSimpleName() );
    thinNameClusterModel = model;
    variableSpace = variables;
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

    GridData gatewayUrlLabelGridData = new GridData();
    gatewayUrlLabelGridData.widthHint = 400; // Label width
    createLabel( mainPanel, BaseMessages.getString( PKG, "NamedClusterDialog.gatewayURL" ),
      gatewayUrlLabelGridData,
      props );

    GridData gatewayUrlTextfieldGridData = new GridData();
    gatewayUrlTextfieldGridData.widthHint = Const.isLinux() ? 380 : 390; // TextField width
    gatewayURLTextField =
      new Text( mainPanel, SWT.SINGLE | SWT.LEFT | SWT.BORDER | SWT.PASSWORD );
    gatewayURLTextField.setText( "" );
    gatewayURLTextField.setLayoutData( gatewayUrlTextfieldGridData );
    gatewayURLTextField.addListener( SWT.CHANGED, clusterListener );
    props.setLook( gatewayURLTextField );

    Composite gatewayAuthenticationPanel = new Composite( mainPanel, SWT.NONE );
    GridLayout authenticationPanelGridLayout = new GridLayout( TWO_COLUMNS, true );
    authenticationPanelGridLayout.marginWidth = 0;
    gatewayAuthenticationPanel.setLayout( authenticationPanelGridLayout );
    GridData authenticationPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    gatewayAuthenticationPanel.setLayoutData( authenticationPanelGridData );
    props.setLook( gatewayAuthenticationPanel );

    GridData gatewayUsernameLabel = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    createLabel( gatewayAuthenticationPanel,
      BaseMessages.getString( PKG, "NamedClusterDialog.gatewayUsername" ),
      gatewayUsernameLabel, props );

    GridData gatewayPasswordLabel = new GridData();
    createLabel( gatewayAuthenticationPanel, BaseMessages.getString( PKG, "NamedClusterDialog.gatewayPassword" ),
      gatewayPasswordLabel, props );

    GridData gatewayUsernameTextFieldGridData = new GridData();
    gatewayUsernameTextFieldGridData.widthHint = Const.isLinux() ? 197 : 200; // TextField width
    gatewayUsernameTextfield =
      createText( gatewayAuthenticationPanel, "", gatewayUsernameTextFieldGridData, props, variableSpace,
        clusterListener );

    GridData gatewayPasswordTextFieldGridData = new GridData();
    gatewayPasswordTextFieldGridData.widthHint = Const.isLinux() ? 197 : 200; // TextField width
    gatewayPasswordTextField =
      createText( gatewayAuthenticationPanel, "", gatewayPasswordTextFieldGridData, props, variableSpace,
        clusterListener );
    gatewayPasswordTextField.setEchoChar( '*' );

    setControl( parent );
    initialize( thinNameClusterModel );
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
      ( (NamedClusterDialog) getWizard() ).getDialogState().equals( "import" ) ?
        BaseMessages.getString( PKG, "NamedClusterDialog.importCluster.title" ) :
        BaseMessages.getString( PKG, "NamedClusterDialog.newCluster.title" ) );

    if ( isConnectedToRepo() ) {
      setDescription( BaseMessages.getString( PKG, "NamedClusterDialog.repositoryNotification" ) );
    }

    gatewayURLTextField.setText( model.getGatewayUrl() );
    gatewayUsernameTextfield.setText( model.getGatewayUsername() );
    gatewayPasswordTextField.setText( model.getGatewayPassword() );

    validate();
  }

  private void validate() {
    thinNameClusterModel.setGatewayUrl( gatewayURLTextField.getText() );
    thinNameClusterModel.setGatewayUsername( gatewayUsernameTextfield.getText() );
    thinNameClusterModel.setGatewayPassword( gatewayPasswordTextField.getText() );
    setPageComplete(
      !thinNameClusterModel.getGatewayUrl().isBlank() && !thinNameClusterModel.getGatewayUsername().isBlank()
        && !thinNameClusterModel.getGatewayPassword().isBlank() );
  }

  public IWizardPage getNextPage() {
    return null;
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", BaseMessages.getString( PKG, "NamedClusterDialog.help" ), "" );
  }
}
