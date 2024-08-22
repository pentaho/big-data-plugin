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
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Listener;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.util.HelpUtils;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabel;

public class SecuritySettingsPage extends WizardPage {

  private PropsUI props;
  private Button noneButton;
  private Button kerberosButton;
  private Button knoxButton;
  private Composite basePanel;
  private Composite parent;
  private Composite mainPanel;
  private ThinNameClusterModel thinNameClusterModel;
  private NamedClusterSecurityType securityType;
  private final Listener securityTypeListener = e -> setSecurityType();

  public enum NamedClusterSecurityType {NONE, KERBEROS, KNOX}

  private static final Class<?> PKG = SecuritySettingsPage.class;

  public SecuritySettingsPage( ThinNameClusterModel model ) {
    super( SecuritySettingsPage.class.getSimpleName() );
    securityType = NamedClusterSecurityType.NONE;
    thinNameClusterModel = model;
    setTitle( BaseMessages.getString( PKG, "NamedClusterDialog.newCluster" ) );
    setDescription( BaseMessages.getString( PKG, "NamedClusterDialog.title" ) );
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

    GridData clusterNameLabelGridData = new GridData();
    clusterNameLabelGridData.widthHint = 400; // Label width
    createLabel( mainPanel, BaseMessages.getString( PKG, "NamedClusterDialog.security" ),
      clusterNameLabelGridData, props );

    noneButton = new Button( mainPanel, SWT.RADIO );
    noneButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.none" ) );
    noneButton.addListener( SWT.Selection, securityTypeListener );
    props.setLook( noneButton );

    kerberosButton = new Button( mainPanel, SWT.RADIO );
    kerberosButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.kerberos" ) );
    kerberosButton.addListener( SWT.Selection, securityTypeListener );
    props.setLook( kerberosButton );

    knoxButton = new Button( mainPanel, SWT.RADIO );
    knoxButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.knox" ) );
    knoxButton.addListener( SWT.Selection, securityTypeListener );
    props.setLook( knoxButton );

    setControl( parent );
    initialize( thinNameClusterModel );
  }

  private void setSecurityType() {
    if ( noneButton.getSelection() ) {
      securityType = NamedClusterSecurityType.NONE;
      thinNameClusterModel.setSecurityType( "None" );
      getContainer().updateButtons();
    }
    if ( kerberosButton.getSelection() ) {
      securityType = NamedClusterSecurityType.KERBEROS;
      thinNameClusterModel.setSecurityType( "Kerberos" );
      getContainer().updateButtons();
    }
    if ( knoxButton.getSelection() ) {
      securityType = NamedClusterSecurityType.KNOX;
      thinNameClusterModel.setSecurityType( "Knox" );
      getContainer().updateButtons();
    }
  }

  public void initialize( ThinNameClusterModel model ) {
    thinNameClusterModel = model;
    noneButton.setSelection( model.getSecurityType().equals( "None" ) );
    kerberosButton.setSelection( model.getSecurityType().equals( "Kerberos" ) );
    knoxButton.setSelection( model.getSecurityType().equals( "Knox" ) );
    if ( noneButton.getSelection() ) {
      securityType = NamedClusterSecurityType.NONE;
    }
    if ( kerberosButton.getSelection() ) {
      securityType = NamedClusterSecurityType.KERBEROS;
    }
    if ( knoxButton.getSelection() ) {
      securityType = NamedClusterSecurityType.KNOX;
    }
    knoxButton.setVisible(
      model.getShimVendor().equals( "Cloudera" ) || model.getShimVendor().equals( "Hortonworks" ) );
  }

  public NamedClusterSecurityType getSecurityType() {
    return securityType;
  }

  public IWizardPage getNextPage() {
    IWizardPage nextPage = null;
    if ( getSecurityType().equals( NamedClusterSecurityType.KERBEROS ) ) {
      nextPage = getWizard().getPage( KerberosSettingsPage.class.getSimpleName() );
    }
    if ( getSecurityType().equals( NamedClusterSecurityType.KNOX ) ) {
      nextPage = getWizard().getPage( KnoxSettingsPage.class.getSimpleName() );
    }
    return nextPage;
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", "https://docs.hitachivantara.com", "" );
  }
}