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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.pages;

import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.util.HelpUtils;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabelWithStyle;

public class AddDriverResultPage extends WizardPage {

  private PropsUI props;
  private Composite parent;
  private Composite basePanel;
  private Composite mainPanel;
  private Label statusLabel;
  private Label iconLabel;
  private Label statusDescriptionLabel;
  private final VariableSpace variableSpace;
  private static final Class<?> PKG = AddDriverResultPage.class;
  private static final String SUCCESS_IMG = "images/success.svg";
  private static final String FAIL_IMG = "images/fail.svg";

  public AddDriverResultPage( VariableSpace variables ) {
    super( AddDriverResultPage.class.getSimpleName() );
    variableSpace = variables;

    setTitle( BaseMessages.getString( PKG, "AddDriverDialog.addDriver" ) );
    setPageComplete( true );
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

    setControl( parent );
  }

  public void setImportResult( boolean pass ) {
    if ( pass ) {
      statusLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.pass" ) );
      statusDescriptionLabel.setText( BaseMessages.getString( PKG, "AddDriverDialog.success.description" ) );
      iconLabel.setImage(
        GUIResource.getInstance().getImage( SUCCESS_IMG, getClass().getClassLoader(), 70, 70 ) );
    } else {
      statusLabel.setText( BaseMessages.getString( PKG, "AddDriverDialog.fail" ) );
      statusDescriptionLabel.setText( BaseMessages.getString( PKG, "AddDriverDialog.fail.description" ) );
      iconLabel.setImage(
        GUIResource.getInstance().getImage( FAIL_IMG, getClass().getClassLoader(), 70, 70 ) );
    }
    mainPanel.pack();
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", BaseMessages.getString( PKG, "NamedClusterDialog.help" ), "" );
  }
}
