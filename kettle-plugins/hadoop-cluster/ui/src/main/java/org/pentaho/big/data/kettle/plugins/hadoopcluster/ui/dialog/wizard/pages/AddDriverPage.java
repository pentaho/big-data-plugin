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

import org.eclipse.jface.wizard.IWizardPage;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.util.HelpUtils;

import java.io.File;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.TWO_COLUMNS;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabel;

public class AddDriverPage extends WizardPage {

  private PropsUI props;
  private Composite parent;
  private Composite mainPanel;
  private final VariableSpace variableSpace;
  private Text driverText;
  private static final Class<?> PKG = AddDriverPage.class;
  private final String NO_FILE_SELECTED = BaseMessages.getString( PKG, "NamedClusterDialog.noFileSelected" );

  public AddDriverPage( VariableSpace variables ) {
    super( AddDriverPage.class.getSimpleName() );
    variableSpace = variables;

    setTitle( BaseMessages.getString( PKG, "AddDriverDialog.addDriver" ) );
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
    GridLayout basePanelGridLayout = new GridLayout( ONE_COLUMN, false );
    basePanelGridLayout.marginWidth = 60; //TO CENTER CONTENTS
    basePanelGridLayout.marginTop = 10; //TO CENTER CONTENTS
    basePanelGridLayout.marginBottom = 30;
    basePanelGridLayout.marginLeft = 20;
    basePanel.setLayout( basePanelGridLayout );
    GridData basePanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    basePanel.setLayoutData( basePanelGridData );
    props.setLook( basePanel );
    //END OF MAIN LAYOUT

    mainPanel = new Composite( basePanel, SWT.NONE );
    mainPanel.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData headerPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    mainPanel.setLayoutData( headerPanelGridData );
    props.setLook( mainPanel );

    Link suportedDriversLink = new Link( mainPanel, SWT.NONE );
    suportedDriversLink.setText(
      "<a href=\"\">" + BaseMessages.getString( PKG, "AddDriverDialog.supportedDrivers" ) + "</a>." );
    props.setLook( suportedDriversLink );
    suportedDriversLink.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        Program.launch( BaseMessages.getString( PKG, "AddDriverDialog.driverVendors" ) );
      }
    } );

    GridData driverFileLabelGridData = new GridData();
    createLabel( mainPanel, BaseMessages.getString( PKG, "AddDriverDialog.driverFile" ),
      driverFileLabelGridData, props );

    Composite driverBrowsePanel = new Composite( mainPanel, SWT.NONE );
    GridLayout druverBrowsePanelGridLayout = new GridLayout( TWO_COLUMNS, false );
    druverBrowsePanelGridLayout.marginWidth = 0;
    driverBrowsePanel.setLayout( druverBrowsePanelGridLayout );
    GridData driverTextGridData = new GridData();
    props.setLook( driverBrowsePanel );

    driverText = new Text( driverBrowsePanel, SWT.BORDER );
    driverText.setEditable( false );
    driverText.setText( NO_FILE_SELECTED );
    driverTextGridData.widthHint = Const.isLinux() ? 310 : 341;
    driverText.setLayoutData( driverTextGridData );
    props.setLook( driverText );

    Button driverBrowseButton = new Button( driverBrowsePanel, SWT.PUSH );
    driverBrowseButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.browse" ) );
    props.setLook( driverBrowseButton );
    Listener driverBrowseButtonListener = e -> driverBrowse();
    driverBrowseButton.addListener( SWT.Selection, driverBrowseButtonListener );

    setControl( parent );
  }

  private void driverBrowse() {
    FileDialog dialog = new FileDialog( mainPanel.getShell(), SWT.OPEN );
    String path = dialog.open();
    if ( path != null ) {
      File file = new File( path );
      if ( file.isFile() ) {
        driverText.setText( file.toString() );
        validate();
      }
    }
  }

  public String getFileName() {
    return driverText.getText();
  }

  public IWizardPage getNextPage() {
    return null;
  }

  private void validate() {
    if ( !driverText.getText().equals( NO_FILE_SELECTED ) ) {
      setPageComplete( true );
    }
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", BaseMessages.getString( PKG, "AddDriverDialog.help" ), "" );
  }
}
