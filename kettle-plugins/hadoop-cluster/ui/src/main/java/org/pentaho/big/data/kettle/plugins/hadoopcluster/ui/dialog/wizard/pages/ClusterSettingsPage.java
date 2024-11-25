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
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.NamedClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.hadoop.shim.api.core.ShimIdentifierInterface;
import org.pentaho.platform.engine.core.system.PentahoSystem;

import java.io.File;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.ONE_COLUMN;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.TWO_COLUMNS;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createLabel;
import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.wizard.util.NamedClusterHelper.createText;

public class ClusterSettingsPage extends WizardPage {

  private PropsUI props;
  private Composite parent;
  private Composite mainPanel;
  private ScrolledComposite clusterScrollPanel;
  private TextVar hostNameTextFieldHdfsGroup;
  private TextVar portTextFieldHdfsGroup;
  private TextVar userNameTextFieldHdfsGroup;
  private TextVar passwordTextFieldHdfsGroup;
  private TextVar hostNameTextFieldJobTrackerGroup;
  private TextVar portTextFieldJobTrackerGroup;
  private TextVar hostNameTextFieldZooKeeperGroup;
  private TextVar portTextFieldZooKeeperGroup;
  private TextVar hostNameTextFieldOozieGroup;
  private TextVar hostNameTextFieldKafkaGroup;
  private Button deleteSiteFilesButton;
  private Text nameOfNamedCluster;
  private Table siteFilesTable;
  private CCombo shimVendorCombo;
  private CCombo shimVersionCombo;
  private Group hdfsGroup;
  private Group jobTrackerGroup;
  private Group zooKeeperGroup;
  private Group oozieGroup;
  private Group kafkaGroup;
  private Composite fillerComposite;
  private Map<String, String> siteFilesPath;
  private ThinNameClusterModel thinNameClusterModel;
  private final Listener clusterListener = e -> validate();
  private final Listener shimVendorListener = e -> displayShimVersions( true );
  private final VariableSpace variableSpace;
  private static final Class<?> PKG = ClusterSettingsPage.class;

  public ClusterSettingsPage( VariableSpace variables, ThinNameClusterModel model ) {
    super( ClusterSettingsPage.class.getSimpleName() );
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

    //START OF HEADER
    Composite headerPanel = new Composite( basePanel, SWT.NONE );
    headerPanel.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData headerPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    headerPanel.setLayoutData( headerPanelGridData );
    props.setLook( headerPanel );

    GridData clusterNameLabelGridData = new GridData();
    clusterNameLabelGridData.widthHint = 400; // Label width
    createLabel( headerPanel, BaseMessages.getString( PKG, "NamedClusterDialog.clusterName" ),
      clusterNameLabelGridData, props );

    GridData clusterNameTextFieldGridData = new GridData();
    clusterNameTextFieldGridData.widthHint = Const.isLinux() ? 395 : 409; // TextField width
    nameOfNamedCluster = new Text( headerPanel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    nameOfNamedCluster.setText( "" );
    nameOfNamedCluster.setLayoutData( clusterNameTextFieldGridData );
    nameOfNamedCluster.addListener( SWT.CHANGED, clusterListener );
    props.setLook( nameOfNamedCluster );
    //END OF HEADER

    //START OF CLUSTER SCROLLABLE PANEL
    clusterScrollPanel = new ScrolledComposite( basePanel, SWT.V_SCROLL | SWT.NONE );
    clusterScrollPanel.setExpandHorizontal( true );
    clusterScrollPanel.setExpandVertical( true );
    clusterScrollPanel.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData clusterScrollPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    clusterScrollPanelGridData.heightHint = 490; //Height of the scrollable panel (WILL NEED TO ADJUST)
    clusterScrollPanel.setLayoutData( clusterScrollPanelGridData );
    props.setLook( clusterScrollPanel );

    //START MAIN PANEL
    mainPanel = new Composite( clusterScrollPanel, SWT.NONE );
    mainPanel.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData mainPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    mainPanel.setLayoutData( mainPanelGridData );
    props.setLook( mainPanel );
    //END MAIN PANEL

    createDriverGroup();
    createSiteXMLFilesGroup();
    //END OF CLUSTER SCROLLABLE PANEL

    clusterScrollPanel.setContent( mainPanel );
    initialize( thinNameClusterModel );
    setControl( parent );
  }

  private void createDriverGroup() {
    Composite driverGroupPanel = new Composite( mainPanel, SWT.NONE );
    GridLayout driverGroupGridLayout = new GridLayout( TWO_COLUMNS, true );
    driverGroupGridLayout.marginWidth = 0;
    driverGroupPanel.setLayout( driverGroupGridLayout );
    GridData driverGroupPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    driverGroupPanel.setLayoutData( driverGroupPanelGridData );
    props.setLook( driverGroupPanel );

    GridData driverLabelGroupGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    createLabel( driverGroupPanel, BaseMessages.getString( PKG, "NamedClusterDialog.driver" ),
      driverLabelGroupGridData, props );

    GridData versionLabelGroupGridData = new GridData();
    createLabel( driverGroupPanel, BaseMessages.getString( PKG, "NamedClusterDialog.version" ),
      versionLabelGroupGridData, props );

    GridData driverComboGroupGridData = new GridData();
    driverComboGroupGridData.widthHint = 204; // Combo width
    shimVendorCombo =
      new CCombo( driverGroupPanel, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    shimVendorCombo.setLayoutData( driverComboGroupGridData );
    shimVendorCombo.setItems( getVendors() );
    shimVendorCombo.addListener( SWT.Selection, shimVendorListener );
    props.setLook( shimVendorCombo );

    GridData versionComboGroupGridData = new GridData();
    versionComboGroupGridData.widthHint = 204; // Combo width
    shimVersionCombo =
      new CCombo( driverGroupPanel, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    shimVersionCombo.setLayoutData( versionComboGroupGridData );
    shimVersionCombo.addListener( SWT.Selection, clusterListener );
    props.setLook( shimVersionCombo );
  }

  private void createSiteXMLFilesGroup() {
    Group siteXmlFilesGroup = new Group( mainPanel, SWT.NONE );
    siteXmlFilesGroup.setText( BaseMessages.getString( PKG, "NamedClusterDialog.siteXmlFiles" ) );
    siteXmlFilesGroup.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData jobTrackerGroupGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    siteXmlFilesGroup.setLayoutData( jobTrackerGroupGridData );
    props.setLook( siteXmlFilesGroup );

    Composite buttonsPanel = new Composite( siteXmlFilesGroup, SWT.NONE );
    GridLayout buttonsPanelGridLayout = new GridLayout( TWO_COLUMNS, false );
    buttonsPanel.setLayout( buttonsPanelGridLayout );
    GridData basePanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    buttonsPanel.setLayoutData( basePanelGridData );
    props.setLook( buttonsPanel );

    Button browseButton = new Button( buttonsPanel, SWT.PUSH );
    GridData browserButtonGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    browseButton.setLayoutData( browserButtonGridData );
    browseButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.browseButton" ) );
    Listener browseListener = e -> browse();
    browseButton.addListener( SWT.Selection, browseListener );
    props.setLook( browseButton );
    deleteSiteFilesButton = new Button( buttonsPanel, SWT.PUSH );
    deleteSiteFilesButton.setText( BaseMessages.getString( PKG, "NamedClusterDialog.remove" ) );
    deleteSiteFilesButton.setToolTipText( BaseMessages.getString( PKG, "NamedClusterDialog.removeSiteFile" ) );
    deleteSiteFilesButton.setEnabled( false );
    GridData deleteButtonGridData = new GridData( SWT.END, SWT.FILL, true, false );
    deleteSiteFilesButton.setLayoutData( deleteButtonGridData );

    Listener removeSiteFileListener = e -> removeSelectedSiteFiles();
    deleteSiteFilesButton.addListener( SWT.Selection, removeSiteFileListener );
    props.setLook( deleteSiteFilesButton );

    siteFilesTable = new Table( siteXmlFilesGroup, SWT.BORDER | SWT.CHECK );
    siteFilesTable.setHeaderVisible( true );
    siteFilesTable.setLinesVisible( true );
    GridData data = new GridData( SWT.FILL, SWT.FILL, true, true );
    data.heightHint = 100;
    siteFilesTable.setLayoutData( data );
    Listener tableListener = e -> processTableSelection();
    siteFilesTable.addListener( SWT.Selection, tableListener );
    props.setLook( siteFilesTable );

    TableColumn fileNameColumn = new TableColumn( siteFilesTable, SWT.NONE );
    fileNameColumn.setText( BaseMessages.getString( PKG, "NamedClusterDialog.file" ) );
    fileNameColumn.setWidth( 330 );
    fileNameColumn.setResizable( false );
  }

  private void processTableSelection() {
    List<TableItem> selectedSiteFiles = getSelectedSiteFiles();
    deleteSiteFilesButton.setEnabled( !selectedSiteFiles.isEmpty() );
  }

  private void removeSelectedSiteFiles() {
    MessageBox warning = new MessageBox( mainPanel.getShell(), SWT.YES | SWT.NO );
    warning.setMessage( BaseMessages.getString( PKG, "NamedClusterDialog.siteFileAlert" ) );
    int buttonClicked = warning.open();
    if ( buttonClicked == SWT.YES ) {
      List<TableItem> selectedSiteFiles = getSelectedSiteFiles();
      for ( TableItem selectedSiteFile : selectedSiteFiles ) {
        siteFilesPath.remove( selectedSiteFile.getText() );
        siteFilesTable.remove( siteFilesTable.indexOf( selectedSiteFile ) );
      }
      deleteSiteFilesButton.setEnabled( false );
      validate();
    }
  }

  private List<TableItem> getSelectedSiteFiles() {
    List<TableItem> selectedSiteFiles = new ArrayList<>();
    for ( TableItem siteFile : siteFilesTable.getItems() ) {
      if ( siteFile.getChecked() ) {
        selectedSiteFiles.add( siteFile );
      }
    }
    return selectedSiteFiles;
  }

  private void createHdfsGroup() {
    hdfsGroup = new Group( mainPanel, SWT.NONE );
    hdfsGroup.setText( BaseMessages.getString( PKG, "NamedClusterDialog.hdfs" ) );
    hdfsGroup.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData hdfsGroupGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    hdfsGroup.setLayoutData( hdfsGroupGridData );
    props.setLook( hdfsGroup );

    if ( ( (NamedClusterDialog) getWizard() ).getDialogState().equals( "new-edit" ) ) {
      GridData hostNameLabelHdfsGroupGridData = new GridData();
      hostNameLabelHdfsGroupGridData.widthHint = 400; // Label width
      createLabel( hdfsGroup, BaseMessages.getString( PKG, "NamedClusterDialog.hostname" ),
        hostNameLabelHdfsGroupGridData, props );

      GridData hostNameTextFieldHdfsGroupdGridData = new GridData();
      hostNameTextFieldHdfsGroupdGridData.widthHint = 400; // TextField width
      hostNameTextFieldHdfsGroup =
        createText( hdfsGroup, "", hostNameTextFieldHdfsGroupdGridData, props, variableSpace, clusterListener );

      GridData portLabelHdfsGroupGridData = new GridData();
      portLabelHdfsGroupGridData.widthHint = 400; // Label width
      createLabel( hdfsGroup, BaseMessages.getString( PKG, "NamedClusterDialog.port" ), portLabelHdfsGroupGridData,
        props );

      GridData portTextFieldHdfsGroupGridData = new GridData();
      portTextFieldHdfsGroupGridData.widthHint = 400; // TextField width
      portTextFieldHdfsGroup =
        createText( hdfsGroup, "", portTextFieldHdfsGroupGridData, props, variableSpace, clusterListener );
    }

    Composite userPasswordHdfsGroupPanel = new Composite( hdfsGroup, SWT.NONE );
    GridLayout userPasswordHdfsGroupGridLayout = new GridLayout( TWO_COLUMNS, true );
    userPasswordHdfsGroupGridLayout.marginWidth = 0;
    userPasswordHdfsGroupPanel.setLayout( userPasswordHdfsGroupGridLayout );
    GridData userPasswordHdfsGroupPanelGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    userPasswordHdfsGroupPanel.setLayoutData( userPasswordHdfsGroupPanelGridData );
    props.setLook( userPasswordHdfsGroupPanel );

    GridData userNameLabelHdfsGroupGridData = new GridData( SWT.BEGINNING, SWT.FILL, true, false );
    createLabel( userPasswordHdfsGroupPanel, BaseMessages.getString( PKG, "NamedClusterDialog.username" ),
      userNameLabelHdfsGroupGridData, props );

    GridData passwordLabelHdfsGroupGridData = new GridData();
    createLabel( userPasswordHdfsGroupPanel, BaseMessages.getString( PKG, "NamedClusterDialog.password" ),
      passwordLabelHdfsGroupGridData, props );

    GridData userNameTextFieldHdfsGroupGridData = new GridData();
    userNameTextFieldHdfsGroupGridData.widthHint = 197; // TextField width
    userNameTextFieldHdfsGroup =
      createText( userPasswordHdfsGroupPanel, "", userNameTextFieldHdfsGroupGridData, props, variableSpace,
        clusterListener );

    GridData passwordTextFieldHdfsGroupGridData = new GridData();
    passwordTextFieldHdfsGroupGridData.widthHint = 197; // TextField width
    passwordTextFieldHdfsGroup =
      createText( userPasswordHdfsGroupPanel, "", passwordTextFieldHdfsGroupGridData, props, variableSpace,
        clusterListener );
    passwordTextFieldHdfsGroup.setEchoChar( '*' );
    mainPanel.pack();
  }

  private void createJobTrackerGroup() {
    jobTrackerGroup = new Group( mainPanel, SWT.NONE );
    jobTrackerGroup.setText( BaseMessages.getString( PKG, "NamedClusterDialog.jobTracker" ) );
    jobTrackerGroup.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData jobTrackerGroupGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    jobTrackerGroup.setLayoutData( jobTrackerGroupGridData );
    props.setLook( jobTrackerGroup );

    GridData hostNameLabelJobTrackerGroupGridData = new GridData();
    hostNameLabelJobTrackerGroupGridData.widthHint = 400; // Label width
    createLabel( jobTrackerGroup, BaseMessages.getString( PKG, "NamedClusterDialog.hostname" ),
      hostNameLabelJobTrackerGroupGridData, props );

    GridData hostNameTextFieldJobTrackerGroupdGridData = new GridData();
    hostNameTextFieldJobTrackerGroupdGridData.widthHint = 400; // TextField width
    hostNameTextFieldJobTrackerGroup =
      createText( jobTrackerGroup, "", hostNameTextFieldJobTrackerGroupdGridData, props, variableSpace,
        clusterListener );

    GridData portLabelJobTrackerGroupGridData = new GridData();
    portLabelJobTrackerGroupGridData.widthHint = 400; // Label width
    createLabel( jobTrackerGroup, BaseMessages.getString( PKG, "NamedClusterDialog.port" ),
      portLabelJobTrackerGroupGridData, props );

    GridData portTextFieldJobTrackerGroupGridData = new GridData();
    portTextFieldJobTrackerGroupGridData.widthHint = 400; // TextField width
    portTextFieldJobTrackerGroup =
      createText( jobTrackerGroup, "", portTextFieldJobTrackerGroupGridData, props, variableSpace, clusterListener );
    mainPanel.pack();
  }

  private void createZooKeeperGroup() {
    zooKeeperGroup = new Group( mainPanel, SWT.NONE );
    zooKeeperGroup.setText( BaseMessages.getString( PKG, "NamedClusterDialog.zooKeeper" ) );
    zooKeeperGroup.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData zooKeeperGroupGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    zooKeeperGroup.setLayoutData( zooKeeperGroupGridData );
    props.setLook( zooKeeperGroup );

    GridData hostNameLabelZooKeeperGroupGridData = new GridData();
    hostNameLabelZooKeeperGroupGridData.widthHint = 400; // Label width
    createLabel( zooKeeperGroup, BaseMessages.getString( PKG, "NamedClusterDialog.hostname" ),
      hostNameLabelZooKeeperGroupGridData, props );

    GridData hostNameTextFieldZooKeeperGroupdGridData = new GridData();
    hostNameTextFieldZooKeeperGroupdGridData.widthHint = 400; // TextField width
    hostNameTextFieldZooKeeperGroup =
      createText( zooKeeperGroup, "", hostNameTextFieldZooKeeperGroupdGridData, props, variableSpace, clusterListener );

    GridData portLabelZooKeeperGroupGridData = new GridData();
    portLabelZooKeeperGroupGridData.widthHint = 400; // Label width
    createLabel( zooKeeperGroup, BaseMessages.getString( PKG, "NamedClusterDialog.port" ),
      portLabelZooKeeperGroupGridData, props );

    GridData portTextFieldZooKeeperGroupGridData = new GridData();
    portTextFieldZooKeeperGroupGridData.widthHint = 400; // TextField width
    portTextFieldZooKeeperGroup =
      createText( zooKeeperGroup, "", portTextFieldZooKeeperGroupGridData, props, variableSpace, clusterListener );
    mainPanel.pack();
  }

  private void createOozieGroup() {
    oozieGroup = new Group( mainPanel, SWT.NONE );
    oozieGroup.setText( BaseMessages.getString( PKG, "NamedClusterDialog.oozie" ) );
    oozieGroup.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData oozieGroupGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    oozieGroup.setLayoutData( oozieGroupGridData );
    props.setLook( oozieGroup );

    GridData hostNameLabelOozieGroupGridData = new GridData();
    hostNameLabelOozieGroupGridData.widthHint = 400; // Label width
    createLabel( oozieGroup, BaseMessages.getString( PKG, "NamedClusterDialog.hostname" ),
      hostNameLabelOozieGroupGridData, props );

    GridData hostNameTextFieldOozieGroupdGridData = new GridData();
    hostNameTextFieldOozieGroupdGridData.widthHint = 400; // TextField width
    hostNameTextFieldOozieGroup =
      createText( oozieGroup, "", hostNameTextFieldOozieGroupdGridData, props, variableSpace, clusterListener );
    mainPanel.pack();
  }

  private void createKafkaGroup() {
    kafkaGroup = new Group( mainPanel, SWT.NONE );
    kafkaGroup.setText( BaseMessages.getString( PKG, "NamedClusterDialog.kafka" ) );
    kafkaGroup.setLayout( new GridLayout( ONE_COLUMN, false ) );
    GridData kafkaGroupGridData = new GridData( SWT.FILL, SWT.FILL, false, false );
    kafkaGroup.setLayoutData( kafkaGroupGridData );
    props.setLook( kafkaGroup );

    GridData hostNameLabelKafkaGroupGridData = new GridData();
    hostNameLabelKafkaGroupGridData.widthHint = 400; // Label width
    createLabel( kafkaGroup, BaseMessages.getString( PKG, "NamedClusterDialog.bootstrapServers" ),
      hostNameLabelKafkaGroupGridData, props );

    GridData hostNameTextFieldKafkaGroupdGridData = new GridData();
    hostNameTextFieldKafkaGroupdGridData.widthHint = 400; // TextField width
    hostNameTextFieldKafkaGroup =
      createText( kafkaGroup, "", hostNameTextFieldKafkaGroupdGridData, props, variableSpace, clusterListener );
    mainPanel.pack();
  }

  private void createFiller() {
    fillerComposite = new Composite( mainPanel, SWT.NONE );
    props.setLook( fillerComposite );
    mainPanel.pack();
  }

  private void browse() {
    FileDialog dialog = new FileDialog( mainPanel.getShell(), SWT.MULTI );
    dialog.open();
    for ( String fileName : dialog.getFileNames() ) {
      addSiteFileToTable( fileName );
      siteFilesPath.put( fileName, dialog.getFilterPath() + File.separator );
    }
    if ( dialog.getFileNames().length > 0 ) {
      validate();
    }
  }

  private void displayShimVersions( boolean validate ) {
    shimVersionCombo.setItems( getVersionsForVendor( shimVendorCombo.getText() ) );
    if ( validate ) {
      shimVersionCombo.select( 0 );
      validate();
    }
  }

  private void validate() {
    thinNameClusterModel.setName( nameOfNamedCluster.getText() );
    thinNameClusterModel.setShimVendor( shimVendorCombo.getText() );
    thinNameClusterModel.setShimVersion( shimVersionCombo.getText() );
    thinNameClusterModel.setHdfsUsername( userNameTextFieldHdfsGroup.getText() );
    thinNameClusterModel.setHdfsPassword( passwordTextFieldHdfsGroup.getText() );
    thinNameClusterModel.setSiteFiles( getTableItems( siteFilesTable.getItems() ) );

    if ( ( (NamedClusterDialog) getWizard() ).getDialogState().equals( "new-edit" ) ) {
      thinNameClusterModel.setHdfsHost( hostNameTextFieldHdfsGroup.getText() );
      thinNameClusterModel.setHdfsPort( portTextFieldHdfsGroup.getText() );
      thinNameClusterModel.setJobTrackerPort( portTextFieldJobTrackerGroup.getText() );
      thinNameClusterModel.setZooKeeperPort( portTextFieldZooKeeperGroup.getText() );
      thinNameClusterModel.setJobTrackerHost( hostNameTextFieldJobTrackerGroup.getText() );
      thinNameClusterModel.setZooKeeperHost( hostNameTextFieldZooKeeperGroup.getText() );
      thinNameClusterModel.setOozieUrl( hostNameTextFieldOozieGroup.getText() );
      thinNameClusterModel.setKafkaBootstrapServers( hostNameTextFieldKafkaGroup.getText() );
      setPageComplete( !thinNameClusterModel.getName().isBlank() && !thinNameClusterModel.getHdfsHost().isBlank()
        && !thinNameClusterModel.getShimVendor().isBlank() && !thinNameClusterModel.getShimVersion().isBlank()
        && thinNameClusterModel.getName().matches( "^[a-zA-Z0-9-]+$" ) );
    }
    if ( ( (NamedClusterDialog) getWizard() ).getDialogState().equals( "import" ) ) {
      setPageComplete( !thinNameClusterModel.getName().isBlank()
        && !thinNameClusterModel.getSiteFiles().isEmpty()
        && thinNameClusterModel.getName().matches( "^[a-zA-Z0-9-]+$" ) );
    }
  }

  public IWizardPage getNextPage() {
    SecuritySettingsPage securitySettingsPage =
      (SecuritySettingsPage) getWizard().getPage( SecuritySettingsPage.class.getSimpleName() );
    securitySettingsPage.initialize( thinNameClusterModel );
    return securitySettingsPage;
  }

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

    thinNameClusterModel = model;
    siteFilesPath = new HashMap<>();
    nameOfNamedCluster.setText( model.getName() );
    if ( !model.getShimVendor().isBlank() ) {
      shimVendorCombo.setText( model.getShimVendor() );
    } else {
      shimVendorCombo.select( 0 );
    }
    if ( !shimVendorCombo.getText().isBlank() ) {
      displayShimVersions( false );
    }
    if ( !model.getShimVersion().isBlank() ) {
      shimVersionCombo.setText( model.getShimVersion() );
    } else {
      shimVersionCombo.select( 0 );
    }
    setTableItems( model.getSiteFiles() );
    disposeComponents();
    createHdfsGroup();
    userNameTextFieldHdfsGroup.setText( model.getHdfsUsername() );
    passwordTextFieldHdfsGroup.setText( model.getHdfsPassword() );
    if ( ( (NamedClusterDialog) getWizard() ).getDialogState().equals( "new-edit" ) ) {
      createJobTrackerGroup();
      createZooKeeperGroup();
      createOozieGroup();
      createKafkaGroup();
      createFiller();
      hostNameTextFieldHdfsGroup.setText( model.getHdfsHost() );
      portTextFieldHdfsGroup.setText( model.getHdfsPort() );
      portTextFieldJobTrackerGroup.setText( model.getJobTrackerPort() );
      portTextFieldZooKeeperGroup.setText( model.getZooKeeperPort() );
      hostNameTextFieldJobTrackerGroup.setText( model.getJobTrackerHost() );
      hostNameTextFieldZooKeeperGroup.setText( model.getZooKeeperHost() );
      hostNameTextFieldOozieGroup.setText( model.getOozieUrl() );
      hostNameTextFieldKafkaGroup.setText( model.getKafkaBootstrapServers() );
    }
    clusterScrollPanel.setMinSize( mainPanel.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );
    mainPanel.pack();
    validate();
  }

  private void disposeComponents() {
    if ( hdfsGroup != null ) {
      hdfsGroup.dispose();
      hdfsGroup = null;
    }
    if ( jobTrackerGroup != null ) {
      jobTrackerGroup.dispose();
      jobTrackerGroup = null;
    }
    if ( zooKeeperGroup != null ) {
      zooKeeperGroup.dispose();
      zooKeeperGroup = null;
    }
    if ( oozieGroup != null ) {
      oozieGroup.dispose();
      oozieGroup = null;
    }
    if ( kafkaGroup != null ) {
      kafkaGroup.dispose();
      kafkaGroup = null;
    }
    if ( fillerComposite != null ) {
      fillerComposite.dispose();
      fillerComposite = null;
    }
    mainPanel.pack();
  }

  private List<SimpleImmutableEntry<String, String>> getTableItems( TableItem[] tableItems ) {
    List<SimpleImmutableEntry<String, String>> siteFiles = new ArrayList<>();
    for ( TableItem tableItem : tableItems ) {
      String path = siteFilesPath.get( tableItem.getText() );
      path = path == null ? "" : path;
      siteFiles.add( new SimpleImmutableEntry( path, tableItem.getText() ) );
    }
    return siteFiles;
  }

  public IWizardPage getPreviousPage() {
    return null;
  }

  public void performHelp() {
    HelpUtils.openHelpDialog( parent.getShell(), "", BaseMessages.getString( PKG, "NamedClusterDialog.help" ), "" );
  }

  private void setTableItems( List<SimpleImmutableEntry<String, String>> siteFiles ) {
    siteFilesTable.removeAll();
    for ( SimpleImmutableEntry<String, String> siteFile : siteFiles ) {
      addSiteFileToTable( siteFile.getValue() );
    }
  }

  private void addSiteFileToTable( String fileName ) {
    TableItem item = new TableItem( siteFilesTable, SWT.NONE );
    item.setText( 0, fileName );
  }

  private String[] getVendors() {
    List<ShimIdentifierInterface> shims = getShimIdentifiers();
    List<String> vendors = new ArrayList<>();
    for ( ShimIdentifierInterface shim : shims ) {
      if ( !vendors.contains( shim.getVendor() ) ) {
        vendors.add( shim.getVendor() );
      }
    }
    return vendors.toArray( new String[ 0 ] );
  }

  private String[] getVersionsForVendor( String vendor ) {
    List<String> versions = new ArrayList<>();
    if ( !vendor.isBlank() ) {
      List<ShimIdentifierInterface> shims = getShimIdentifiers();
      for ( ShimIdentifierInterface shim : shims ) {
        if ( shim.getVendor().equals( vendor ) ) {
          versions.add( shim.getVersion() );
        }
      }
    }
    return versions.toArray( new String[ 0 ] );
  }

  // FOR DEV MODE ONLY
  class ShimMock implements ShimIdentifierInterface {

    private String vendor;
    private String version;

    public ShimMock( String venVar, String verVar ) {
      vendor = venVar;
      version = verVar;
    }

    public String getId() {
      return null;
    }

    public void setId( String var1 ) {
    }

    public String getVendor() {
      return vendor;
    }

    public void setVendor( String var1 ) {
      vendor = var1;
    }

    public String getVersion() {
      return version;
    }

    public void setVersion( String var1 ) {
      version = var1;
    }

    public ShimType getType() {
      return null;
    }

    public void setType( ShimType var1 ) {
    }
  }

  private boolean isDevMode() {
    NamedClusterDialog namedClusterDialog = (NamedClusterDialog) getWizard();
    return namedClusterDialog.isDevMode();
  }

  private List<ShimIdentifierInterface> getShimIdentifiers() {
    List<ShimIdentifierInterface> shims = PentahoSystem.getAll( ShimIdentifierInterface.class );
    shims.sort( Comparator.comparing( ShimIdentifierInterface::getVendor ) );
    if ( isDevMode() ) {
      shims = new ArrayList<>();
      shims.add( new ShimMock( "Apache", "1.1" ) );
      shims.add( new ShimMock( "Apache", "1.2" ) );
      shims.add( new ShimMock( "Cloudera", "2.1" ) );
      shims.add( new ShimMock( "Cloudera", "2.2" ) );
    }
    return shims;
  }
}
