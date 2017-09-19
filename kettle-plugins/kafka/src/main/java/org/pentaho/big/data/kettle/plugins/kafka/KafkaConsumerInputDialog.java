/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.kafka;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.repository.dialog.SelectObjectDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.ConnectionType.CLUSTER;
import static org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputMeta.ConnectionType.DIRECT;

@SuppressWarnings( { "FieldCanBeLocal", "unused" } )
public class KafkaConsumerInputDialog extends BaseStepDialog implements StepDialogInterface {

  public static final int INPUT_WIDTH = 350;
  private static Class<?> PKG = KafkaConsumerInputMeta.class;
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  private static final Map<String, String> DEFAULT_OPTION_VALUES = ImmutableMap.of( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
      CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name );
  private final KafkaFactory kafkaFactory = KafkaFactory.defaultFactory();

  private KafkaConsumerInputMeta meta;
  private TransMeta executorTransMeta = null;

  private Label wlTransPath;
  private TextVar wTransPath;
  private Button wbBrowseTrans;

  private ObjectLocationSpecificationMethod specificationMethod;

  private Label wlClusterName;
  private ComboVar wClusterName;
  private Label wlTopic;
  private Label wlConsumerGroup;
  private TextVar wConsumerGroup;
  private ModifyListener lsMod;
  private TableView fieldsTable;
  private TableView topicsTable;
  private TableView optionsTable;
  private Label wlBatchSize;
  private TextVar wBatchSize;
  private Label wlBatchDuration;
  private TextVar wBatchDuration;

  private CTabFolder wTabFolder;
  private CTabItem wSetupTab;
  private CTabItem wBatchTab;
  private CTabItem wFieldsTab;
  private CTabItem wOptionsTab;

  private Composite wSetupComp;
  private Composite wFieldsComp;
  private Composite wBatchComp;
  private Composite wOptionsComp;
  private Button wbDirect;
  private Button wbCluster;
  private Label wlBootstrapServers;
  private TextVar wBootstrapServers;

  public KafkaConsumerInputDialog( Shell parent, Object in, TransMeta tr, String sname ) {
    super( parent, (BaseStepMeta) in, tr, sname );
    meta = (KafkaConsumerInputMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );
    shell.setMinimumSize( 527, 622 );

    lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Shell.Title" ) );

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );

    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, 0 );
    wlStepname.setLayoutData( fdlStepname );

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.width = 250;
    fdStepname.left = new FormAttachment( 0, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 5 );
    wStepname.setLayoutData( fdStepname );

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( spacer );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    fdSpacer.width = 497;
    spacer.setLayoutData( fdSpacer );

    wlTransPath = new Label( shell, SWT.LEFT );
    props.setLook( wlTransPath );
    wlTransPath.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Transformation" ) );
    FormData fdlTransPath = new FormData();
    fdlTransPath.left = new FormAttachment( 0, 0 );
    fdlTransPath.top = new FormAttachment( spacer, 15 );
    fdlTransPath.right = new FormAttachment( 50, 0 );
    wlTransPath.setLayoutData( fdlTransPath );

    wTransPath = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransPath );
    wTransPath.addModifyListener( lsMod );
    FormData fdTransPath = new FormData();
    fdTransPath.left = new FormAttachment( 0, 0 );
    fdTransPath.right = new FormAttachment( 75, 0 );
    fdTransPath.top = new FormAttachment( wlTransPath, 5 );
    wTransPath.setLayoutData( fdTransPath );

    wbBrowseTrans = new Button( shell, SWT.PUSH );
    props.setLook( wbBrowseTrans );
    wbBrowseTrans.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Transformation.Browse" ) );
    FormData fdBrowseTrans = new FormData();
    fdBrowseTrans.left = new FormAttachment( wTransPath, 5 );
    fdBrowseTrans.top = new FormAttachment( wlTransPath, 5 );
    wbBrowseTrans.setLayoutData( fdBrowseTrans );

    wbBrowseTrans.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        if ( repository != null ) {
          selectRepositoryTrans();
        } else {
          selectFileTrans();
        }
      }
    } );

    // Start of tabbed display
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    wTabFolder.setUnselectedCloseVisible( true );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100, 0 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -5 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOK.setLayoutData( fdOk );

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( hSpacer );
    FormData fdhSpacer = new FormData();
    fdhSpacer.height = 2;
    fdhSpacer.left = new FormAttachment( 0, 0 );
    fdhSpacer.bottom = new FormAttachment( wCancel, -15 );
    fdhSpacer.right = new FormAttachment( 100, 0 );
    hSpacer.setLayoutData( fdhSpacer );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransPath, 15 );
    fdTabFolder.bottom = new FormAttachment( hSpacer, -15 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    wTabFolder.setLayoutData( fdTabFolder );

    buildSetupTab();
    buildBatchTab();
    buildFieldsTab();
    buildOptionsTab();

    lsCancel = e -> cancel();
    lsOK = e -> ok();

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wClusterName.addSelectionListener( lsDef );
    wStepname.addSelectionListener( lsDef );

    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    setSize();

    meta.setChanged( changed );

    wTabFolder.setSelection( 0 );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  private void buildSetupTab() {
    wSetupTab = new CTabItem( wTabFolder, SWT.NONE );
    wSetupTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.SetupTab" ) );

    wSetupComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

    Group wConnectionGroup = new Group( wSetupComp, SWT.SHADOW_ETCHED_IN );
    wConnectionGroup.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Connection" ) );
    FormLayout flConnection = new FormLayout();
    flConnection.marginHeight = 15;
    flConnection.marginWidth = 15;
    wConnectionGroup.setLayout( flConnection );

    FormData fdConnectionGroup = new FormData();
    fdConnectionGroup.left = new FormAttachment( 0, 0 );
    fdConnectionGroup.top = new FormAttachment( 0, 0 );
    fdConnectionGroup.right = new FormAttachment( 100, 0 );
    fdConnectionGroup.width = INPUT_WIDTH;
    wConnectionGroup.setLayoutData( fdConnectionGroup );

    props.setLook( wConnectionGroup );

    wbDirect = new Button( wConnectionGroup, SWT.RADIO );
    wbDirect.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Direct" ) );
    FormData fdbDirect = new FormData();
    fdbDirect.left = new FormAttachment( 0, 0 );
    fdbDirect.top = new FormAttachment( 0, 0 );
    wbDirect.setLayoutData( fdbDirect );
    wbDirect.addSelectionListener( new SelectionListener() {
      @Override public void widgetSelected( final SelectionEvent selectionEvent ) {
        lsMod.modifyText( null );
        toggleVisibility( true );
      }
      @Override public void widgetDefaultSelected( final SelectionEvent selectionEvent ) {
        toggleVisibility( true );
      }
    } );
    props.setLook( wbDirect );

    wbCluster = new Button( wConnectionGroup, SWT.RADIO );
    wbCluster.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Cluster" ) );
    FormData fdbCluster = new FormData();
    fdbCluster.left = new FormAttachment( 0, 0 );
    fdbCluster.top = new FormAttachment( wbDirect, 10 );
    wbCluster.setLayoutData( fdbCluster );
    wbCluster.addSelectionListener( new SelectionListener() {
      @Override public void widgetSelected( final SelectionEvent selectionEvent ) {
        lsMod.modifyText( null );
        toggleVisibility( false );
      }
      @Override public void widgetDefaultSelected( final SelectionEvent selectionEvent ) {
        toggleVisibility( false );
      }
    } );
    props.setLook( wbCluster );

    Label environmentSeparator = new Label( wConnectionGroup, SWT.SEPARATOR | SWT.VERTICAL );
    FormData fdenvironmentSeparator = new FormData();
    fdenvironmentSeparator.top = new FormAttachment( wbDirect, 0, SWT.TOP );
    fdenvironmentSeparator.left = new FormAttachment( wbCluster, 15 );
    fdenvironmentSeparator.bottom = new FormAttachment( wbCluster, 0, SWT.BOTTOM );
    environmentSeparator.setLayoutData( fdenvironmentSeparator );

    wlClusterName = new Label( wConnectionGroup, SWT.LEFT );
    props.setLook( wlClusterName );
    wlClusterName.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.HadoopCluster" ) );
    FormData fdlClusterName = new FormData();
    fdlClusterName.left = new FormAttachment( environmentSeparator, 15 );
    fdlClusterName.top = new FormAttachment( 0, 0 );
    fdlClusterName.right = new FormAttachment( 78, 0 );
    wlClusterName.setLayoutData( fdlClusterName );

    wClusterName = new ComboVar( transMeta, wConnectionGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wClusterName );
    wClusterName.addModifyListener( lsMod );
    FormData fdClusterName = new FormData();
    fdClusterName.left = new FormAttachment( wlClusterName, 0, SWT.LEFT );
    fdClusterName.top = new FormAttachment( wlClusterName, 5 );
    fdClusterName.right = new FormAttachment( 78, 0 );
    wClusterName.setLayoutData( fdClusterName );

    wlBootstrapServers = new Label( wConnectionGroup, SWT.LEFT );
    props.setLook( wlBootstrapServers );
    wlBootstrapServers.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BootstrapServers" ) );
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment( environmentSeparator, 15 );
    fdlBootstrapServers.top = new FormAttachment( 0, 0 );
    fdlBootstrapServers.right = new FormAttachment( 78, 0 );
    wlBootstrapServers.setLayoutData( fdlBootstrapServers );

    wBootstrapServers = new TextVar( transMeta, wConnectionGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBootstrapServers );
    wBootstrapServers.addModifyListener( lsMod );
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment( wlBootstrapServers, 0, SWT.LEFT );
    fdBootstrapServers.top = new FormAttachment( wlBootstrapServers, 5 );
    fdBootstrapServers.right = new FormAttachment( 78, 0 );
    wBootstrapServers.setLayoutData( fdBootstrapServers );

    wlTopic = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlTopic );
    wlTopic.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Topics" ) );
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment( 0, 0 );
    fdlTopic.top = new FormAttachment( wConnectionGroup, 10 );
    fdlTopic.right = new FormAttachment( 50, 0 );
    wlTopic.setLayoutData( fdlTopic );

    wConsumerGroup = new TextVar( transMeta, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConsumerGroup );
    wConsumerGroup.addModifyListener( lsMod );
    FormData fdConsumerGroup = new FormData();
    fdConsumerGroup.left = new FormAttachment( 0, 0 );
    fdConsumerGroup.bottom = new FormAttachment( 100, 0 );
    fdConsumerGroup.width = INPUT_WIDTH;
    wConsumerGroup.setLayoutData( fdConsumerGroup );

    wlConsumerGroup = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlConsumerGroup );
    wlConsumerGroup.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.ConsumerGroup" ) );
    FormData fdlConsumerGroup = new FormData();
    fdlConsumerGroup.left = new FormAttachment( 0, 0 );
    fdlConsumerGroup.bottom = new FormAttachment( wConsumerGroup, -5, SWT.TOP );
    fdlConsumerGroup.right = new FormAttachment( 50, 0 );
    wlConsumerGroup.setLayoutData( fdlConsumerGroup );

    buildTopicsTable( wSetupComp, wlTopic, wlConsumerGroup );

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );
  }

  public void toggleVisibility( final boolean isDirect ) {
    wlBootstrapServers.setVisible( isDirect );
    wBootstrapServers.setVisible( isDirect );
    wlClusterName.setVisible( !isDirect );
    wClusterName.setVisible( !isDirect );
  }

  private void buildBatchTab() {
    wBatchTab = new CTabItem( wTabFolder, SWT.NONE );
    wBatchTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BatchTab" ) );

    wBatchComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wBatchComp );
    FormLayout batchLayout = new FormLayout();
    batchLayout.marginHeight = 15;
    batchLayout.marginWidth = 15;
    wBatchComp.setLayout( batchLayout );

    FormData fdBatchComp = new FormData();
    fdBatchComp.left = new FormAttachment( 0, 0 );
    fdBatchComp.top = new FormAttachment( 0, 0 );
    fdBatchComp.right = new FormAttachment( 100, 0 );
    fdBatchComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdBatchComp );

    wlBatchSize = new Label( wBatchComp, SWT.LEFT );
    props.setLook( wlBatchSize );
    wlBatchSize.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BatchSize" ) );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.top = new FormAttachment( 0, 0 );
    fdlBatchSize.right = new FormAttachment( 50, 0 );
    wlBatchSize.setLayoutData( fdlBatchSize );

    wBatchSize = new TextVar( transMeta, wBatchComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( 0, 0 );
    fdBatchSize.top = new FormAttachment( wlBatchSize, 5 );
    fdBatchSize.width = 75;
    wBatchSize.setLayoutData( fdBatchSize );

    wlBatchDuration = new Label( wBatchComp, SWT.LEFT );
    props.setLook( wlBatchDuration );
    wlBatchDuration.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BatchDuration" ) );
    FormData fdlBatchDuration = new FormData();
    fdlBatchDuration.left = new FormAttachment( 0, 0 );
    fdlBatchDuration.top = new FormAttachment( wBatchSize, 10 );
    fdlBatchDuration.right = new FormAttachment( 50, 0 );
    wlBatchDuration.setLayoutData( fdlBatchDuration );

    wBatchDuration = new TextVar( transMeta, wBatchComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchDuration );
    wBatchDuration.addModifyListener( lsMod );
    FormData fdBatchDuration = new FormData();
    fdBatchDuration.left = new FormAttachment( 0, 0 );
    fdBatchDuration.top = new FormAttachment( wlBatchDuration, 5 );
    fdBatchDuration.width = 75;
    wBatchDuration.setLayoutData( fdBatchDuration );

    wBatchComp.layout();
    wBatchTab.setControl( wBatchComp );
  }

  private void buildFieldsTab() {
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.FieldsTab" ) );

    wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wFieldsComp.setLayout( fieldsLayout );

    FormData fieldsFormData = new FormData();
    fieldsFormData.left = new FormAttachment( 0, 0 );
    fieldsFormData.top = new FormAttachment( wFieldsComp, 0 );
    fieldsFormData.right = new FormAttachment( 100, 0 );
    fieldsFormData.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fieldsFormData );

    buildFieldTable( wFieldsComp, wFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );
  }

  private void buildOptionsTab() {
    wOptionsTab = new CTabItem( wTabFolder, SWT.NONE );
    wOptionsTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.OptionsTab" ) );

    wOptionsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wOptionsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wOptionsComp.setLayout( fieldsLayout );

    Label optionsLabel = new Label( wOptionsComp, SWT.LEFT );
    props.setLook( optionsLabel );
    optionsLabel.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.OptionsLabel" ) );
    FormData fdlOptions = new FormData();
    fdlOptions.left = new FormAttachment( 0, 0 );
    fdlOptions.top = new FormAttachment( 0, 0 );
    fdlOptions.right = new FormAttachment( 50, 0 );
    optionsLabel.setLayoutData( fdlOptions );

    FormData optionsFormData = new FormData();
    optionsFormData.left = new FormAttachment( 0, 0 );
    optionsFormData.top = new FormAttachment( wOptionsComp, 0 );
    optionsFormData.right = new FormAttachment( 100, 0 );
    optionsFormData.bottom = new FormAttachment( 100, 0 );
    wOptionsComp.setLayoutData( optionsFormData );

    buildOptionsTable( wOptionsComp, optionsLabel );

    wOptionsComp.layout();
    wOptionsTab.setControl( wOptionsComp );
  }

  private void buildFieldTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getFieldColumns();

    int fieldCount = KafkaConsumerField.Name.values().length;

    fieldsTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      fieldCount,
      true,
      lsMod,
      props,
      false
    );

    fieldsTable.setSortable( false );
    fieldsTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 147 );
      table.getColumn( 2 ).setWidth( 147 );
      table.getColumn( 3 ).setWidth( 147 );
    } );

    populateFieldData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.right = new FormAttachment( 100, 0 );

    // resize the columns to fit the data in them
    Arrays.stream( fieldsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    // don't let any rows get deleted or added (this does not affect the read-only state of the cells)
    fieldsTable.setReadonly( true );
    fieldsTable.setLayoutData( fdData );
  }

  private void buildOptionsTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getOptionsColumns();

    if ( meta.getConfig().size() == 0 ) {
      // inital call
      List<String> list = KafkaDialogHelper.getConsumerAdvancedConfigOptionNames();
      Map<String, String> advancedConfig = new LinkedHashMap<>();
      for ( String item : list ) {
        advancedConfig.put( item, DEFAULT_OPTION_VALUES.getOrDefault( item, "" ) );
      }
      meta.setConfig( advancedConfig );
    }
    int fieldCount = meta.getConfig().size();

    optionsTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      fieldCount,
      false,
      lsMod,
      props,
      false
    );

    optionsTable.setSortable( false );
    optionsTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 220 );
      table.getColumn( 2 ).setWidth( 220 );
    } );

    populateOptionsData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.right = new FormAttachment( 100, 0 );
    fdData.bottom = new FormAttachment( 100, 0 );

    // resize the columns to fit the data in them
    Arrays.stream( optionsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    optionsTable.setLayoutData( fdData );
  }

  private ColumnInfo[] getFieldColumns() {
    KafkaConsumerField.Type[] values = KafkaConsumerField.Type.values();
    String[] supportedTypes = Arrays.stream( values ).map( v -> v.toString() ).toArray( String[]::new );

    ColumnInfo referenceName = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Ref" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, true );

    ColumnInfo name = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Name" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo type = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Type" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, supportedTypes, false );

    // don't let the user edit the type for anything other than key & msg fields
    type.setDisabledListener( rowNumber -> {
      String ref = fieldsTable.getTable().getItem( rowNumber ).getText( 1 );
      KafkaConsumerField.Name refName = KafkaConsumerField.Name.valueOf( ref.toUpperCase() );

      return !( refName == KafkaConsumerField.Name.KEY || refName == KafkaConsumerField.Name.MESSAGE );
    } );

    return new ColumnInfo[]{ referenceName, name, type };
  }

  private ColumnInfo[] getOptionsColumns() {

    ColumnInfo optionName = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.NameField" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo value = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Value" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    value.setUsingVariables( true );

    return new ColumnInfo[]{ optionName, value };
  }

  private void populateFieldData() {
    List<KafkaConsumerField> fieldDefinitions = meta.getFieldDefinitions();
    int rowIndex = 0;
    for ( KafkaConsumerField field : fieldDefinitions ) {
      TableItem key = fieldsTable.getTable().getItem( rowIndex++ );

      if ( field.getKafkaName() != null ) {
        key.setText( 1, field.getKafkaName().toString() );
      }

      if ( field.getOutputName() != null ) {
        key.setText( 2, field.getOutputName() );
      }

      if ( field.getOutputType() != null ) {
        key.setText( 3, field.getOutputType().toString() );
      }
    }
  }

  private void populateOptionsData() {
    int rowIndex = 0;
    for ( Map.Entry<String, String> entry : meta.getConfig().entrySet() ) {
      TableItem key = optionsTable.getTable().getItem( rowIndex++ );
      key.setText( 1, entry.getKey() );
      key.setText( 2, entry.getValue() );
    }
  }

  private void populateTopicsData() {
    List<String> topics = meta.getTopics();
    int rowIndex = 0;
    for ( String topic : topics ) {
      TableItem key = topicsTable.getTable().getItem( rowIndex++ );
      if ( topic != null ) {
        key.setText( 1, topic );
      }
    }
  }

  private void buildTopicsTable( Composite parentWidget, Control controlAbove, Control controlBelow ) {
    ColumnInfo[] columns = new ColumnInfo[]{ new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.NameField" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, new String[1], false ) };

    columns[0].setUsingVariables( true );

    int topicsCount = meta.getTopics().size();

    Listener lsFocusInTopic = e -> {
      CCombo ccom = (CCombo) e.widget;
      ComboVar cvar = (ComboVar) ccom.getParent();

      KafkaDialogHelper kdh = new KafkaDialogHelper( wClusterName, cvar, wbCluster, wBootstrapServers, kafkaFactory,
        meta.getNamedClusterService(), meta.getNamedClusterServiceLocator(), meta.getMetastoreLocator(), optionsTable );
      kdh.clusterNameChanged( e );
    };

    topicsTable = new TableView(
      transMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      topicsCount,
      false,
      lsMod,
      props,
      false,
      true,
      lsFocusInTopic
    );

    topicsTable.setSortable( false );
    topicsTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 316 );
    } );

    populateTopicsData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( controlAbove, 5 );
    fdData.right = new FormAttachment( 0, 337 );
    fdData.bottom = new FormAttachment( controlBelow, -10, SWT.TOP );

    // resize the columns to fit the data in them
    Arrays.stream( topicsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    topicsTable.setLayoutData( fdData );
  }

  private void getData() {
    if ( meta.getTransformationPath() != null ) {
      wTransPath.setText( meta.getTransformationPath() );
    }

    try {
      List<String> names = meta.getNamedClusterService().listNames( Spoon.getInstance().getMetaStore() );
      wClusterName.setItems( names.toArray( new String[ names.size() ] ) );
    } catch ( MetaStoreException e ) {
      log.logError( "Failed to get defined named clusters", e );
    }

    if ( meta.getClusterName() != null ) {
      wClusterName.setText( meta.getClusterName() );
    }

    if ( meta.getDirectBootstrapServers() != null ) {
      wBootstrapServers.setText( meta.getDirectBootstrapServers() );
    }

    populateTopicsData();

    if ( meta.getConsumerGroup() != null ) {
      wConsumerGroup.setText( meta.getConsumerGroup() );
    }

    if ( meta.getBatchSize() != null ) {
      wBatchSize.setText( meta.getBatchSize() );
    }
    if ( meta.getBatchDuration() != null ) {
      wBatchDuration.setText( meta.getBatchDuration() );
    }
    if ( isDirect() ) {
      wbCluster.setSelection( false );
      wbDirect.setSelection( true );
    } else {
      wbCluster.setSelection( true );
      wbDirect.setSelection( false );
    }
    toggleVisibility( isDirect() );

    populateFieldData();
  }

  private boolean isDirect() {
    return DIRECT.equals( meta.getConnectionType() );
  }

  private Image getImage() {
    PluginInterface plugin =
      PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[ 0 ];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(),
        ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }

  private void cancel() {
    meta.setChanged( false );
    dispose();
  }

  private void ok() {
    stepname = wStepname.getText();
    meta.setTransformationPath( wTransPath.getText() );
    meta.setClusterName( wClusterName.getText() );

    setTopicsFromTable();

    meta.setConsumerGroup( wConsumerGroup.getText() );
    meta.setBatchSize( wBatchSize.getText() );
    meta.setBatchDuration( wBatchDuration.getText() );
    meta.setConnectionType( wbDirect.getSelection() ? DIRECT : CLUSTER );
    meta.setDirectBootstrapServers( wBootstrapServers.getText() );

    setFieldsFromTable();

    setOptionsFromTable();

    dispose();
  }

  private void setFieldsFromTable() {
    int itemCount = fieldsTable.getItemCount();
    for ( int rowIndex = 0; rowIndex < itemCount; rowIndex++ ) {
      TableItem row = fieldsTable.getTable().getItem( rowIndex );
      String kafkaName = row.getText( 1 );
      String outputName = row.getText( 2 );
      String outputType = row.getText( 3 );
      try {
        KafkaConsumerField.Name ref = KafkaConsumerField.Name.valueOf( kafkaName.toUpperCase() );
        KafkaConsumerField field = new KafkaConsumerField(
          ref,
          outputName,
          KafkaConsumerField.Type.valueOf( outputType )
        );
        meta.setField( field );
      } catch ( IllegalArgumentException e ) {
        if ( isDebug() ) {
          logDebug( e.getMessage(), e );
        }
      }
    }
  }

  private void setTopicsFromTable() {
    int itemCount = topicsTable.getItemCount();
    ArrayList<String> tableTopics = new ArrayList<String>();
    for ( int rowIndex = 0; rowIndex < itemCount; rowIndex++ ) {
      TableItem row = topicsTable.getTable().getItem( rowIndex );
      String topic = row.getText( 1 );
      if ( !"".equals( topic ) && tableTopics.indexOf( topic ) == -1 ) {
        tableTopics.add( topic );
      }
    }
    meta.setTopics( tableTopics );
  }

  private void setOptionsFromTable() {
    meta.setConfig( KafkaDialogHelper.getConfig( optionsTable ) );
  }

  private void selectRepositoryTrans() {
    try {
      SelectObjectDialog sod = new SelectObjectDialog( shell, repository );
      String transName = sod.open();
      RepositoryDirectoryInterface repdir = sod.getDirectory();
      if ( transName != null && repdir != null ) {
        loadRepositoryTrans( transName, repdir );
        String path = getPath( executorTransMeta.getRepositoryDirectory().getPath() );
        String fullPath = path + "/" + executorTransMeta.getName();
        wTransPath.setText( fullPath );
        specificationMethod = ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME;
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "TransExecutorDialog.ErrorSelectingObject.DialogTitle" ),
        BaseMessages.getString( PKG, "TransExecutorDialog.ErrorSelectingObject.DialogMessage" ), ke );
    }
  }

  protected String getPath( String path ) {
    String parentPath = this.transMeta.getRepositoryDirectory().getPath();
    if ( path.startsWith( parentPath ) ) {
      path = path.replace( parentPath, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}" );
    }
    return path;
  }

  private void loadRepositoryTrans( String transName, RepositoryDirectoryInterface repdir ) throws KettleException {
    // Read the transformation...
    //
    executorTransMeta =
      repository.loadTransformation( transMeta.environmentSubstitute( transName ), repdir, null, false, null );
    executorTransMeta.clearChanged();
  }

  private void selectFileTrans() {
    String curFile = transMeta.environmentSubstitute( wTransPath.getText() );

    FileObject root = null;

    String parentFolder = null;
    try {
      parentFolder =
        KettleVFS.getFileObject( transMeta.environmentSubstitute( transMeta.getFilename() ) ).getParent().toString();
    } catch ( Exception e ) {
      // Take no action
    }

    try {
      root = KettleVFS.getFileObject( curFile != null ? curFile : Const.getUserHomeDirectory() );

      VfsFileChooserDialog vfsFileChooser = Spoon.getInstance().getVfsFileChooserDialog( root.getParent(), root );
      FileObject file =
        vfsFileChooser.open(
          shell, null, Const.STRING_TRANS_FILTER_EXT, Const.getTransformationFilterNames(),
          VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE );
      if ( file == null ) {
        return;
      }
      String fileName = file.getName().toString();
      if ( fileName != null ) {
        loadFileTrans( fileName );
        if ( parentFolder != null && fileName.startsWith( parentFolder ) ) {
          fileName = fileName.replace( parentFolder, "${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_DIRECTORY + "}" );
        }
        wTransPath.setText( fileName );
        specificationMethod = ObjectLocationSpecificationMethod.FILENAME;
      }
    } catch ( IOException | KettleException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "TransExecutorDialog.ErrorLoadingTransformation.DialogTitle" ),
        BaseMessages.getString( PKG, "TransExecutorDialog.ErrorLoadingTransformation.DialogMessage" ), e );
    }
  }

  private void loadFileTrans( String fname ) throws KettleException {
    executorTransMeta = new TransMeta( transMeta.environmentSubstitute( fname ), repository );
    executorTransMeta.clearChanged();
  }

  // Method is defined as package-protected in order to be accessible by unit tests
  void loadTransformation() throws KettleException {
    String filename = wTransPath.getText();
    if ( repository != null ) {
      specificationMethod = ObjectLocationSpecificationMethod.REPOSITORY_BY_NAME;
    } else {
      specificationMethod = ObjectLocationSpecificationMethod.FILENAME;
    }
    switch ( specificationMethod ) {
      case FILENAME:
        if ( Utils.isEmpty( filename ) ) {
          return;
        }
        if ( !filename.endsWith( ".ktr" ) ) {
          filename = filename + ".ktr";
          wTransPath.setText( filename );
        }
        loadFileTrans( filename );
        break;
      case REPOSITORY_BY_NAME:
        if ( Utils.isEmpty( filename ) ) {
          return;
        }
        if ( filename.endsWith( ".ktr" ) ) {
          filename = filename.replace( ".ktr", "" );
          wTransPath.setText( filename );
        }
        String transPath = transMeta.environmentSubstitute( filename );
        String realTransname = transPath;
        String realDirectory = "";
        int index = transPath.lastIndexOf( "/" );
        if ( index != -1 ) {
          realTransname = transPath.substring( index + 1 );
          realDirectory = transPath.substring( 0, index );
        }

        if ( Utils.isEmpty( realDirectory ) || Utils.isEmpty( realTransname ) ) {
          throw new KettleException(
            BaseMessages.getString( PKG, "TransExecutorDialog.Exception.NoValidMappingDetailsFound" ) );
        }
        RepositoryDirectoryInterface repdir = repository.findDirectory( realDirectory );
        if ( repdir == null ) {
          throw new KettleException( BaseMessages.getString(
            PKG, "TransExecutorDialog.Exception.UnableToFindRepositoryDirectory" ) );
        }
        loadRepositoryTrans( realTransname, repdir );
        break;
      default:
        break;
    }
  }
}

