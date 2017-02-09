/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.HBaseServiceLocalImp;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.ConfigurationProducer;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingAdmin;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingEditor;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.bigdata.api.hbase.ByteConversionUtil;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboValuesSelectionListener;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Dialog class for HBaseInput
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class HBaseInputDialog extends BaseStepDialog implements StepDialogInterface, ConfigurationProducer {

  /** various UI bits and pieces for the dialog */
  private Label m_stepnameLabel;
  private Text m_stepnameText;

  // The tabs of the dialog
  private CTabFolder m_wTabFolder;
  private CTabItem m_wConfigTab;

  private CTabItem m_wFilterTab;

  private CTabItem m_editorTab;

  NamedClusterWidgetImpl namedClusterWidget;

  // Core config line
  private Button m_coreConfigBut;
  private TextVar m_coreConfigText;

  // Default config line
  private Button m_defaultConfigBut;
  private TextVar m_defaultConfigText;

  private final HBaseInputMeta m_currentMeta;
  private final HBaseInputMeta m_originalMeta;
  private final HBaseInputMeta m_configurationMeta;

  // Table name line
  private Button m_mappedTableNamesBut;
  private CCombo m_mappedTableNamesCombo;

  // Mapping name line
  private Button m_mappingNamesBut;
  private CCombo m_mappingNamesCombo;

  /** Store the mapping information in the step's meta data */
  private Button m_storeMappingInStepMetaData;

  // Key start line
  private TextVar m_keyStartText;

  // Key stop line
  private TextVar m_keyStopText;

  // Rows to be cached by Scanner
  private TextVar m_scanCacheText;

  // Key as a column
  // private Button m_includeKey;

  // Key information
  private String m_keyName;
  private Mapping.KeyType m_keyType;
  private Label m_keyInfo;
  private Button m_getKeyInfoBut;

  // Fields table widget
  private TableView m_fieldsView;

  // filters fields widget
  private TableView m_filtersView;
  private ColumnInfo m_filterAliasCI;
  private Button m_matchAllBut;
  private Button m_matchAnyBut;

  // mapping editor composite
  private MappingEditor m_mappingEditor;

  // cached copy of the mapped columns
  private Map<String, HBaseValueMetaInterface> m_mappedColumns;

  // lookup map for indexed columns
  private Map<String, String> m_indexedLookup = new HashMap<>();
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private HBaseService hBaseLocalService;

  public HBaseInputDialog( Shell parent, Object in, TransMeta tr, String name ) {

    super( parent, (BaseStepMeta) in, tr, name );

    m_currentMeta = (HBaseInputMeta) in;
    m_originalMeta = (HBaseInputMeta) m_currentMeta.clone();
    m_configurationMeta = (HBaseInputMeta) m_currentMeta.clone();
    namedClusterService = m_currentMeta.getNamedClusterService();
    runtimeTestActionService = m_currentMeta.getRuntimeTestActionService();
    runtimeTester = m_currentMeta.getRuntimeTester();
    namedClusterServiceLocator = m_currentMeta.getNamedClusterServiceLocator();
    hBaseLocalService = new HBaseServiceLocalImp();
  }

  private HBaseService getHBaseLocalService() {
    return hBaseLocalService;
  }

  public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );

    props.setLook( shell );
    setShellImage( shell, m_currentMeta );

    // used to listen to a text field (m_wStepname)
    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
      }
    };

    changed = m_currentMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( Messages.getString( "HBaseInputDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Stepname line
    m_stepnameLabel = new Label( shell, SWT.RIGHT );
    m_stepnameLabel.setText( Messages.getString( "HBaseInputDialog.StepName.Label" ) );
    props.setLook( m_stepnameLabel );

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    fd.top = new FormAttachment( 0, margin );
    m_stepnameLabel.setLayoutData( fd );
    m_stepnameText = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_stepnameText.setText( stepname );
    props.setLook( m_stepnameText );
    m_stepnameText.addModifyListener( lsMod );

    // format the text field
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, 0 );
    m_stepnameText.setLayoutData( fd );

    m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );
    m_wTabFolder.setSimple( false );

    // Start of the config tab
    m_wConfigTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wConfigTab.setText( Messages.getString( "HBaseInputDialog.ConfigTab.TabTitle" ) );

    Composite wConfigComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wConfigComp );

    FormLayout configLayout = new FormLayout();
    configLayout.marginWidth = 3;
    configLayout.marginHeight = 3;
    wConfigComp.setLayout( configLayout );

    Label namedClusterLab = new Label( wConfigComp, SWT.RIGHT );
    namedClusterLab.setText( Messages.getString( "HBaseInputDialog.NamedCluster.Label" ) );
    namedClusterLab.setToolTipText( Messages.getString( "HBaseInputDialog.NamedCluster.TipText" ) );
    props.setLook( namedClusterLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 10 );
    fd.right = new FormAttachment( middle, -margin );
    namedClusterLab.setLayoutData( fd );

    namedClusterWidget = new NamedClusterWidgetImpl( wConfigComp, false, namedClusterService, runtimeTestActionService, runtimeTester );
    namedClusterWidget.initiate();
    props.setLook( namedClusterWidget );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( middle, 0 );
    namedClusterWidget.setLayoutData( fd );

    // core config line
    Label coreConfigLab = new Label( wConfigComp, SWT.RIGHT );
    coreConfigLab.setText( Messages.getString( "HBaseInputDialog.CoreConfig.Label" ) );
    coreConfigLab.setToolTipText( Messages.getString( "HBaseInputDialog.CoreConfig.TipText" ) );
    props.setLook( coreConfigLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( namedClusterWidget, margin );
    fd.right = new FormAttachment( middle, -margin );
    coreConfigLab.setLayoutData( fd );

    m_coreConfigBut = new Button( wConfigComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_coreConfigBut );
    m_coreConfigBut.setText( Messages.getString( "System.Button.Browse" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( namedClusterWidget, 0 );
    m_coreConfigBut.setLayoutData( fd );

    m_coreConfigBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        String[] extensions = null;
        String[] filterNames = null;

        extensions = new String[2];
        filterNames = new String[2];
        extensions[0] = "*.xml";
        filterNames[0] = Messages.getString( "HBaseInputDialog.FileType.XML" );
        extensions[1] = "*";
        filterNames[1] = Messages.getString( "System.FileType.AllFiles" );

        dialog.setFilterExtensions( extensions );

        if ( dialog.open() != null ) {
          m_coreConfigText.setText( dialog.getFilterPath() + System.getProperty( "file.separator" )
              + dialog.getFileName() );
        }

      }
    } );

    m_coreConfigText = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_coreConfigText );
    m_coreConfigText.addModifyListener( lsMod );

    // set the tool tip to the contents with any env variables expanded
    m_coreConfigText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_coreConfigText.setToolTipText( transMeta.environmentSubstitute( m_coreConfigText.getText() ) );
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( namedClusterWidget, margin );
    fd.right = new FormAttachment( m_coreConfigBut, -margin );
    m_coreConfigText.setLayoutData( fd );

    // default config line
    Label defaultConfigLab = new Label( wConfigComp, SWT.RIGHT );
    defaultConfigLab.setText( Messages.getString( "HBaseInputDialog.DefaultConfig.Label" ) );
    defaultConfigLab.setToolTipText( Messages.getString( "HBaseInputDialog.DefaultConfig.TipText" ) );
    props.setLook( defaultConfigLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_coreConfigText, margin );
    fd.right = new FormAttachment( middle, -margin );
    defaultConfigLab.setLayoutData( fd );

    m_defaultConfigBut = new Button( wConfigComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_defaultConfigBut );
    m_defaultConfigBut.setText( Messages.getString( "System.Button.Browse" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_coreConfigText, 0 );
    m_defaultConfigBut.setLayoutData( fd );

    m_defaultConfigBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        String[] extensions = null;
        String[] filterNames = null;

        extensions = new String[2];
        filterNames = new String[2];
        extensions[0] = "*.xml";
        filterNames[0] = Messages.getString( "HBaseInputDialog.FileType.XML" );
        extensions[1] = "*";
        filterNames[1] = Messages.getString( "System.FileType.AllFiles" );

        dialog.setFilterExtensions( extensions );

        if ( dialog.open() != null ) {
          m_defaultConfigText.setText( dialog.getFilterPath() + System.getProperty( "file.separator" )
              + dialog.getFileName() );
        }

      }
    } );

    m_defaultConfigText = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_defaultConfigText );
    m_defaultConfigText.addModifyListener( lsMod );

    // set the tool tip to the contents with any env variables expanded
    m_defaultConfigText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_defaultConfigText.setToolTipText( transMeta.environmentSubstitute( m_defaultConfigText.getText() ) );
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_coreConfigText, margin );
    fd.right = new FormAttachment( m_defaultConfigBut, -margin );
    m_defaultConfigText.setLayoutData( fd );

    // table name
    Label tableNameLab = new Label( wConfigComp, SWT.RIGHT );
    tableNameLab.setText( Messages.getString( "HBaseInputDialog.TableName.Label" ) );
    tableNameLab.setToolTipText( Messages.getString( "HBaseInputDialog.TableName.TipText" ) );
    props.setLook( tableNameLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_defaultConfigText, margin );
    fd.right = new FormAttachment( middle, -margin );
    tableNameLab.setLayoutData( fd );

    m_mappedTableNamesBut = new Button( wConfigComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_mappedTableNamesBut );
    m_mappedTableNamesBut.setText( Messages.getString( "HBaseInputDialog.TableName.Button" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_defaultConfigText, 0 );
    m_mappedTableNamesBut.setLayoutData( fd );

    m_mappedTableNamesCombo = new CCombo( wConfigComp, SWT.BORDER );
    props.setLook( m_mappedTableNamesCombo );

    m_mappedTableNamesCombo.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
        m_mappedTableNamesCombo.setToolTipText( transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() ) );
      }
    } );

    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_defaultConfigText, margin );
    fd.right = new FormAttachment( m_mappedTableNamesBut, -margin );
    m_mappedTableNamesCombo.setLayoutData( fd );

    m_mappedTableNamesBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setupMappedTableNames();
      }
    } );

    // mapping name
    Label mappingNameLab = new Label( wConfigComp, SWT.RIGHT );
    mappingNameLab.setText( Messages.getString( "HBaseInputDialog.MappingName.Label" ) );
    mappingNameLab.setToolTipText( Messages.getString( "HBaseInputDialog.MappingName.TipText" ) );
    props.setLook( mappingNameLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_mappedTableNamesCombo, margin );
    fd.right = new FormAttachment( middle, -margin );
    mappingNameLab.setLayoutData( fd );

    m_mappingNamesBut = new Button( wConfigComp, SWT.PUSH | SWT.CENTER );
    props.setLook( m_mappingNamesBut );
    m_mappingNamesBut.setText( Messages.getString( "HBaseInputDialog.MappingName.Button" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_mappedTableNamesCombo, 0 );
    m_mappingNamesBut.setLayoutData( fd );

    m_mappingNamesBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setupMappingNamesForTable( false );
      }
    } );

    m_mappingNamesCombo = new CCombo( wConfigComp, SWT.BORDER );
    props.setLook( m_mappingNamesCombo );

    m_mappingNamesCombo.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_currentMeta.setChanged();
        // checkKeyInformation(true);

        m_mappingNamesCombo.setToolTipText( transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) );
        m_storeMappingInStepMetaData.setSelection( false );
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_mappedTableNamesCombo, margin );
    fd.right = new FormAttachment( m_mappingNamesBut, -margin );
    m_mappingNamesCombo.setLayoutData( fd );

    // store mapping in meta data
    Label storeMapping = new Label( wConfigComp, SWT.RIGHT );
    storeMapping.setText( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInputDialog.StoreMapping.Label" ) );
    storeMapping.setToolTipText(
        BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInputDialog.StoreMapping.TipText" ) );
    props.setLook( storeMapping );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_mappingNamesCombo, margin );
    fd.right = new FormAttachment( middle, -margin );
    storeMapping.setLayoutData( fd );

    m_storeMappingInStepMetaData = new Button( wConfigComp, SWT.CHECK );
    props.setLook( m_storeMappingInStepMetaData );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_mappingNamesCombo, margin );
    m_storeMappingInStepMetaData.setLayoutData( fd );

    // keystart
    Label keyStartLab = new Label( wConfigComp, SWT.RIGHT );
    keyStartLab.setText( Messages.getString( "HBaseInputDialog.KeyStart.Label" ) );
    keyStartLab.setToolTipText( Messages.getString( "HBaseInputDialog.KeyStart.TipText" ) );
    props.setLook( keyStartLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_storeMappingInStepMetaData, margin );
    fd.right = new FormAttachment( middle, -margin );
    keyStartLab.setLayoutData( fd );

    m_keyStartText = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_keyStartText.setToolTipText( Messages.getString( "HBaseInputDialog.KeyStart.TipText" ) );
    m_keyStartText.addModifyListener( lsMod );
    props.setLook( m_keyStartText );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_storeMappingInStepMetaData, margin );
    m_keyStartText.setLayoutData( fd );

    // keystop
    Label keyStopLab = new Label( wConfigComp, SWT.RIGHT );
    keyStopLab.setText( Messages.getString( "HBaseInputDialog.KeyStop.Label" ) );
    keyStopLab.setToolTipText( Messages.getString( "HBaseInputDialog.KeyStop.TipText" ) );
    props.setLook( keyStopLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_keyStartText, margin );
    fd.right = new FormAttachment( middle, -margin );
    keyStopLab.setLayoutData( fd );

    m_keyStopText = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_keyStopText.setToolTipText( Messages.getString( "HBaseInputDialog.KeyStop.TipText" ) );
    m_keyStopText.addModifyListener( lsMod );
    props.setLook( m_keyStopText );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_keyStartText, margin );
    m_keyStopText.setLayoutData( fd );

    // Scanner caching
    Label scannerCacheLab = new Label( wConfigComp, SWT.RIGHT );
    scannerCacheLab.setText( Messages.getString( "HBaseInputDialog.ScannerCache.Label" ) );
    scannerCacheLab.setToolTipText( Messages.getString( "HBaseInputDialog.ScannerCache.TipText" ) );
    props.setLook( scannerCacheLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_keyStopText, margin );
    fd.right = new FormAttachment( middle, -margin );
    scannerCacheLab.setLayoutData( fd );

    m_scanCacheText = new TextVar( transMeta, wConfigComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    m_scanCacheText.setToolTipText( Messages.getString( "HBaseInputDialog.ScannerCache.TipText" ) );
    props.setLook( m_scanCacheText );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_keyStopText, margin );
    m_scanCacheText.setLayoutData( fd );

    m_getKeyInfoBut = new Button( wConfigComp, SWT.PUSH );
    m_getKeyInfoBut.setText( "Get Key/Fields Info" );
    props.setLook( m_getKeyInfoBut );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    m_getKeyInfoBut.setLayoutData( fd );
    m_getKeyInfoBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        checkKeyInformation( false, true );
      }
    } );

    Group keyGroup = new Group( wConfigComp, SWT.SHADOW_ETCHED_IN );
    FormLayout keyLayout = new FormLayout();
    keyGroup.setLayout( keyLayout );
    props.setLook( keyGroup );

    m_keyInfo = new Label( keyGroup, SWT.RIGHT );
    m_keyInfo.setText( "-- Key details --" );
    props.setLook( m_keyInfo );
    fd = new FormData();
    fd.top = new FormAttachment( 0, margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, -margin );
    m_keyInfo.setLayoutData( fd );

    fd = new FormData();
    fd.right = new FormAttachment( m_getKeyInfoBut, -margin );
    fd.left = new FormAttachment( middle, 0 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    keyGroup.setLayoutData( fd );

    // fields stuff
    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_ALIAS" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_KEY" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_FAMILY" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_NAME" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_TYPE" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_FORMAT" ), ColumnInfo.COLUMN_TYPE_FORMAT,
              3 ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Fields.FIELD_INDEXED" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ), };

    colinf[0].setReadOnly( true );
    colinf[1].setReadOnly( true );
    colinf[2].setReadOnly( true );
    colinf[3].setReadOnly( true );
    colinf[4].setReadOnly( true );
    colinf[5].setReadOnly( true );

    colinf[5].setComboValuesSelectionListener( new ComboValuesSelectionListener() {

      public String[] getComboValues( TableItem tableItem, int rowNr, int colNr ) {
        String[] comboValues = new String[] {};
        int type = ValueMeta.getType( tableItem.getText( colNr - 1 ) );
        switch ( type ) {
          case ValueMetaInterface.TYPE_DATE:
            comboValues = Const.getDateFormats();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
          case ValueMetaInterface.TYPE_BIGNUMBER:
          case ValueMetaInterface.TYPE_NUMBER:
            comboValues = Const.getNumberFormats();
            break;
          default:
            break;
        }
        return comboValues;
      }
    } );

    m_fieldsView = new TableView( transMeta, wConfigComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( m_scanCacheText, margin * 2 );
    fd.bottom = new FormAttachment( m_getKeyInfoBut, -margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_fieldsView.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wConfigComp.setLayoutData( fd );

    wConfigComp.layout();
    m_wConfigTab.setControl( wConfigComp );

    // --- mapping editor tab
    m_editorTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_editorTab.setText( Messages.getString( "HBaseInputDialog.MappingEditorTab.TabTitle" ) );

    m_mappingEditor =
      new MappingEditor( shell, m_wTabFolder, this, null, SWT.FULL_SELECTION | SWT.MULTI, false, props, transMeta,
        namedClusterService, runtimeTestActionService, runtimeTester, namedClusterServiceLocator );

    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( 0, 0 );
    m_mappingEditor.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.right = new FormAttachment( 100, 0 );
    m_mappingEditor.setLayoutData( fd );

    m_mappingEditor.layout();
    m_editorTab.setControl( m_mappingEditor );

    // ----- Start of the filter tab --------
    m_wFilterTab = new CTabItem( m_wTabFolder, SWT.NONE );
    m_wFilterTab.setText( Messages.getString( "HBaseInputDialog.FilterTab.TabTitle" ) );

    Composite wFilterComp = new Composite( m_wTabFolder, SWT.NONE );
    props.setLook( wFilterComp );

    FormLayout filterLayout = new FormLayout();
    filterLayout.marginWidth = 3;
    filterLayout.marginHeight = 3;
    wFilterComp.setLayout( filterLayout );

    m_matchAllBut = new Button( wFilterComp, SWT.RADIO );
    m_matchAllBut.setText( Messages.getString( "HBaseInputDialog.Filters.RADIO_ALL" ) );
    props.setLook( m_matchAllBut );
    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( 0, 0 );
    m_matchAllBut.setLayoutData( fd );
    m_matchAllBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
      }
    } );

    m_matchAnyBut = new Button( wFilterComp, SWT.RADIO );
    m_matchAnyBut.setText( Messages.getString( "HBaseInputDialog.Filters.RADIO_ANY" ) );
    props.setLook( m_matchAnyBut );
    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( m_matchAllBut, 0 );
    fd.right = new FormAttachment( 100, -margin );
    m_matchAnyBut.setLayoutData( fd );
    m_matchAnyBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        m_currentMeta.setChanged();
      }
    } );

    m_matchAllBut.setSelection( true );

    final ColumnInfo[] colinf2 =
        new ColumnInfo[] {
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Filters.FIELD_ALIAS" ) + "     ",
              ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Filters.FIELD_TYPE" ), ColumnInfo.COLUMN_TYPE_TEXT,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Filters.FIELD_OPERATOR" ),
              ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Filters.FIELD_COMPARISON" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Filters.FIELD_FORMAT" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
              false ),
          new ColumnInfo( Messages.getString( "HBaseInputDialog.Filters.FIELD_SIGNED" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
              false ), };

    colinf2[0].setReadOnly( false );
    colinf2[1].setReadOnly( false );
    colinf2[2].setReadOnly( true );
    colinf2[3].setReadOnly( false );
    colinf2[4].setReadOnly( false );
    colinf2[5].setReadOnly( true );

    m_filterAliasCI = colinf2[0];
    m_filterAliasCI.setComboValues( new String[] { "" } );
    colinf2[2].setComboValues( ColumnFilter.ComparisonType.getAllOperators() );
    colinf2[5].setComboValues( new String[] { "Y", "N" } );

    colinf2[2].setComboValuesSelectionListener( new ComboValuesSelectionListener() {

      public String[] getComboValues( TableItem tableItem, int rowNr, int colNr ) {
        String[] comboValues = colinf2[2].getComboValues();

        // try to fill in the type
        String alias = tableItem.getText( 1 );
        HBaseValueMetaInterface vm = null;
        if ( !Utils.isEmpty( alias ) ) {
          vm = setFilterTableTypeColumn( tableItem );
        }

        if ( vm != null ) {
          if ( vm.isNumeric() || vm.isDate() || vm.isBoolean() ) {
            comboValues = ColumnFilter.ComparisonType.getNumericOperators();
          } else if ( vm.isString() ) {
            comboValues = ColumnFilter.ComparisonType.getStringOperators();
          } else {
            comboValues = new String[1];
            comboValues[0] = "";
          }
        } else {
          // if we've not got a connection, or there is no user-specified
          // columns saved in the meta class, then just get all the
          // operators
          comboValues = ColumnFilter.ComparisonType.getAllOperators();
        }

        return comboValues;
      }
    } );

    colinf2[4].setComboValuesSelectionListener( new ComboValuesSelectionListener() {
      public String[] getComboValues( TableItem tableItem, int rowNr, int colNr ) {
        String[] comboValues = new String[] {};

        // try to fill in the type
        String alias = tableItem.getText( 1 );
        if ( !Utils.isEmpty( alias ) ) {
          setFilterTableTypeColumn( tableItem );
        }
        int type = ValueMeta.getType( tableItem.getText( 2 ) );
        switch ( type ) {
          case ValueMetaInterface.TYPE_DATE:
            comboValues = Const.getDateFormats();
            break;
          case ValueMetaInterface.TYPE_INTEGER:
          case ValueMetaInterface.TYPE_BIGNUMBER:
          case ValueMetaInterface.TYPE_NUMBER:
            comboValues = Const.getNumberFormats();
            break;
          default:
            break;
        // if there is not type information available (no connection and no
        // user-specified
        // columns in the meta class) then the user will just have to type
        // in their own
        // formatting string (if necessary)
        }
        return comboValues;
      }
    } );

    m_filtersView = new TableView( transMeta, wFilterComp, SWT.FULL_SELECTION | SWT.MULTI, colinf2, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( m_matchAllBut, margin * 2 );
    fd.bottom = new FormAttachment( 100, -margin * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_filtersView.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wFilterComp.setLayoutData( fd );

    wFilterComp.layout();
    m_wFilterTab.setControl( wFilterComp );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_stepnameText, margin );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, -50 );
    m_wTabFolder.setLayoutData( fd );

    // Buttons inherited from BaseStepDialog
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( Messages.getString( "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( Messages.getString( "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel }, margin, m_wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );
    wOK.addListener( SWT.Selection, lsOK );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    m_stepnameText.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    m_wTabFolder.setSelection( 0 );
    setSize();

    getData();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }

    return stepname;
  }

  protected HBaseValueMetaInterface setFilterTableTypeColumn( TableItem tableItem ) {
    // try to fill in the type
    String alias = tableItem.getText( 1 ).trim();
    if ( !Utils.isEmpty( alias ) ) {
      // try using the mapping information first since it is complete
      if ( transMeta.environmentSubstitute( alias ).equals( m_keyName ) ) {
        tableItem.setText( 2, m_keyType.toString() );
        HBaseValueMetaInterface vm = m_mappedColumns.get( transMeta.environmentSubstitute( alias ) );
        if ( vm != null ) {
          vm.setType( HBaseInput.getKettleTypeByKeyType( m_keyType ) );
          String type = ValueMetaBase.getTypeDesc( vm.getType() );
          tableItem.setText( 2, type );
          return vm;
        }
      } else if ( m_mappedColumns != null ) {
        HBaseValueMetaInterface vm = m_mappedColumns.get( transMeta.environmentSubstitute( alias ) );
        if ( vm != null ) {
          String type = ValueMetaBase.getTypeDesc( vm.getType() );
          if ( vm.getType() == ValueMetaInterface.TYPE_INTEGER ) {
            if ( vm.getIsLongOrDouble() ) {
              type = "Long";
            } else {
              type = "Integer";
            }
          }
          if ( vm.getType() == ValueMetaInterface.TYPE_NUMBER ) {
            if ( vm.getIsLongOrDouble() ) {
              type = "Double";
            } else {
              type = "Float";
            }
          }

          tableItem.setText( 2, type );
          return vm;
        }
      } else if ( m_currentMeta.getOutputFields() != null && m_currentMeta.getOutputFields().size() > 0 ) {

        // use the user-selected fields information
        for ( HBaseValueMetaInterface vm : m_currentMeta.getOutputFields() ) {
          String aliasF = vm.getAlias();
          if ( alias.equals( aliasF ) ) {
            String type = ValueMetaBase.getTypeDesc( vm.getType() );
            tableItem.setText( 2, type );
            return vm;
          }
        }
      }
    }

    return null;
  }

  protected void updateMetaConnectionDetails( HBaseInputMeta meta ) {
    NamedCluster nc = namedClusterWidget.getSelectedNamedCluster();
    if ( nc != null ) {
      meta.setNamedCluster( nc );
    }

    meta.setCoreConfigURL( m_coreConfigText.getText() );
    meta.setDefaulConfigURL( m_defaultConfigText.getText() );
    meta.setSourceTableName( m_mappedTableNamesCombo.getText() );
    meta.setSourceMappingName( m_mappingNamesCombo.getText() );
  }

  protected void ok() {
    if ( Utils.isEmpty( m_stepnameText.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( Messages.getString( "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( Messages.getString( "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    NamedCluster selectedNamedCluster = namedClusterWidget.getSelectedNamedCluster();
    if ( selectedNamedCluster == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( Messages.getString( "Dialog.Error" ) );
      mb.setMessage( Messages.getString( "HBaseInputDialog.NamedClusterNotSelected.Msg" ) );
      mb.open();
      return;
    } else {
      if ( StringUtils.isEmpty( selectedNamedCluster.getZooKeeperHost() ) ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setText( Messages.getString( "Dialog.Error" ) );
        mb.setMessage( Messages.getString( "HBaseInputDialog.NamedClusterMissingValues.Msg" ) );
        mb.open();
        return;
      }
    }

    stepname = m_stepnameText.getText();
    updateMetaConnectionDetails( m_currentMeta );
    m_currentMeta.setKeyStartValue( m_keyStartText.getText() );
    m_currentMeta.setKeyStopValue( m_keyStopText.getText() );
    m_currentMeta.setScannerCacheSize( m_scanCacheText.getText() );
    m_currentMeta.setMatchAnyFilter( m_matchAnyBut.getSelection() );

    List<HBaseValueMetaInterface> outputFields =
        getOutputFields( m_fieldsView, m_mappedTableNamesCombo.getText(), m_mappingNamesCombo.getText(), m_indexedLookup, getHBaseLocalService().getHBaseValueMetaInterfaceFactory(),
            getHBaseLocalService().getByteConversionUtil() );
    m_currentMeta.setOutputFields( outputFields );

    List<ColumnFilter> columnsFilters = getColumnsFilters( m_filtersView, getHBaseLocalService().getColumnFilterFactory() );
    m_currentMeta.setColumnFilters( columnsFilters );

    if ( m_storeMappingInStepMetaData.getSelection() ) {
      if ( Utils.isEmpty( m_mappingNamesCombo.getText() ) ) {
        List<String> problems = new ArrayList<String>();

        Mapping toSet = m_mappingEditor.getMapping( false, problems, false );
        if ( problems.size() > 0 ) {
          StringBuffer p = new StringBuffer();
          for ( String s : problems ) {
            p.append( s ).append( "\n" );
          }
          MessageDialog md =
              new MessageDialog( shell, BaseMessages.getString( HBaseInputMeta.PKG,
                  "HBaseInputDialog.Error.IssuesWithMapping.Title" ), null, BaseMessages.getString( HBaseInputMeta.PKG,
                    "HBaseInputDialog.Error.IssuesWithMapping" ) + ":\n\n" + p.toString(), MessageDialog.WARNING,
                  new String[] { BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInputDialog.Error.IssuesWithMapping.ButtonOK" ),
                      BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInputDialog.Error.IssuesWithMapping.ButtonCancel" ) }, 0 );
          MessageDialog.setDefaultImage( GUIResource.getInstance().getImageSpoon() );
          int idx = md.open() & 0xFF;
          if ( idx == 1 || idx == 255 /* 255 = escape pressed */ ) {
            return; // Cancel
          }
        }
        m_currentMeta.setMapping( toSet );
      } else {
        HBaseConnection connection = null;
        try {
          connection = getHBaseConnection();
          MappingAdmin admin = new MappingAdmin( connection );
          Mapping current =
              admin.getMapping( transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() ), transMeta
                  .environmentSubstitute( m_mappingNamesCombo.getText() ) );

          m_currentMeta.setMapping( current );
          m_currentMeta.setSourceMappingName( "" );
        } catch ( Exception e ) {
          logError( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" )
              + " \""
              + transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() + ","
                  + transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) + "\"" ), e );
          new ErrorDialog( shell, Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" ), Messages
              .getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" )
              + " \""
              + transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() + ","
                  + transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) + "\"" ), e );
        } finally {
          if ( connection != null ) {
            try {
              connection.close();
            } catch ( Exception e ) {
              String msg = Messages.getString( "HBaseInputDialog.ErrorMessage.FailedClosingHBaseConnection" );
              logError( msg, e );
              new ErrorDialog( shell, msg, msg, e );
            }
          }
        }
      }
    } else {
      // we're going to use a mapping stored in HBase - null out any stored
      // mapping
      m_currentMeta.setMapping( null );
    }

    if ( !m_originalMeta.equals( m_currentMeta ) ) {
      m_currentMeta.setChanged();
      changed = m_currentMeta.hasChanged();
    }

    dispose();
  }

  @VisibleForTesting
  static List<HBaseValueMetaInterface> getOutputFields( TableView fieldsView, String tableName, String mappingName, Map<String, String> indexedLookup, HBaseValueMetaInterfaceFactory vmFactory,
      ByteConversionUtil convUtills ) {
    List<HBaseValueMetaInterface> outputFields = null;
    int numNonEmpty = fieldsView.nrNonEmpty();
    if ( numNonEmpty > 0 ) {
      outputFields = new ArrayList<>( numNonEmpty );
      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = fieldsView.getNonEmpty( i );
        String alias = item.getText( 1 ).trim();
        String isKey = item.getText( 2 ).trim();
        String family = item.getText( 3 ).trim();
        String column = item.getText( 4 ).trim();
        String type = item.getText( 5 ).trim();
        String format = item.getText( 6 ).trim();

        HBaseValueMetaInterface vm = vmFactory.createHBaseValueMetaInterface( family, column, alias, ValueMeta.getType( type ), -1, -1 );

        vm.setTableName( tableName );
        vm.setMappingName( mappingName );
        vm.setKey( isKey.equalsIgnoreCase( "Y" ) );
        String indexItems = indexedLookup.get( alias );
        if ( indexItems != null ) {
          Object[] values = convUtills.stringIndexListToObjects( indexItems );
          vm.setIndex( values );
          vm.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
        }
        vm.setConversionMask( format );
        outputFields.add( vm );
      }
    }
    return outputFields;
  }

  @VisibleForTesting
  static List<ColumnFilter> getColumnsFilters( TableView filtersView, ColumnFilterFactory colFtFactory ) {
    List<ColumnFilter> filters = null;
    int numNonEmpty = filtersView.nrNonEmpty();
    if ( numNonEmpty > 0 ) {
      filters = new ArrayList<>( numNonEmpty );
      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = filtersView.getNonEmpty( i );
        String alias = item.getText( 1 ).trim();
        String type = item.getText( 2 ).trim();
        String operator = item.getText( 3 ).trim();
        String comparison = item.getText( 4 ).trim();
        String signed = item.getText( 6 ).trim();
        String format = item.getText( 5 ).trim();

        ColumnFilter f = colFtFactory.createFilter( alias );
        f.setFieldType( type );
        f.setComparisonOperator( ColumnFilter.ComparisonType.stringToOpp( operator ) );
        f.setConstant( comparison );
        f.setSignedComparison( signed.equalsIgnoreCase( "Y" ) );
        f.setFormat( format );
        filters.add( f );
      }
    }
    return filters;
  }

  protected void cancel() {
    stepname = null;
    m_currentMeta.setChanged( changed );

    dispose();
  }

  private void getData() {

    namedClusterWidget.setSelectedNamedCluster( m_currentMeta.getNamedCluster().getName() );

    if ( !Utils.isEmpty( m_currentMeta.getCoreConfigURL() ) ) {
      m_coreConfigText.setText( m_currentMeta.getCoreConfigURL() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getDefaultConfigURL() ) ) {
      m_defaultConfigText.setText( m_currentMeta.getDefaultConfigURL() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getSourceTableName() ) ) {
      m_mappedTableNamesCombo.setText( m_currentMeta.getSourceTableName() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getSourceMappingName() ) ) {
      m_mappingNamesCombo.setText( m_currentMeta.getSourceMappingName() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getKeyStartValue() ) ) {
      m_keyStartText.setText( m_currentMeta.getKeyStartValue() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getKeyStopValue() ) ) {
      m_keyStopText.setText( m_currentMeta.getKeyStopValue() );
    }

    if ( !Utils.isEmpty( m_currentMeta.getScannerCacheSize() ) ) {
      m_scanCacheText.setText( m_currentMeta.getScannerCacheSize() );
    }

    m_matchAnyBut.setSelection( m_currentMeta.getMatchAnyFilter() );
    m_matchAllBut.setSelection( !m_currentMeta.getMatchAnyFilter() );

    // filters
    if ( m_currentMeta.getColumnFilters() != null && m_currentMeta.getColumnFilters().size() > 0 ) {
      for ( ColumnFilter f : m_currentMeta.getColumnFilters() ) {
        TableItem item = new TableItem( m_filtersView.table, SWT.NONE );

        if ( !Utils.isEmpty( f.getFieldAlias() ) ) {
          item.setText( 1, f.getFieldAlias() );
        }
        if ( !Utils.isEmpty( f.getFieldType() ) ) {
          item.setText( 2, f.getFieldType() );
        }
        if ( f.getComparisonOperator() != null ) {
          item.setText( 3, f.getComparisonOperator().toString() );
        }
        if ( !Utils.isEmpty( f.getConstant() ) ) {
          item.setText( 4, f.getConstant() );
        }
        item.setText( 6, ( f.getSignedComparison() ) ? "Y" : "N" );
        if ( !Utils.isEmpty( f.getFormat() ) ) {
          item.setText( 5, f.getFormat() );
        }
      }

      m_filtersView.removeEmptyRows();
      m_filtersView.setRowNums();
      m_filtersView.optWidth( true );
    }

    if ( Utils.isEmpty( m_currentMeta.getSourceMappingName() ) && m_currentMeta.getMapping() != null ) {
      m_mappingEditor.setMapping( m_currentMeta.getMapping() );
      m_storeMappingInStepMetaData.setSelection( true );
    }

    // do the key and columns
    checkKeyInformation( true, false );
  }

  @Override public HBaseService getHBaseService() throws ClusterInitializationException {
    NamedCluster nc = namedClusterWidget.getSelectedNamedCluster();
    return namedClusterServiceLocator.getService( nc, HBaseService.class );
  }

  public HBaseConnection getHBaseConnection() throws IOException, ClusterInitializationException {
    String coreConf = "";
    String defaultConf = "";
    String zookeeperHosts = "";

    NamedCluster nc = namedClusterWidget.getSelectedNamedCluster();
    HBaseService hBaseService = getHBaseService();
    if ( nc != null ) {
      zookeeperHosts = transMeta.environmentSubstitute( nc.getZooKeeperHost() );
    }

    if ( !Utils.isEmpty( m_coreConfigText.getText() ) ) {
      coreConf = transMeta.environmentSubstitute( m_coreConfigText.getText() );
    }

    if ( !Utils.isEmpty( m_defaultConfigText.getText() ) ) {
      defaultConf = transMeta.environmentSubstitute( m_defaultConfigText.getText() );
    }

    if ( Utils.isEmpty( zookeeperHosts ) && Utils.isEmpty( coreConf ) && Utils.isEmpty( defaultConf ) ) {
      throw new IOException( BaseMessages.getString( HBaseInputMeta.PKG,
          "MappingDialog.Error.Message.CantConnectNoConnectionDetailsProvided" ) );
    }
    return hBaseService.getHBaseConnection( transMeta, coreConf, defaultConf, null );
  }

  private void checkKeyInformation( boolean quiet, boolean readFieldsFromMapping ) {
    String zookeeperQuorumText = null;

    NamedCluster nc = namedClusterWidget.getSelectedNamedCluster();
    if ( nc != null ) {
      zookeeperQuorumText = nc.getZooKeeperHost();
    }

    boolean displayFieldsEmbeddedMapping =
      ( ( m_mappingEditor.getMapping( false, null, false ) != null && Utils.isEmpty( m_mappingNamesCombo.getText() ) ) );
    boolean displayFieldsMappingFromHBase =
      ( !Utils.isEmpty( m_coreConfigText.getText() ) || !Utils.isEmpty( zookeeperQuorumText ) )
        && !Utils.isEmpty( m_mappedTableNamesCombo.getText() ) && !Utils.isEmpty( m_mappingNamesCombo.getText() );

    if ( displayFieldsEmbeddedMapping || displayFieldsMappingFromHBase ) {
      try {
        m_indexedLookup = new HashMap<String, String>();

        MappingAdmin admin = null;

        Mapping current = null;
        Map<String, HBaseValueMetaInterface> mappedColumns = null;

        boolean filterAliasesDone = false;
        HBaseConnection connection = null;

        try {
          if ( displayFieldsMappingFromHBase && readFieldsFromMapping ) {
            connection = getHBaseConnection();
            if ( displayFieldsMappingFromHBase ) {
              admin = new MappingAdmin( connection );
            }
            current =
              admin.getMapping( transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() ), transMeta
                .environmentSubstitute( m_mappingNamesCombo.getText() ) );
          } else {
            current = m_mappingEditor.getMapping( false, null, true );
          }

          if ( current != null ) {
            // Key information
            m_keyName = current.getKeyName();
            m_keyType = current.getKeyType();
            m_keyInfo.setText( "HBase Key: " + m_keyName + " (" + m_keyType.toString() + ")" );

            mappedColumns = current.getMappedColumns();
            m_mappedColumns = mappedColumns; // cached copy

            // Set up the alias combo box in the filters tab
            List<String> filterAliasNames = new ArrayList<String>();
            filterAliasNames.add( m_keyName );
            for ( String alias : mappedColumns.keySet() ) {
              HBaseValueMetaInterface column = mappedColumns.get( alias );
              String aliasS = column.getAlias();
              if ( column.isNumeric() || column.isDate() || column.isString() || column.isBoolean() ) {
                filterAliasNames.add( aliasS );
              }
            }
            String[] filterAliasNamesA = filterAliasNames.toArray( new String[1] );
            m_filterAliasCI.setComboValues( filterAliasNamesA );
            filterAliasesDone = true;
          } else {
            m_keyInfo.setText( "" );
          }
        } catch ( Exception ex ) {
          if ( !quiet ) {
            logError( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" )
              + " \""
              + transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() + ","
              + transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) + "\"" ), ex );
            new ErrorDialog( shell, Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" ), Messages
              .getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" )
              + " \""
              + transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() + ","
              + transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) + "\"" ), ex );
          }
          m_keyInfo.setText( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" ) );
        } finally {
          if ( connection != null ) {
            connection.close();
          }
        }

        // Fields information
        m_fieldsView.clearAll( false );

        if ( current != null && readFieldsFromMapping ) {
          TableItem item = new TableItem( m_fieldsView.table, SWT.NONE );
          item.setText( 1, m_keyName );
          item.setText( 2, "Y" );
          item.setText( 7, "N" );
          if ( current.getKeyType() == Mapping.KeyType.DATE || current.getKeyType() == Mapping.KeyType.UNSIGNED_DATE ) {
            item.setText( 5, ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_DATE ) );
          } else if ( current.getKeyType() == Mapping.KeyType.STRING ) {
            item.setText( 5, ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_STRING ) );
          } else if ( current.getKeyType() == Mapping.KeyType.INTEGER
            || current.getKeyType() == Mapping.KeyType.UNSIGNED_INTEGER
            || current.getKeyType() == Mapping.KeyType.UNSIGNED_LONG
            || current.getKeyType() == Mapping.KeyType.LONG ) {
            item.setText( 5, ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_INTEGER ) );
          } else {
            item.setText( 5, ValueMeta.getTypeDesc( ValueMetaInterface.TYPE_BINARY ) );
          }

          // get all the fields from the mapping
          for ( String alias : mappedColumns.keySet() ) {
            HBaseValueMetaInterface column = mappedColumns.get( alias );
            String aliasS = column.getAlias();
            String family = column.getColumnFamily();
            String name = column.getColumnName();
            String type = column.getTypeDesc();
            String format = column.getConversionMask();

            item = new TableItem( m_fieldsView.table, SWT.NONE );
            if ( column.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
              String valuesString = getHBaseLocalService().getByteConversionUtil().objectIndexValuesToString( column.getIndex() );

              m_indexedLookup.put( aliasS, valuesString );
              item.setText( 7, "Y" );
            } else {
              item.setText( 7, "N" );
            }

            item.setText( 1, aliasS );
            item.setText( 2, "N" );
            item.setText( 3, family );
            item.setText( 4, name );
            item.setText( 5, type );
            if ( !Utils.isEmpty( format ) ) {
              item.setText( 6, format );
            }
          }
        }

        if ( !readFieldsFromMapping && m_currentMeta.getOutputFields() != null
          && m_currentMeta.getOutputFields().size() > 0 ) {

          // user has selected some fields from the mapping to output
          List<String> filterAliasNames = new ArrayList<String>();
          for ( HBaseValueMetaInterface column : m_currentMeta.getOutputFields() ) {
            TableItem item = new TableItem( m_fieldsView.table, SWT.NONE );

            String aliasS = column.getAlias();
            String type = column.getTypeDesc();
            item.setText( 1, aliasS );
            item.setText( 5, type );

            if ( column.isKey() ) {
              item.setText( 2, "Y" );
              item.setText( 7, "N" );

              if ( !Utils.isEmpty( column.getConversionMask() ) ) {
                item.setText( 6, column.getConversionMask() );
              }
              if ( !filterAliasesDone ) {                //todo check for key type may be do not work in some cases
                filterAliasNames.add( aliasS );
              }
              continue; // skip the rest
            }

            item.setText( 2, "N" );

            if ( column.isNumeric() || column.isDate() || column.isString() ) {
              if ( !filterAliasesDone ) {
                filterAliasNames.add( aliasS );
              }
            }
            String family = column.getColumnFamily();
            String name = column.getColumnName();
            String format = column.getConversionMask();

            if ( column.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
              String valuesString = getHBaseLocalService().getByteConversionUtil().objectIndexValuesToString( column.getIndex() );

              m_indexedLookup.put( aliasS, valuesString );
              item.setText( 7, "Y" );
            } else {
              item.setText( 7, "N" );
            }

            item.setText( 3, family );
            item.setText( 4, name );

            if ( !Utils.isEmpty( format ) ) {
              item.setText( 6, format );
            }
          }

          // set the allowable combo values for the selectable columns in the
          // filter tab
          if ( !filterAliasesDone ) {
            String[] filterAliasNamesA = filterAliasNames.toArray( new String[1] );
            m_filterAliasCI.setComboValues( filterAliasNamesA );
            filterAliasesDone = true;
          }
        }

        m_fieldsView.removeEmptyRows();
        m_fieldsView.setRowNums();
        m_fieldsView.optWidth( true );

      } catch ( Exception ex ) {
        if ( !quiet ) {
          logError( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" )
            + " \""
            + transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() + ","
            + transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) + "\"" ), ex );
          new ErrorDialog( shell, Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" ), Messages
            .getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" )
            + " \""
            + transMeta.environmentSubstitute( m_mappedTableNamesCombo.getText() + ","
            + transMeta.environmentSubstitute( m_mappingNamesCombo.getText() ) + "\"" ), ex );
        }
        m_keyInfo.setText( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToGetMapping" ) );
      }
    } else {
      m_keyInfo.setText( "" );
    }
  }

  private void setupMappedTableNames() {
    m_mappedTableNamesCombo.removeAll();

    HBaseConnection connection = null;
    try {
      connection = getHBaseConnection();
      MappingAdmin admin = new MappingAdmin( connection );
      Set<String> tableNames = admin.getMappedTables();

      for ( String s : tableNames ) {
        m_mappedTableNamesCombo.add( s );
      }
    } catch ( Exception e ) {
      logError( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToConnect" ), e );
      new ErrorDialog( shell, Messages.getString( "HBaseInputDialog.ErrorMessage." + "UnableToConnect" ), Messages
          .getString( "HBaseInputDialog.ErrorMessage.UnableToConnect" ), e );
    } finally {
      if ( connection != null ) {
        try {
          connection.close();
        } catch ( Exception e ) {
          String msg = Messages.getString( "HBaseInputDialog.ErrorMessage.FailedClosingHBaseConnection" );
          logError( msg, e );
          new ErrorDialog( shell, msg, msg,  e );
        }
      }
    }
  }

  private void setupMappingNamesForTable( boolean quiet ) {
    m_mappingNamesCombo.removeAll();

    if ( !Utils.isEmpty( m_mappedTableNamesCombo.getText() ) ) {
      HBaseConnection connection = null;
      try {
        connection = getHBaseConnection();
        MappingAdmin admin = new MappingAdmin( connection );

        List<String> mappingNames = admin.getMappingNames( m_mappedTableNamesCombo.getText().trim() );

        for ( String n : mappingNames ) {
          m_mappingNamesCombo.add( n );
        }
      } catch ( Exception ex ) {
        if ( !quiet ) {
          logError( Messages.getString( "HBaseInputDialog.ErrorMessage.UnableToConnect" ), ex );
          new ErrorDialog( shell, Messages.getString( "HBaseInputDialog.ErrorMessage." + "UnableToConnect" ), Messages
              .getString( "HBaseInputDialog.ErrorMessage.UnableToConnect" ), ex );
        }
      } finally {
        if ( connection != null ) {
          try {
            connection.close();
          } catch ( Exception e ) {
            if ( !quiet ) {
              String msg = Messages.getString( "HBaseInputDialog.ErrorMessage.FailedClosingHBaseConnection" );
              logError( msg, e );
              new ErrorDialog( shell, msg, msg,  e );
            }
          }
        }
      }
    }
  }

  public String getCurrentConfiguration() {
    updateMetaConnectionDetails( m_configurationMeta );
    return m_configurationMeta.getXML();
  }
}
