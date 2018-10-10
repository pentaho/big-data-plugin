/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.impl.avro.input;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.big.data.kettle.plugins.formats.avro.input.AvroInputField;
import org.pentaho.big.data.kettle.plugins.formats.avro.input.AvroInputMetaBase;
import org.pentaho.big.data.kettle.plugins.formats.impl.avro.BaseAvroStepDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ColumnsResizer;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.util.List;

public class AvroInputDialog extends BaseAvroStepDialog<AvroInputMeta> {

  private static final int SHELL_WIDTH = 698;
  private static final int SHELL_HEIGHT = 554;

  private static final int AVRO_ORIGINAL_PATH_COLUMN_INDEX = 1;
  private static final int AVRO_DISPLAY_PATH_COLUMN_INDEX = 2;
  private static final int AVRO_TYPE_COLUMN_INDEX = 3;
  private static final int AVRO_INDEXED_VALUES_COLUMN_INDEX = 4;
  private static final int FIELD_NAME_COLUMN_INDEX = 5;
  private static final int FIELD_TYPE_COLUMN_INDEX = 6;
  private static final int FORMAT_COLUMN_INDEX = 7;

  private static final String SCHEMA_SCHEME_DEFAULT = "hdfs";

  private TableView wInputFields;
  protected TextVar wSchemaPath;
  protected Button wbSchemaBrowse;
  protected Composite wSchemaFileComposite;
  protected Composite wSchemaFieldComposite;
  Button wbGetSchemaFromFile;
  Button wbGetSchemaFromField;
  Button wFieldContainsSchemaPath;
  Button wCacheSchemas;
  private Button m_getLookupFieldsBut;
  ComboVar wSchemaFieldNameCombo;
  private TableView m_lookupView;


  private Button wPassThruFields;
  private Button wAllowNullValues;

  public AvroInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (AvroInputMeta) in, transMeta, sname );
  }

  protected Control createAfterFile( Composite afterFile ) {
    CTabFolder wTabFolder = new CTabFolder( afterFile, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addFileTab( wTabFolder );
    addSchemaTab( wTabFolder );
    addFieldsTab( wTabFolder );
    addLookupFieldsTab( wTabFolder );

    new FD( wTabFolder ).left( 0, 0 ).top( 0, MARGIN ).right( 100, 0 ).bottom( 100, 0 ).apply();
    wTabFolder.setSelection( 0 );

    return wTabFolder;
  }


  protected void populateNestedFieldsTable() {
    // this schema overrides any that might be in a container file
    String schemaFileName = wSchemaPath.getText();
    schemaFileName = transMeta.environmentSubstitute( schemaFileName );

    String avroFileName = wPath.getText();
    avroFileName = transMeta.environmentSubstitute( avroFileName );

    List<? extends IAvroInputField> defaultFields = null;
    try {
      defaultFields = AvroInput
        .getLeafFields( meta.getNamedClusterServiceLocator(), meta.getNamedCluster(), schemaFileName, avroFileName );
      if ( defaultFields != null ) {
        wInputFields.clearAll();
        for ( IAvroInputField field : defaultFields ) {
          TableItem item = new TableItem( wInputFields.table, SWT.NONE );
          if ( field != null ) {
            setField( item, field.getDisplayableAvroFieldName(), AVRO_ORIGINAL_PATH_COLUMN_INDEX );
            setField( item, clearIndexFromFieldName( field.getDisplayableAvroFieldName() ), AVRO_DISPLAY_PATH_COLUMN_INDEX );
            setField( item, field.getAvroType().getName(), AVRO_TYPE_COLUMN_INDEX );
            setField( item, field.getIndexedValues(), AVRO_INDEXED_VALUES_COLUMN_INDEX );
            setField( item, field.getPentahoFieldName(), FIELD_NAME_COLUMN_INDEX );
            setField( item, ValueMetaFactory.getValueMetaName( field.getPentahoType() ), FIELD_TYPE_COLUMN_INDEX );
            setField( item, field.getStringFormat(), FORMAT_COLUMN_INDEX );
          }
        }

        wInputFields.removeEmptyRows();
        wInputFields.setRowNums();
        wInputFields.optWidth( true );
      }
    } catch ( Exception ex ) {
      logError( BaseMessages.getString( PKG, "AvroInput.Error.UnableToLoadSchemaFromContainerFile" ), ex );
      new ErrorDialog( shell, stepname, BaseMessages.getString( PKG,
        "AvroInput.Error.UnableToLoadSchemaFromContainerFile", avroFileName ), ex );
    }
  }

  private void setField( TableItem item, String fieldValue, int fieldIndex ) {
    if ( !Utils.isEmpty( fieldValue ) ) {
      item.setText( fieldIndex, fieldValue );
    }
  }

  private void addLookupFieldsTab( CTabFolder wTabFolder ) {
    CTabItem m_wVarsTab = new CTabItem( wTabFolder, SWT.NONE );
    m_wVarsTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.VarsTab.Title" ) );
    Composite wVarsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wVarsComp );

    FormLayout varsLayout = new FormLayout();
    varsLayout.marginWidth = MARGIN;
    varsLayout.marginHeight = MARGIN;
    wVarsComp.setLayout( varsLayout );

    //    new FD( wVarsComp ).top(0, 0).bottom( 100, 0 ).left( 0, 0 ).right( 100, 0 ).apply();

    // lookup fields (variables) tab

    // get lookup fields but
    m_getLookupFieldsBut = new Button( wVarsComp, SWT.PUSH | SWT.CENTER );
    m_getLookupFieldsBut.setText( BaseMessages.getString( PKG, "AvroInputDialog.Button.GetLookupFields" ) );
    new FD( m_getLookupFieldsBut ).bottom( 100, -Const.MARGIN * 2 ).right( 100, 0 ).apply();
    m_getLookupFieldsBut.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        // get incoming field names
        getIncomingFields();
      }
    } );

    final ColumnInfo[] colinf2 =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.LOOKUP_NAME" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.LOOKUP_VARIABLE" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.LOOKUP_DEFAULT_VALUE" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colinf2[ 0 ].setAutoResize( false );
    colinf2[ 1 ].setAutoResize( false );
    colinf2[ 2 ].setAutoResize( false );
    m_lookupView =
      new TableView( transMeta, wVarsComp, SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER, colinf2, 1, lsMod, props );
    new FD( m_lookupView ).top( 0, Const.MARGIN * 2 ).bottom( m_getLookupFieldsBut, -Const.MARGIN * 2 ).left( 0, 0 )
      .right( 100, 0 ).apply();

    ColumnsResizer resizer = new ColumnsResizer( 0, 33, 33, 34 );
    resizer.addColumnResizeListeners( m_lookupView.getTable() );
    m_lookupView.getTable().addListener( SWT.Resize, resizer );
    m_lookupView.optWidth( true );

    m_wVarsTab.setControl( wVarsComp );

  }

  private void getIncomingFields() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        BaseStepDialog.getFieldsFromPrevious( r, m_lookupView, 1, new int[] { 1 }, null, -1, -1, null );
      }
    } catch ( KettleException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), e );
    }
  }

  private void addFieldsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.FieldsTab.TabTitle" ) );

    Composite wComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wComp );

    FormLayout layout = new FormLayout();
    layout.marginWidth = MARGIN;
    layout.marginHeight = MARGIN;
    layout.marginBottom = MARGIN;
    wComp.setLayout( layout );

    //get fields button
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        populateNestedFieldsTable();
      }
    };
    Button wGetFields = new Button( wComp, SWT.PUSH );
    wGetFields.setText( BaseMessages.getString( PKG, "AvroInputDialog.Fields.Get" ) );
    props.setLook( wGetFields );
    new FD( wGetFields ).bottom( 100, 0 ).right( 100, 0 ).apply();
    wGetFields.addListener( SWT.Selection, lsGet );

    // fields table
    ColumnInfo avroOriginalPathColumnInfo =
      new ColumnInfo( "Original Avro Path", ColumnInfo.COLUMN_TYPE_TEXT,
        false, true );
    ColumnInfo avroDisplayPathColumnInfo =
      new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.column.Path" ), ColumnInfo.COLUMN_TYPE_NONE,
        false, true );
    ColumnInfo avroTypeColumnInfo =
      new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.column.avro.type" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, true );
    ColumnInfo avroIndexColumnInfo =
      new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.column.avro.indexedValues" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    ColumnInfo nameColumnInfo =
      new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false, false );
    ColumnInfo typeColumnInfo = new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.column.Type" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() );
    ColumnInfo formatColumnInfo = new ColumnInfo( BaseMessages.getString( PKG, "AvroInputDialog.Fields.column.Format" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, Const.getDateFormats() );

    ColumnInfo[] parameterColumns =
      new ColumnInfo[] { avroOriginalPathColumnInfo, avroDisplayPathColumnInfo, avroTypeColumnInfo, avroIndexColumnInfo,
        nameColumnInfo, typeColumnInfo, formatColumnInfo };
    parameterColumns[ 1 ].setAutoResize( false );
    parameterColumns[ 3 ].setAutoResize( false );
    parameterColumns[ 4 ].setUsingVariables( true );
    parameterColumns[ 6 ].setAutoResize( false );

    wInputFields =
      new TableView( transMeta, wComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER | SWT.NO_SCROLL | SWT.V_SCROLL,
        parameterColumns, 8, null, props );
    ColumnsResizer resizer = new ColumnsResizer( 0, 0, 20, 15, 15, 20, 15, 15 );
    wInputFields.getTable().addListener( SWT.Resize, resizer );
    new FD( wInputFields ).left( 0, 0 ).right( 100, 0 ).top( 0, Const.MARGIN * 2 ).bottom( wGetFields, -FIELDS_SEP )
      .apply();

    wInputFields.setRowNums();
    wInputFields.optWidth( true );

    // Accept fields from previous steps?
    wPassThruFields = new Button( wComp, SWT.CHECK );
    wPassThruFields.setText( BaseMessages.getString( PKG, "AvroInputDialog.PassThruFields.Label" ) );
    wPassThruFields.setToolTipText( BaseMessages.getString( PKG, "AvroInputDialog.PassThruFields.Tooltip" ) );
    wPassThruFields.setOrientation( SWT.LEFT_TO_RIGHT );
    props.setLook( wPassThruFields );
    new FD( wPassThruFields ).left( 0, 0 ).top( wInputFields, 10 ).apply();

    // Accept fields from previous steps?
    wAllowNullValues = new Button( wComp, SWT.CHECK );
    wAllowNullValues.setText( BaseMessages.getString( PKG, "AvroInputDialog.AllowNullValues.Label" ) );
    wAllowNullValues.setOrientation( SWT.LEFT_TO_RIGHT );
    props.setLook( wAllowNullValues );
    new FD( wAllowNullValues ).left( 0, 0 ).top( wPassThruFields, 5 ).apply();


    new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply();

    wTab.setControl( wComp );
    for ( ColumnInfo col : parameterColumns ) {
      col.setAutoResize( false );
    }
    resizer.addColumnResizeListeners( wInputFields.getTable() );
    setTruncatedColumn( wInputFields.getTable(), 1 );
    if ( !Const.isWindows() ) {
      addColumnTooltip( wInputFields.getTable(), 1 );
    }

    wInputFields.getColumns()[ AVRO_INDEXED_VALUES_COLUMN_INDEX ].setAutoResize( true );
  }

  private void addSchemaTab( CTabFolder wTabFolder ) {
    // Create & Set up a new Tab Item
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "AvroInputDialog.Schema.TabTitle" ) );
    Composite wTabComposite = new Composite( wTabFolder, SWT.NONE );
    wTab.setControl( wTabComposite );
    props.setLook( wTabComposite );
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = MARGIN;
    wTabComposite.setLayout( formLayout );

    // Set up the Source Group
    Group wSourceGroup = new Group( wTabComposite, SWT.SHADOW_NONE );
    props.setLook( wSourceGroup );
    wSourceGroup.setText( BaseMessages.getString( PKG, "AvroInputDialog.Schema.SourceTitle" ) );

    FormLayout layout = new FormLayout();
    layout.marginWidth = MARGIN;
    layout.marginHeight = MARGIN;
    wSourceGroup.setLayout( layout );
    new FD( wSourceGroup ).top( 0, 0 ).right( 100, -MARGIN ).left( 0, MARGIN ).apply();

    int RADIO_BUTTON_WIDTH = 150;
    Label separator = new Label( wSourceGroup, SWT.SEPARATOR | SWT.VERTICAL );
    props.setLook( separator );
    new FD( separator ).left( 0, RADIO_BUTTON_WIDTH ).top( 0, 0 ).bottom( 100, 0 ).apply();

    wbGetSchemaFromFile = new Button( wSourceGroup, SWT.RADIO );
    wbGetSchemaFromFile.setText( getBaseMsg( "AvroDialog.File.SpecifyFileName" ) );
    props.setLook( wbGetSchemaFromFile );
    new FD( wbGetSchemaFromFile ).left( 0, 0 ).top( 0, 0 ).width( RADIO_BUTTON_WIDTH ).apply();

    wbGetSchemaFromField = new Button( wSourceGroup, SWT.RADIO );
    wbGetSchemaFromField.setText( getBaseMsg( "AvroDialog.File.GetDataFromField" ) );
    props.setLook( wbGetSchemaFromField );
    new FD( wbGetSchemaFromField ).left( 0, 0 ).top( wbGetSchemaFromFile, FIELDS_SEP ).width( RADIO_BUTTON_WIDTH )
      .apply();

    //Make a composite to hold the dynamic right side of the group
    Composite wSchemaSettingsDynamicArea = new Composite( wSourceGroup, SWT.NONE );
    props.setLook( wSchemaSettingsDynamicArea );
    FormLayout fileSettingsDynamicAreaLayout = new FormLayout();
    wSchemaSettingsDynamicArea.setLayout( fileSettingsDynamicAreaLayout );
    new FD( wSchemaSettingsDynamicArea ).right( 100, 0 ).left( wbGetSchemaFromFile, MARGIN ).top( 0, -MARGIN ).apply();

    //Put the File selection stuff in it
    wSchemaFileComposite = new Composite( wSchemaSettingsDynamicArea, SWT.NONE );
    FormLayout schemaFileLayout = new FormLayout();
    wSchemaFileComposite.setLayout( schemaFileLayout );
    new FD( wSchemaFileComposite ).left( 0, 0 ).right( 100, RADIO_BUTTON_WIDTH + MARGIN - 15 ).top( 0, 0 ).apply();

    Label wlSchemaPath = new Label( wSchemaFileComposite, SWT.RIGHT );
    wlSchemaPath.setText( BaseMessages.getString( PKG, "AvroInputDialog.Schema.FileName" ) );
    props.setLook( wlSchemaPath );
    new FD( wlSchemaPath ).left( 0, 0 ).top( 0, MARGIN ).apply();

    wSchemaPath = new TextVar( transMeta, wSchemaFileComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaPath );
    new FD( wSchemaPath ).left( 0, 0 ).top( wlSchemaPath, FIELD_LABEL_SEP ).width( FIELD_LARGE + VAR_EXTRA_WIDTH )
      .rright().apply();

    wbSchemaBrowse = new Button( wSchemaFileComposite, SWT.PUSH );
    props.setLook( wbSchemaBrowse );
    wbSchemaBrowse.setText( getMsg( "System.Button.Browse" ) );
    wbSchemaBrowse.addListener( SWT.Selection, event -> browseForFileInputPathForSchema() );
    int bOffset =
      ( wbSchemaBrowse.computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y - wSchemaPath.computeSize( SWT.DEFAULT,
        SWT.DEFAULT, false ).y ) / 2;
    new FD( wbSchemaBrowse ).left( wSchemaPath, FIELD_LABEL_SEP ).top( wlSchemaPath, FIELD_LABEL_SEP - bOffset )
      .apply();

    wSchemaFieldComposite = new Composite( wSchemaSettingsDynamicArea, SWT.NONE );
    FormLayout schemaFieldLayout = new FormLayout();
    wSchemaFieldComposite.setLayout( schemaFieldLayout );
    new FD( wSchemaFieldComposite ).left( 0, 0 ).right( 100, RADIO_BUTTON_WIDTH + MARGIN - 15 ).top( 0, 0 ).apply();

    Label fieldNameLabel = new Label( wSchemaFieldComposite, SWT.NONE );
    fieldNameLabel.setText( getBaseMsg( "AvroDialog.FieldName.Label" ) );
    props.setLook( fieldNameLabel );
    new FD( fieldNameLabel ).left( 0, 0 ).top( wSchemaFieldComposite, 0 ).apply();
    wSchemaFieldNameCombo = new ComboVar( transMeta, wSchemaFieldComposite, SWT.LEFT | SWT.BORDER );
    updateIncomingFieldList( wSchemaFieldNameCombo );
    new FD( wSchemaFieldNameCombo ).left( 0, 0 ).top( fieldNameLabel, FIELD_LABEL_SEP ).width( FIELD_MEDIUM )
      .apply();

    // Accept fields from previous steps?
    wFieldContainsSchemaPath = new Button( wSchemaFieldComposite, SWT.CHECK );
    wFieldContainsSchemaPath.setText( BaseMessages.getString( PKG, "AvroInputDialog.FieldContainsSchemaPath.Label" ) );
    wFieldContainsSchemaPath.setOrientation( SWT.LEFT_TO_RIGHT );
    props.setLook( wFieldContainsSchemaPath );
    new FD( wFieldContainsSchemaPath ).left( 0, 0 ).top( wSchemaFieldNameCombo, 10 ).apply();

    // Accept fields from previous steps?
    wCacheSchemas = new Button( wSchemaFieldComposite, SWT.CHECK );
    wCacheSchemas.setText( BaseMessages.getString( PKG, "AvroInputDialog.CacheSchemas.Label" ) );
    wCacheSchemas.setOrientation( SWT.LEFT_TO_RIGHT );
    props.setLook( wCacheSchemas );
    new FD( wCacheSchemas ).left( 0, 0 ).top( wFieldContainsSchemaPath, 5 ).apply();

    //Setup the radio button event handler
    SelectionAdapter fileSettingRadioSelectionAdapter = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        wSchemaFileComposite.setVisible( !wbGetSchemaFromField.getSelection() );
        wSchemaFieldComposite.setVisible( wbGetSchemaFromField.getSelection() );
      }
    };
    wbGetSchemaFromFile.addSelectionListener( fileSettingRadioSelectionAdapter );
    wbGetSchemaFromField.addSelectionListener( fileSettingRadioSelectionAdapter );

    wbGetSchemaFromFile.setSelection( true );
    wbGetSchemaFromField.setSelection( false );
    wSchemaFileComposite.setVisible( true );
    wSchemaFieldComposite.setVisible( false );
  }

  private void browseForFileInputPathForSchema() {
    try {
      String path = transMeta.environmentSubstitute( wSchemaPath.getText() );
      VfsFileChooserDialog fileChooserDialog;
      String fileName;
      if ( Utils.isEmpty( path ) ) {
        fileChooserDialog = getVfsFileChooserDialog( null, null );
        fileName = SCHEMA_SCHEME_DEFAULT + "://";
      } else {
        FileObject initialFile = getInitialFile( wSchemaPath.getText() );
        FileObject rootFile = initialFile.getFileSystem().getRoot();
        fileChooserDialog = getVfsFileChooserDialog( rootFile, initialFile );
        fileName = null;
      }

      FileObject selectedFile =
        fileChooserDialog.open( shell, null, getSchemeFromPath( path ), true, fileName, FILES_FILTERS,
          fileFilterNames, true, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, true, true );
      if ( selectedFile != null ) {
        wSchemaPath.setText( selectedFile.getURL().toString() );
      }
    } catch ( KettleFileException ex ) {
      log.logError( getBaseMsg( "AvroInputDialog.SchemaFileBrowser.KettleFileException" ) );
    } catch ( FileSystemException ex ) {
      log.logError( getBaseMsg( "AvroInputDialog.SchemaFileBrowser.FileSystemException" ) );
    }
  }

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  @Override
  protected void getData( AvroInputMeta meta ) {

    wPath.setText( "" );
    wFieldNameCombo.setText( "" );
    wSchemaPath.setText( "" );
    wbGetDataFromFile.setSelection( true );
    wbGetDataFromField.setSelection( false );
    wDataFileComposite.setVisible( true );
    wDataFieldComposite.setVisible( false );
    encodingCombo.select( meta.isDataBinaryEncoded() ? 0 : 1 );

    wbGetSchemaFromFile.setSelection( true );
    wbGetSchemaFromField.setSelection( false );
    wSchemaFileComposite.setVisible( true );
    wSchemaFieldComposite.setVisible( false );

    wFieldContainsSchemaPath.setSelection( false );
    wCacheSchemas.setSelection( false );

    if ( meta.getDataLocation() != null ) {
      if ( ( meta.getDataLocationType() == AvroInputMetaBase.LocationDescriptor.FILE_NAME ) ) {
        wPath.setText( meta.getDataLocation() );
      } else if ( ( meta.getDataLocationType() == AvroInputMetaBase.LocationDescriptor.FIELD_NAME ) ) {
        wFieldNameCombo.setText( meta.getDataLocation() );
        wbGetDataFromFile.setSelection( false );
        wbGetDataFromField.setSelection( true );
        wDataFileComposite.setVisible( false );
        wDataFieldComposite.setVisible( true );
      }
    }

    if ( meta.getSchemaLocation() != null ) {
      if ( ( meta.getSchemaLocationType() == AvroInputMetaBase.LocationDescriptor.FILE_NAME ) ) {
        wSchemaPath.setText( meta.getSchemaLocation() );
      } else if ( ( meta.getSchemaLocationType() == AvroInputMetaBase.LocationDescriptor.FIELD_NAME ) ) {
        wSchemaFieldNameCombo.setText( meta.getSchemaLocation() );
        wbGetSchemaFromFile.setSelection( false );
        wbGetSchemaFromField.setSelection( true );
        wSchemaFileComposite.setVisible( false );
        wSchemaFieldComposite.setVisible( true );
        wFieldContainsSchemaPath.setSelection( false );
        wCacheSchemas.setSelection( meta.isCacheSchemas() );
      } else if ( ( meta.getSchemaLocationType()
        == AvroInputMetaBase.LocationDescriptor.FIELD_CONTAINING_FILE_NAME ) ) {
        wSchemaFieldNameCombo.setText( meta.getSchemaLocation() );
        wbGetSchemaFromFile.setSelection( false );
        wbGetSchemaFromField.setSelection( true );
        wSchemaFileComposite.setVisible( false );
        wSchemaFieldComposite.setVisible( true );
        wFieldContainsSchemaPath.setSelection( true );
        wCacheSchemas.setSelection( meta.isCacheSchemas() );
      }
    }

    wPassThruFields.setSelection( meta.inputFiles.passingThruFields );
    wAllowNullValues.setSelection( meta.isAllowNullForMissingFields() );

    int itemIndex = 0;
    for ( AvroInputField inputField : meta.getInputFields() ) {
      TableItem item = null;
      if ( itemIndex < wInputFields.table.getItemCount() ) {
        item = wInputFields.table.getItem( itemIndex );
      } else {
        item = new TableItem( wInputFields.table, SWT.NONE );
      }

      if ( inputField.getAvroFieldName() != null ) {
        item.setText( AVRO_ORIGINAL_PATH_COLUMN_INDEX, inputField.getDisplayableAvroFieldName() );
        item.setText( AVRO_DISPLAY_PATH_COLUMN_INDEX, clearIndexFromFieldName( inputField.getDisplayableAvroFieldName() ) );
      }
      if ( inputField.getAvroType() != null ) {
        item.setText( AVRO_TYPE_COLUMN_INDEX, inputField.getAvroType().getName() );
      }
      if ( inputField.getIndexedValues() != null ) {
        item.setText( AVRO_INDEXED_VALUES_COLUMN_INDEX, inputField.getIndexedValues() );
      }
      if ( inputField.getPentahoFieldName() != null ) {
        item.setText( FIELD_NAME_COLUMN_INDEX, inputField.getPentahoFieldName() );
      }
      if ( inputField.getTypeDesc() != null ) {
        item.setText( FIELD_TYPE_COLUMN_INDEX, inputField.getTypeDesc() );
      }
      if ( inputField.getStringFormat() != null ) {
        item.setText( FORMAT_COLUMN_INDEX, inputField.getStringFormat() );
      } else {
        item.setText( FORMAT_COLUMN_INDEX, "" );
      }
      itemIndex++;
    }
  }

  /**
   * Fill meta object from UI options.
   */
  @Override
  protected void getInfo( AvroInputMeta meta, boolean preview ) {
    if ( wbGetDataFromField.getSelection() ) {
      meta.setDataLocation( wFieldNameCombo.getText(), AvroInputMetaBase.LocationDescriptor.FIELD_NAME );
    } else {
      meta.setDataLocation( wPath.getText(), AvroInputMetaBase.LocationDescriptor.FILE_NAME );
    }

    if ( wbGetSchemaFromField.getSelection() ) {
      if ( wFieldContainsSchemaPath.getSelection() ) {
        meta.setSchemaLocation( wSchemaFieldNameCombo.getText(),
          AvroInputMetaBase.LocationDescriptor.FIELD_CONTAINING_FILE_NAME );
      } else {
        meta.setSchemaLocation( wSchemaFieldNameCombo.getText(), AvroInputMetaBase.LocationDescriptor.FIELD_NAME );
      }
      meta.setCacheSchemas( wCacheSchemas.getSelection() );
    } else {
      meta.setSchemaLocation( wSchemaPath.getText(), AvroInputMetaBase.LocationDescriptor.FILE_NAME );
      meta.setCacheSchemas( false );
    }

    meta.inputFiles.passingThruFields = wPassThruFields.getSelection();
    meta.setAllowNullForMissingFields( wAllowNullValues.getSelection() );
    meta.setDataBinaryEncoded( encodingCombo.getSelectionIndex() == 0 ? true : false );

    int nrFields = wInputFields.nrNonEmpty();
    meta.inputFields = new AvroInputField[ nrFields ];
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wInputFields.getNonEmpty( i );
      AvroInputField field = new AvroInputField();
      field.setFormatFieldName( extractFieldName( item.getText( AVRO_ORIGINAL_PATH_COLUMN_INDEX ) ) );
      field.setAvroType( item.getText( AVRO_TYPE_COLUMN_INDEX ) );
      field.setIndexedValues( item.getText( AVRO_INDEXED_VALUES_COLUMN_INDEX ) );
      field.setPentahoFieldName( item.getText( FIELD_NAME_COLUMN_INDEX ) );
      field.setPentahoType( ValueMetaFactory.getIdForValueMeta( item.getText( FIELD_TYPE_COLUMN_INDEX ) ) );
      field.setStringFormat( item.getText( FORMAT_COLUMN_INDEX ) );
      meta.inputFields[ i ] = field;
    }
  }

  private String extractFieldName( String parquetNameTypeFromUI ) {
    if ( ( parquetNameTypeFromUI != null ) && ( parquetNameTypeFromUI.indexOf( "(" ) >= 0 ) ) {
      return StringUtils.substringBefore( parquetNameTypeFromUI, "(" ).trim();
    }
    return parquetNameTypeFromUI;
  }

  private String getSchemeFromPath( String path ) {
    if ( Utils.isEmpty( path ) ) {
      return SCHEMA_SCHEME_DEFAULT;
    }
    int endIndex = path.indexOf( ':' );
    if ( endIndex > 0 ) {
      return path.substring( 0, endIndex );
    } else {
      return SCHEMA_SCHEME_DEFAULT;
    }
  }

  private void doPreview() {
    getInfo( meta, true );
    TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation( transMeta, meta, wStepname.getText() );
    transMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    previewMeta.getVariable( "Internal.Transformation.Filename.Directory" );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString( PKG,
        "AvroInputDialog.PreviewSize.DialogTitle" ), BaseMessages.getString( PKG,
        "AvroInputDialog.PreviewSize.DialogMessage" ) );
    int previewSize = numberDialog.open();

    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog =
        new TransPreviewProgressDialog( shell, previewMeta, new String[] { wStepname.getText() }, new int[] {
          previewSize } );
      progressDialog.open();

      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog( shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ),
              BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
        new PreviewRowsDialog( shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog.getPreviewRowsMeta(
          wStepname.getText() ), progressDialog.getPreviewRows( wStepname.getText() ), loggingText );
      prd.open();
    }
  }

  private String clearIndexFromFieldName( String fieldName ) {
    String cleanFieldName = fieldName;
    int bracketPos = cleanFieldName.indexOf( "[" );
    if ( bracketPos > 0 ) {
      cleanFieldName = cleanFieldName.substring( 0, bracketPos ) + "[]";
    }

    return cleanFieldName;
  }

  @Override
  protected int getWidth() {
    return SHELL_WIDTH;
  }

  @Override
  protected int getHeight() {
    return SHELL_HEIGHT;
  }

  @Override
  protected String getStepTitle() {
    return BaseMessages.getString( PKG, "AvroInputDialog.Shell.Title" );
  }

  @Override
  protected Listener getPreview() {
    return new Listener() {
      public void handleEvent( Event e ) {
        doPreview();
      }
    };
  }
}
