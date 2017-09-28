/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.avro.output;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputOutputField;
import org.pentaho.big.data.kettle.plugins.formats.impl.avro.BaseAvroStepDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ColumnsResizer;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class AvroOutputDialog extends BaseAvroStepDialog<AvroOutputMeta> implements StepDialogInterface {

  private static final Class<?> PKG = AvroOutputMeta.class;

  private static final int SHELL_WIDTH = 698;
  private static final int SHELL_HEIGHT = 554;

  private static final int COLUMNS_SEP = 5 * MARGIN;

  private static final String SCHEMA_SCHEME_DEFAULT = "hdfs";

  private TableView wOutputFields;

  protected ComboVar wCompression;
  protected TextVar wRecordName;
  protected TextVar wDocValue;
  protected TextVar wNameSpace;
  protected TextVar wSchemaPath;
  protected Button wbSchemaBrowse;

  public AvroOutputDialog( Shell parent, Object avroOutputMeta, TransMeta transMeta, String sname ) {
    this( parent, (AvroOutputMeta) avroOutputMeta, transMeta, sname );
  }

  public AvroOutputDialog( Shell parent, AvroOutputMeta avroOutputMeta, TransMeta transMeta, String sname ) {
    super( parent, avroOutputMeta, transMeta, sname );
    this.meta = avroOutputMeta;
  }

  // TODO name
  protected Control createAfterFile( Composite afterFile ) {
    CTabFolder wTabFolder = new CTabFolder( afterFile, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addFieldsTab( wTabFolder );
    addSchemaTab( wTabFolder );
    addOptionsTab( wTabFolder );

    new FD( wTabFolder ).left( 0, 0 ).top( 0, MARGIN ).right( 100, 0 ).bottom( 100, 0 ).apply();
    wTabFolder.setSelection( 0 );
    return wTabFolder;
  }

  @Override
  protected String getStepTitle() {
    return BaseMessages.getString( PKG, "AvroOutputDialog.Shell.Title" );
  }

  private void addFieldsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "AvroOutputDialog.FieldsTab.TabTitle" ) );

    Composite wComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wComp );

    FormLayout layout = new FormLayout();
    layout.marginWidth = MARGIN;
    layout.marginHeight = MARGIN;
    wComp.setLayout( layout );

    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        getFields();
      }
    };

    Button wGetFields = new Button( wComp, SWT.PUSH );
    wGetFields.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.Get" ) );
    props.setLook( wGetFields );
    new FD( wGetFields ).bottom( 100, 0 ).right( 100, 0 ).apply();

    wGetFields.addListener( SWT.Selection, lsGet );

    ColumnInfo[] parameterColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Path" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Name" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Type" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Default" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Null" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, NullableValuesEnum.getValuesArr(), true ) };
    parameterColumns[0].setAutoResize( false );
    parameterColumns[1].setUsingVariables( true );
    wOutputFields =
        new TableView( transMeta, wComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER | SWT.NO_SCROLL | SWT.V_SCROLL,
            parameterColumns, 7, lsMod, props );
    ColumnsResizer resizer = new ColumnsResizer( 0, 30, 20, 20, 20, 10 );
    wOutputFields.getTable().addListener( SWT.Resize, resizer );

    props.setLook( wOutputFields );
    new FD( wOutputFields ).left( 0, 0 ).right( 100, 0 ).top( wComp, 0 ).bottom( wGetFields, -FIELDS_SEP ).apply();

    wOutputFields.setRowNums();
    wOutputFields.optWidth( true );

    new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply();

    wTab.setControl( wComp );
    for ( ColumnInfo col : parameterColumns ) {
      col.setAutoResize( false );
    }
    resizer.addColumnResizeListeners( wOutputFields.getTable() );
    setTruncatedColumn( wOutputFields.getTable(), 1 );
    if ( !Const.isWindows() ) {
      addColumnTooltip( wOutputFields.getTable(), 1 );
    }
  }

  /**
   * Enum with valid list of Nullable values - used for the Nullable combo box
   *
   * Also contains convience methods to get the default value and return a list of values as string to populate combo box
    */
  protected enum NullableValuesEnum {
    YES( "Yes" ),
    NO( "No" );

    private String value;

    NullableValuesEnum( String value ) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public static NullableValuesEnum getDefaultValue() {
      return NullableValuesEnum.YES;
    }

    public static String[] getValuesArr() {
      String[] valueArr = new String[NullableValuesEnum.values().length];

      int i = 0;

      for ( NullableValuesEnum nullValueEnum : NullableValuesEnum.values() ) {
        valueArr[i++] = nullValueEnum.getValue();
      }

      return valueArr;
    }
  }

  private void addSchemaTab( CTabFolder wTabFolder ) {
    // Create & Set up a new Tab Item
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.TabTitle" ) );
    Composite wTabComposite = new Composite( wTabFolder, SWT.NONE );
    wTab.setControl( wTabComposite );
    props.setLook( wTabComposite );
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = MARGIN;
    wTabComposite.setLayout( formLayout );

    // Set up the Source Group
    Group wSourceGroup = new Group( wTabComposite, SWT.SHADOW_NONE );
    props.setLook( wSourceGroup );
    wSourceGroup.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.SourceTitle" ) );

    FormLayout layout = new FormLayout();
    layout.marginWidth = MARGIN;
    wSourceGroup.setLayout( layout );

    FormData fdSource = new FormData();
    fdSource.top = new FormAttachment( 0, 0 );
    fdSource.right = new FormAttachment( 100, -15 );
    fdSource.left = new FormAttachment( 0, 15 );

    wSourceGroup.setLayoutData( fdSource );

    Label wlSchemaPath = new Label( wSourceGroup, SWT.RIGHT );
    wlSchemaPath.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.FileName" ) );
    props.setLook( wlSchemaPath );
    new FD( wlSchemaPath ).left( 0, 0 ).top( 0, 0 ).apply();
    wSchemaPath = new TextVar( transMeta, wSourceGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchemaPath );
    new FD( wSchemaPath ).left( 0, 0 ).top( wlSchemaPath, FIELD_LABEL_SEP ).width( FIELD_LARGE + VAR_EXTRA_WIDTH )
        .rright().apply();

    wbSchemaBrowse = new Button( wSourceGroup, SWT.PUSH );
    props.setLook( wbBrowse );
    wbSchemaBrowse.setText( getMsg( "System.Button.Browse" ) );
    wbSchemaBrowse.addListener( SWT.Selection, event -> browseForFileInputPathForSchema() );
    int bOffset = ( wbSchemaBrowse.computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y - wSchemaPath.
        computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y ) / 2;
    new FD( wbSchemaBrowse ).left( wSchemaPath, FIELD_LABEL_SEP ).top( wlSchemaPath, FIELD_LABEL_SEP - bOffset )
        .apply();

//      http://jira.pentaho.com/browse/BACKLOG-18706 - Tech Debt - Remove commented Code in AvroOutputDialog when new requirements come in
//        The code below is commented out because the radio buttons were descoped for this release.  They will be
//        added back in scope for a future release.  When new requirements come in, either A) uncomment the code or
//        B) remove the code.

//    // Set up the From File Button
//    Button wFromFileButton = new Button(wSourceGroup, SWT.RADIO);
//    wFromFileButton.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.FromFile" ) );
//    props.setLook( wFromFileButton );
//    FormData fdFromFile = new FormData();
//    fdFromFile.top = new FormAttachment( 0, 10 );
//    fdFromFile.left = new FormAttachment( 0, 10 );
//    wFromFileButton.setLayoutData( fdFromFile );
//    wFromFileButton.addSelectionListener( new SelectionAdapter() {
//      public void widgetSelected( SelectionEvent e ) {
//        stackedLayout.topControl = fromFileComposite;
//        stackedLayoutComposite.layout();
//      }
//    } );
//
//    // Set up the From Field Button
//    Button wFromFieldButton = new Button(wSourceGroup, SWT.RADIO);
//    wFromFieldButton.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.FromField" ) );
//    props.setLook( wFromFieldButton );
//    FormData fdFromField = new FormData();
//    fdFromField.left = new FormAttachment( 0, 10 );
//    fdFromField.top = new FormAttachment( wFromFileButton, 7 );
//    wFromFieldButton.setLayoutData( fdFromField );
//    wFromFieldButton.addSelectionListener( new SelectionAdapter() {
//      public void widgetSelected( SelectionEvent e ) {
//        stackedLayout.topControl = fromFieldComposite;
//        stackedLayoutComposite.layout();
//      }
//    } );
//
//    // separator
//    Label fileFieldSeparator = new Label( wSourceGroup, SWT.SEPARATOR | SWT.VERTICAL );
//    FormData fd_environmentSeparator = new FormData();
//    fd_environmentSeparator.top = new FormAttachment( 0, 10 );
//    fd_environmentSeparator.left = new FormAttachment( wFromFileButton, 50 );
//    fd_environmentSeparator.bottom = new FormAttachment( 100, -10 );
//    fileFieldSeparator.setLayoutData( fd_environmentSeparator );
//
//
//    // stacked layout composite
//    stackedLayoutComposite = new Composite( wSourceGroup, SWT.NONE );
//    props.setLook( stackedLayoutComposite );
//    stackedLayout = new StackLayout();
//    stackedLayoutComposite.setLayout( stackedLayout );
//    FormData fd_stackedLayoutComposite = new FormData();
//    fd_stackedLayoutComposite.top = new FormAttachment( 0 );
//    fd_stackedLayoutComposite.left = new FormAttachment( fileFieldSeparator, 30 );
//    fd_stackedLayoutComposite.bottom = new FormAttachment( 100, -10 );
//    fd_stackedLayoutComposite.right = new FormAttachment( 100, -7 );
//    stackedLayoutComposite.setLayoutData( fd_stackedLayoutComposite );
//
//    // Set up the From File Stack Composite (this will be shown when user clicks on "From file:" radio button
//    fromFileComposite = new Composite( stackedLayoutComposite, SWT.NONE );
//    fromFileComposite.setLayout( new FormLayout() );
//    props.setLook( fromFileComposite );
//    addSchemaFileWidgets( fromFileComposite, fileFieldSeparator );
//
//    // Set up the From field Stack Composite (this will be shown when user clicks on "From field:" radio button
//    fromFieldComposite = new Composite( stackedLayoutComposite, SWT.NONE );
//    fromFieldComposite.setLayout( new FormLayout() );
//    props.setLook( fromFieldComposite );
//    addSchemaFieldWidgets( fromFieldComposite, fileFieldSeparator );
//
//    stackedLayout.topControl = fromFileComposite;

    // Set up Avro Details Group
    Group wAvroDetailsGroup = new Group( wTabComposite, SWT.SHADOW_NONE );
    props.setLook( wAvroDetailsGroup );
    wAvroDetailsGroup.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.AvroDetailsTitle" ) );

    layout = new FormLayout();
    layout.marginWidth = MARGIN;
    layout.marginHeight = MARGIN;
    wSourceGroup.setLayout( layout );

    wAvroDetailsGroup.setLayout( layout );

    FormData fdAvroDetailsGroup = new FormData();
    fdAvroDetailsGroup.top = new FormAttachment( wSourceGroup, 15 );
    fdAvroDetailsGroup.right = new FormAttachment( 100, -15 );
    fdAvroDetailsGroup.left = new FormAttachment( 0, 15 );

    wAvroDetailsGroup.setLayoutData( fdAvroDetailsGroup );
    // Set up the Namespace Text Box
    Label wlNameSpace = new Label( wAvroDetailsGroup, SWT.RIGHT );
    wlNameSpace.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.Namespace" ) );
    props.setLook( wlNameSpace );
    new FD( wlNameSpace ).left( 0, 0 ).top( 0, 0 ).apply();
    wNameSpace = new TextVar( transMeta, wAvroDetailsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wNameSpace );
    new FD( wNameSpace ).left( 0, 0 ).top( wlNameSpace, FIELD_LABEL_SEP ).width( 250 ).rright().apply();

    // Set up the RecordName Text Box
    Label wlRecordName = new Label( wAvroDetailsGroup, SWT.RIGHT );
    wlRecordName.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.RecordName" ) );
    props.setLook( wlRecordName );
    new FD( wlRecordName ).left( 0, 0 ).top( wNameSpace, FIELDS_SEP ).apply();
    wRecordName = new TextVar( transMeta, wAvroDetailsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRecordName );
    new FD( wRecordName ).left( 0, 0 ).top( wlRecordName, FIELD_LABEL_SEP ).width( 250 ).rright().apply();

    // Set up the DocValue Text Box
    Label wlDocValue = new Label( wAvroDetailsGroup, SWT.RIGHT );
    wlDocValue.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Schema.DocValue" ) );
    props.setLook( wlDocValue );
    new FD( wlDocValue ).left( 0, 0 ).top( wRecordName, FIELDS_SEP ).apply();
    wDocValue = new TextVar( transMeta, wAvroDetailsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDocValue );
    new FD( wDocValue ).left( 0, 0 ).top( wlDocValue, FIELD_LABEL_SEP ).width( 250 ).rright().apply();
  }

  private void addOptionsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Options.TabTitle" ) );
    Composite wComp = new Composite( wTabFolder, SWT.NONE );
    wTab.setControl( wComp );
    props.setLook( wComp );
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = formLayout.marginWidth = MARGIN;
    wComp.setLayout( formLayout );

    Label lCompression = createLabel( wComp, "AvroOutputDialog.Options.Compression" );
    new FD( lCompression ).left( 0, 0 ).top( wComp, 0 ).apply();
    wCompression = createComboVar( wComp, meta.getCompressionTypes() );
    new FD( wCompression ).left( 0, 0 ).top( lCompression, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH ).apply();

  }

  protected ComboVar createComboVar( Composite container, String[] options ) {
    ComboVar combo = new ComboVar( transMeta, container, SWT.LEFT | SWT.BORDER );
    combo.setItems( options );
    combo.addModifyListener( lsMod );
    return combo;
  }

  protected String getComboVarValue( ComboVar combo ) {
    String text = combo.getText();
    String data = (String) combo.getData( text );
    return data != null ? data : text;
  }

  private Label createLabel( Composite container, String labelRef ) {
    Label label = new Label( container, SWT.NONE );
    label.setText( BaseMessages.getString( PKG, labelRef ) );
    props.setLook( label );
    return label;
  }

  @Override
  protected void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    List<String> validationErrorFields = validateOutputFields( wOutputFields, meta );

    if ( validationErrorFields != null && !validationErrorFields.isEmpty() ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "AvroOutput.MissingDefaultFields.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "AvroOutput.MissingDefaultFields.Msg" ) );
      mb.open();
      return;
    }

    getInfo( meta, false );
    dispose();
  }

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  protected void getData( AvroOutputMeta meta ) {
    if ( meta.getFilename() != null ) {
      wPath.setText( meta.getFilename() );
    }
    if ( meta.getSchemaFilename() != null ) {
      wSchemaPath.setText( meta.getSchemaFilename() );
    }
    if ( meta.getDocValue() != null ) {
      wDocValue.setText( meta.getDocValue() );
    }
    if ( meta.getNamespace() != null ) {
      wNameSpace.setText( meta.getNamespace() );
    }
    if ( meta.getRecordName() != null ) {
      wRecordName.setText( meta.getRecordName() );
    }
    populateFieldsUI( meta, wOutputFields );
    wCompression.setText( meta.getCompressionType() );
  }

  // ui -> meta
  @Override
  protected void getInfo( AvroOutputMeta meta, boolean preview ) {
    meta.setFilename( wPath.getText() );
    meta.setDocValue( wDocValue.getText() );
    meta.setNamespace( wNameSpace.getText() );
    meta.setRecordName( wRecordName.getText() );
    meta.setSchemaFilename( wSchemaPath.getText() );
    meta.setCompressionType( wCompression.getText() );

    saveOutputFields( wOutputFields, meta );
  }

  private void saveOutputFields( TableView wFields, AvroOutputMeta meta ) {
    int nrFields = wFields.nrNonEmpty();

    List<FormatInputOutputField> outputFields = new ArrayList<>();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      int j = 1;
      FormatInputOutputField field = new FormatInputOutputField();
      field.setPath( item.getText( j++ ) );
      field.setName( item.getText( j++ ) );
      field.setType( item.getText( j++ ) );
      field.setIfNullValue( item.getText( j++ ) );
      field.setNullString( getNullableValue( item.getText( j++ ) ) );

      outputFields.add( field );
    }
    meta.setOutputFields( outputFields );
  }

  private List<String> validateOutputFields( TableView wFields, AvroOutputMeta meta ) {
    int nrFields = wFields.nrNonEmpty();
    List<String> validationErrorFields = new ArrayList<String>();

    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      int j = 1;

      String path = item.getText( j++ );
      String name = item.getText( j++ );
      String type = item.getText( j++ );
      String defaultValue = item.getText( j++ );
      String nullString = getNullableValue( item.getText( j++ ) );

      if ( nullString.equals( NullableValuesEnum.NO.getValue() ) && ( defaultValue == null || defaultValue.trim().isEmpty() ) ) {
        validationErrorFields.add( name );
      }
    }
    return validationErrorFields;
  }

  private String getNullableValue( String nullString ) {
    return ( nullString != null && !nullString.isEmpty() ) ? nullString : NullableValuesEnum.getDefaultValue().getValue();
  }

  private void populateFieldsUI( AvroOutputMeta meta, TableView wOutputFields ) {
    populateFieldsUI( meta.getOutputFields(), wOutputFields, ( field, item ) -> {
      int i = 1;
      item.setText( i++, coalesce( field.getPath() ) );
      item.setText( i++, coalesce( field.getName() ) );
      item.setText( i++, coalesce( field.getTypeDesc() ) );
      item.setText( i++, coalesce( field.getIfNullValue() ) );
      item.setText( i++, coalesce( field.getNullString() ) );
    } );
  }

  private String coalesce( String value ) {
    return value == null ? "" : value;
  }

  private void populateFieldsUI( List<FormatInputOutputField> fields, TableView wFields,
      BiConsumer<FormatInputOutputField, TableItem> converter ) {
    int nrFields = fields.size();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = null;
      if ( i < wFields.table.getItemCount() ) {
        item = wFields.table.getItem( i );
      } else {
        item = new TableItem( wFields.table, SWT.NONE );
      }
      converter.accept( fields.get( i ), item );
    }
  }

  protected void getFields() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null ) {
        TableItemInsertListener listener = new TableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, ValueMetaInterface v ) {
            return true;
          }
        };
        BaseStepDialog.getFieldsFromPrevious( r, wOutputFields, 1, new int[] { 1, 2 }, new int[] { 3 }, -1, -1, listener );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }
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

      FileObject
          selectedFile =
          fileChooserDialog
              .open( shell, null, getSchemeFromPath( path ), true, fileName, FILES_FILTERS, fileFilterNames, true,
                  VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, true, true );
      if ( selectedFile != null ) {
        wSchemaPath.setText( selectedFile.getURL().toString() );
      }
    } catch ( KettleFileException ex ) {
      log.logError( getBaseMsg( "AvroInputDialog.SchemaFileBrowser.KettleFileException" ) );
    } catch ( FileSystemException ex ) {
      log.logError( getBaseMsg( "AvroInputDialog.SchemaFileBrowser.FileSystemException" ) );
    }
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
  protected Listener getPreview() {
    return null;
  }
}

