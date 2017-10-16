/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputOutputField;
import org.pentaho.big.data.kettle.plugins.formats.impl.NullableValuesEnum;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.BaseParquetStepDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
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

import java.util.Arrays;
import java.util.function.BiConsumer;

public class ParquetOutputDialog extends BaseParquetStepDialog<ParquetOutputMeta> implements StepDialogInterface {

  private static final Class<?> PKG = ParquetOutputMeta.class;

  private static final int SHELL_WIDTH = 698;
  private static final int SHELL_HEIGHT = 620;

  private static final int COLUMNS_SEP = 5 * MARGIN;

  private static final int OFFSET = 16;

  private TableView wOutputFields;

  private Button wOverwriteExistingFile;

  private ComboVar wCompression;
  private ComboVar wVersion;
  private TextVar wRowSize;
  private TextVar wPageSize;

  private TextVar wExtension;
  private TextVar wDictPageSize;

  private Label lDict;
  private Button wDictionaryEncoding;
  private Button wIncludeDateInFilename;
  private Button wIncludeTimeInFilename;
  private Button wSpecifyDateTimeFormat;
  private ComboVar wDateTimeFormat;

  public ParquetOutputDialog( Shell parent, Object parquetOutputMeta, TransMeta transMeta, String sname ) {
    this( parent, (ParquetOutputMeta) parquetOutputMeta, transMeta, sname );
  }

  public ParquetOutputDialog( Shell parent, ParquetOutputMeta parquetOutputMeta, TransMeta transMeta, String sname ) {
    super( parent, parquetOutputMeta, transMeta, sname );
    this.meta = parquetOutputMeta;
  }

  protected Control createAfterFile( Composite afterFile ) {
    wOverwriteExistingFile = new Button( afterFile, SWT.CHECK );
    wOverwriteExistingFile.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.OverwriteFile.Label" ) );
    props.setLook( wOverwriteExistingFile );
    new FD( wOverwriteExistingFile ).left( 0, 0 ).top( afterFile, FIELDS_SEP ).apply();
    wOverwriteExistingFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
      }
    } );

    CTabFolder wTabFolder = new CTabFolder( afterFile, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addFieldsTab( wTabFolder );
    addOptionsTab( wTabFolder );

    new FD( wTabFolder ).left( 0, 0 ).top( wOverwriteExistingFile, MARGIN ).right( 100, 0 ).bottom( 100, 0 ).apply();
    wTabFolder.setSelection( 0 );
    return wTabFolder;
  }

  @Override
  protected String getStepTitle() {
    return BaseMessages.getString( PKG, "ParquetOutputDialog.Shell.Title" );
  }

  private void addFieldsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FieldsTab.TabTitle" ) );

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
    wGetFields.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.Get" ) );
    props.setLook( wGetFields );
    new FD( wGetFields ).bottom( 100, 0 ).right( 100, 0 ).apply();

    wGetFields.addListener( SWT.Selection, lsGet );

    ColumnInfo[] parameterColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Path" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Name" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Type" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO,  ValueMetaFactory.getValueMetaNames(), false ),
      new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Default" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Null" ),
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

  private void addOptionsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.TabTitle" ) );
    Composite wComp = new Composite( wTabFolder, SWT.NONE );
    wTab.setControl( wComp );
    props.setLook( wComp );
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = formLayout.marginWidth = MARGIN;
    wComp.setLayout( formLayout );

    Label lCompression = createLabel( wComp, "ParquetOutputDialog.Options.Compression" );
    new FD( lCompression ).left( 0, 0 ).top( wComp, 0 ).apply();
    wCompression = createComboVar( wComp, meta.getCompressionTypes() );
    new FD( wCompression ).left( 0, 0 ).top( lCompression, FIELD_LABEL_SEP ).width( FIELD_TINY + VAR_EXTRA_WIDTH ).apply();

    Label lVersion = createLabel( wComp, "ParquetOutputDialog.Options.Version" );
    new FD( lVersion ).left( 0, 0 ).top( wCompression, FIELDS_SEP ).apply();
    wVersion = createComboVar( wComp, meta.getVersionTypes() );
    new FD( wVersion ).left( 0, 0 ).top( lVersion, FIELD_LABEL_SEP ).width( FIELD_TINY + VAR_EXTRA_WIDTH ).apply();

    Label lRowSize = createLabel( wComp, "ParquetOutputDialog.Options.RowSize" );
    new FD( lRowSize ).left( 0, 0 ).top( wVersion, FIELDS_SEP ).apply();
    wRowSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wRowSize ).left( 0, 0 ).top( lRowSize, FIELD_LABEL_SEP ).width( FIELD_TINY + VAR_EXTRA_WIDTH ).apply();
    setIntegerOnly( wRowSize );
    wRowSize.addModifyListener( lsMod );

    Label lDataPageSize = createLabel( wComp, "ParquetOutputDialog.Options.PageSize" );
    new FD( lDataPageSize ).left( 0, 0 ).top( wRowSize, FIELDS_SEP ).apply();
    wPageSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wPageSize ).left( 0, 0 ).top( lDataPageSize, FIELD_LABEL_SEP ).width( FIELD_TINY + VAR_EXTRA_WIDTH ).apply();
    setIntegerOnly( wPageSize );
    wPageSize.addModifyListener( lsMod );

    wDictionaryEncoding = new Button( wComp, SWT.CHECK );
    wDictionaryEncoding.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.DictionaryEncoding" ) );
    props.setLook( wDictionaryEncoding );
    new FD( wDictionaryEncoding ).left( 0, 0 ).top( wPageSize, FIELDS_SEP ).apply();

    wDictionaryEncoding.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
        actualizeDictionaryPageSizeControl();
      }
    } );

    lDict = new Label( wComp, SWT.NONE );
    lDict.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.DictPageSize" ) );
    new FD( lDict ).left( 0, OFFSET ).top( wDictionaryEncoding, FIELD_LABEL_SEP ).apply();
    wDictPageSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wDictPageSize ).left( 0, OFFSET ).top( lDict, FIELD_LABEL_SEP ).width( FIELD_TINY + VAR_EXTRA_WIDTH - OFFSET ).apply();
    setIntegerOnly( wDictPageSize );
    wDictPageSize.addModifyListener( lsMod );

    Control leftRef = wCompression;
    // 2nd column
    Label lExtension = new Label( wComp, SWT.NONE );
    lExtension.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.Extension" ) );
    new FD( lExtension ).left( leftRef, COLUMNS_SEP ).top( wComp, 0 ).apply();
    wExtension = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wExtension ).left( leftRef, COLUMNS_SEP ).top( lExtension, FIELD_LABEL_SEP )
      .width( FIELD_TINY + VAR_EXTRA_WIDTH ).apply();
    wExtension.addModifyListener( lsMod );

    wIncludeDateInFilename = new Button( wComp, SWT.CHECK );
    wIncludeDateInFilename.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.IncludeDateInFilename" ) );
    props.setLook( wIncludeDateInFilename );
    new FD( wIncludeDateInFilename ).left( leftRef, COLUMNS_SEP ).top( wExtension, MARGIN ).apply();
    wIncludeDateInFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
      }
    } );

    wIncludeTimeInFilename = new Button( wComp, SWT.CHECK );
    wIncludeTimeInFilename.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.IncludeTimeInFilename" ) );
    props.setLook( wIncludeTimeInFilename );
    new FD( wIncludeTimeInFilename ).left( leftRef, COLUMNS_SEP ).top( wIncludeDateInFilename, FIELDS_SEP ).apply();
    wIncludeTimeInFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
      }
    } );

    wSpecifyDateTimeFormat = new Button( wComp, SWT.CHECK );
    wSpecifyDateTimeFormat.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.SpecifyDateTimeFormat" ) );
    props.setLook( wSpecifyDateTimeFormat );
    new FD( wSpecifyDateTimeFormat ).left( leftRef, COLUMNS_SEP ).top( wIncludeTimeInFilename, FIELDS_SEP ).apply();

    wSpecifyDateTimeFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
        wDateTimeFormat.setEnabled( wSpecifyDateTimeFormat.getSelection() );
        actualizeDateTimeControls();
      }
    } );

    String[] dates = Const.getDateFormats();
    dates =
        Arrays.stream( dates ).filter( d -> d.indexOf( '/' ) < 0 && d.indexOf( '\\' ) < 0 && d.indexOf( ':' ) < 0 )
            .toArray( String[]::new ); // remove formats with slashes and colons
    wDateTimeFormat = createComboVar( wComp, dates );
    props.setLook( wDateTimeFormat );
    new FD( wDateTimeFormat ).left( leftRef, COLUMNS_SEP + OFFSET ).top( wSpecifyDateTimeFormat, FIELD_LABEL_SEP )
      .width( 200 ).apply();
    wDateTimeFormat.addModifyListener( lsMod );


  }

  void actualizeDictionaryPageSizeControl() {
    boolean dictionaryEncoding = wDictionaryEncoding.getSelection();
    lDict.setEnabled( dictionaryEncoding );
    wDictPageSize.setEnabled( dictionaryEncoding );
  }

  void actualizeDateTimeControls() {
    boolean allowedToIncludeDateTime = !wSpecifyDateTimeFormat.getSelection();
    wIncludeDateInFilename.setEnabled( allowedToIncludeDateTime );
    wIncludeTimeInFilename.setEnabled( allowedToIncludeDateTime );
    if ( !allowedToIncludeDateTime ) {
      wIncludeDateInFilename.setSelection( false );
      wIncludeTimeInFilename.setSelection( false );
    }
  }

  protected ComboVar createComboVar( Composite container, String[] options ) {
    ComboVar combo = new ComboVar( transMeta, container, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
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

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  protected void getData( ParquetOutputMeta meta ) {
    if ( meta.getFilename() != null ) {
      wPath.setText( meta.getFilename() );
    }
    wOverwriteExistingFile.setSelection( meta.isOverrideOutput() );
    populateFieldsUI( meta, wOutputFields );
    wCompression.setText( coalesce( meta.getCompressionType() ) );
    wVersion.setText( coalesce( meta.getParquetVersion() ) );
    wDictionaryEncoding.setSelection( meta.isEnableDictionary() );

    wDictPageSize.setText( coalesce( meta.getDictPageSize() ) );
    wRowSize.setText( coalesce( meta.getRowGroupSize() ) );
    wPageSize.setText( coalesce( meta.getDataPageSize() ) );
    wExtension.setText( coalesce( meta.getExtension() ) );
    wIncludeDateInFilename.setSelection( meta.isDateInFilename() );
    wIncludeTimeInFilename.setSelection( meta.isTimeInFilename() );

    String dateTimeFormat = coalesce( meta.getDateTimeFormat() );
    if ( !dateTimeFormat.isEmpty() ) {
      wSpecifyDateTimeFormat.setSelection( true );
      wDateTimeFormat.setText( dateTimeFormat );
    } else {
      wSpecifyDateTimeFormat.setSelection( false );
      wDateTimeFormat.setEnabled( false );
    }

    actualizeDictionaryPageSizeControl();
    actualizeDateTimeControls();
  }

  private String coalesce( String value ) {
    return value == null ? "" : value;
  }

  // ui -> meta
  @Override
  protected void getInfo( ParquetOutputMeta meta, boolean preview ) {
    meta.setFilename( wPath.getText() );
    meta.setOverrideOutput( wOverwriteExistingFile.getSelection() );
    saveOutputFields( wOutputFields, meta );
    meta.setCompressionType( wCompression.getText() );
    meta.setParquetVersion( wVersion.getText() );
    meta.setEnableDictionary( wDictionaryEncoding.getSelection() );
    meta.setDictPageSize( wDictPageSize.getText() );
    meta.setRowGroupSize( wRowSize.getText() );
    meta.setDataPageSize( wPageSize.getText() );
    meta.setExtension( wExtension.getText() );
    if ( wSpecifyDateTimeFormat.getSelection() ) {
      meta.setDateTimeFormat( wDateTimeFormat.getText() );
      meta.setDateInFilename( false );
      meta.setTimeInFilename( false );
    } else {
      meta.setDateTimeFormat( null );
      meta.setDateInFilename( wIncludeDateInFilename.getSelection() );
      meta.setTimeInFilename( wIncludeTimeInFilename.getSelection() );
    }
  }

  private void saveOutputFields( TableView wFields, ParquetOutputMeta meta ) {
    int nrFields = wFields.nrNonEmpty();

    FormatInputOutputField[] outputFields = new FormatInputOutputField[nrFields];
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      int j = 1;
      FormatInputOutputField field = new FormatInputOutputField();
      field.setPath( item.getText( j++ ) );
      field.setName( item.getText( j++ ) );
      field.setType( item.getText( j++ ) );
      field.setIfNullValue( item.getText( j++ ) );
      field.setNullable( NullableValuesEnum.YES.getValue().equals( item.getText( j++ ) ) );
      outputFields[i] = field;
    }
    meta.outputFields = outputFields;
  }

  private void populateFieldsUI( ParquetOutputMeta meta, TableView wOutputFields ) {
    populateFieldsUI( meta.outputFields, wOutputFields, ( field, item ) -> {
      int i = 1;
      item.setText( i++, coalesce( field.getPath() ) );
      item.setText( i++, coalesce( field.getName() ) );
      item.setText( i++, coalesce( field.getTypeDesc() ) );
      item.setText( i++, coalesce( field.getIfNullValue() ) );
      item.setText( i++, field.isNullable() ? NullableValuesEnum.YES.getValue() : NullableValuesEnum.NO.getValue() );
    } );
  }

  private void populateFieldsUI( FormatInputOutputField[] fields, TableView wFields,
      BiConsumer<FormatInputOutputField, TableItem> converter ) {
    for ( int i = 0; i < fields.length; i++ ) {
      TableItem item = null;
      if ( i < wFields.table.getItemCount() ) {
        item = wFields.table.getItem( i );
      } else {
        item = new TableItem( wFields.table, SWT.NONE );
      }
      converter.accept( fields[i], item );
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
        BaseStepDialog.getFieldsFromPrevious( r, wOutputFields, 1, new int[] { 1, 2 }, new int[] { 3 }, -1, -1, false,
          listener );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
          .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
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
    // no preview
    return null;
  }
}

