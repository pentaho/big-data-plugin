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

package org.pentaho.big.data.kettle.plugins.formats.avro.output;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.big.data.kettle.plugins.formats.avro.BaseAvroStepDialog;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
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

public class AvroOutputDialog extends BaseAvroStepDialog<AvroOutputMeta> implements StepDialogInterface {

  private static final Class<?> PKG = AvroOutputMetaBase.class;

  private static final int SHELL_WIDTH = 698;
  private static final int SHELL_HEIGHT = 554;

  private static final int COLUMNS_SEP = 5 * MARGIN;

  private TableView wOutputFields;

  private ComboVar wCompression;
  private ComboVar wVersion;
  private TextVar wRowSize;
  private TextVar wPageSize;
  private ComboVar wEncoding;
  private TextVar wDictPageSize;


  public AvroOutputDialog( Shell parent, Object parquetOutputMeta, TransMeta transMeta, String sname ) {
    this( parent, (AvroOutputMeta) parquetOutputMeta, transMeta, sname );
  }

  public AvroOutputDialog( Shell parent, AvroOutputMeta parquetOutputMeta, TransMeta transMeta, String sname ) {
    super( parent, parquetOutputMeta, transMeta, sname );
    this.meta = parquetOutputMeta;
  }

  // TODO name
  protected Control createAfterFile( Composite afterFile ) {
    CTabFolder wTabFolder = new CTabFolder( afterFile, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addFieldsTab( wTabFolder );
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
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Default" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "AvroOutputDialog.Fields.column.Null" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, false ) };
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

    Label lVersion = createLabel( wComp, "AvroOutputDialog.Options.Version" );
    new FD( lVersion ).left( 0, 0 ).top( wCompression, FIELDS_SEP ).apply();
    wVersion = createComboVar( wComp, meta.getVersionTypes() );
    new FD( wVersion ).left( 0, 0 ).top( lVersion, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH ).apply();

    Label lRowSize = createLabel( wComp, "AvroOutputDialog.Options.RowSize" );
    new FD( lRowSize ).left( 0, 0 ).top( wVersion, FIELDS_SEP ).apply();
    wRowSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wRowSize ).left( 0, 0 ).top( lRowSize, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH ).apply();
    setIntegerOnly( wRowSize );

    Label lDataPageSize = createLabel( wComp, "AvroOutputDialog.Options.PageSize" );
    new FD( lDataPageSize ).left( 0, 0 ).top( wRowSize, FIELDS_SEP ).apply();
    wPageSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wPageSize ).left( 0, 0 ).top( lDataPageSize, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH ).apply();
    setIntegerOnly( wPageSize );

    Control leftRef = wCompression;
    // 2nd column
    Label lEncoding = new Label( wComp, SWT.NONE );
    lEncoding.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Options.Encoding" ) );
    new FD( lEncoding ).left( leftRef, COLUMNS_SEP ).top( wComp, 0 ).apply();
    wEncoding = createComboVar( wComp, meta.getEncodingTypes() );
    new FD( wEncoding ).left( leftRef, COLUMNS_SEP ).top( lEncoding, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH ).apply();

    Label lDict = new Label( wComp, SWT.NONE );
    lDict.setText( BaseMessages.getString( PKG, "AvroOutputDialog.Options.DictPageSize" ) );
    new FD( lDict ).left( leftRef, COLUMNS_SEP ).top( wEncoding, FIELDS_SEP ).apply();
    wDictPageSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    new FD( wDictPageSize ).left( leftRef, COLUMNS_SEP ).top( lDict, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH ).apply();
    setIntegerOnly( wDictPageSize );
    wEncoding.addModifyListener( new ModifyListener() {
      @Override
      public void modifyText( ModifyEvent e ) {
        wDictPageSize.setEnabled( wEncoding.getText().equals( AvroOutputMeta.EncodingType.DICTIONARY.toString() ) );
      }
    } );
  }

  protected ComboVar createComboVar( Composite container, String[] options ) {
    ComboVar combo = new ComboVar( transMeta, container, SWT.LEFT | SWT.BORDER );
    combo.setItems( options );
    return combo;
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
  protected void getData( AvroOutputMeta meta ) {
    if ( meta.getFilename() != null ) {
      wPath.setText( meta.getFilename() );
    }
    populateFieldsUI( meta, wOutputFields );
    wCompression.setText( meta.getCompression() );
    wEncoding.setText( meta.getEncoding() );
    wVersion.setText( meta.getAvroVersion() );
  }

  // ui -> meta
  @Override
  protected void getInfo( AvroOutputMeta meta, boolean preview ) {
    meta.setFilename( wPath.getText() );
    // TODO
    saveOutputFields( wOutputFields, meta );
  }

  private void saveOutputFields( TableView wFields, AvroOutputMeta meta ) {
    int nrFields = wFields.nrNonEmpty();

    List<FormatInputField> outputFields = new ArrayList<>();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      int j = 1;
      FormatInputField field = new FormatInputField();
      field.setPath( item.getText( j++ ) );
      field.setName( item.getText( j++ ) );
      field.setType( item.getText( j++ ) );
      field.setIfNullValue( item.getText( j++ ) );
      field.setNullString( item.getText( j++ ) );
      outputFields.add( field );
    }
    meta.setOutputFields( outputFields );
  }

  private void populateFieldsUI( AvroOutputMeta meta, TableView wOutputFields ) {
    // path / name / type / default / null
    wOutputFields.getTable().removeAll();
    for ( FormatInputField field : meta.getOutputFields() ) {
      wOutputFields.add( field.getPath(), field.getName(), field.getTypeDesc(), field.getIfNullValue(), field.getNullString() );
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
        BaseStepDialog.getFieldsFromPrevious( r, wOutputFields, 1, new int[] { 1 }, new int[] { 3 }, -1, -1,
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
    return null;
  }
}

