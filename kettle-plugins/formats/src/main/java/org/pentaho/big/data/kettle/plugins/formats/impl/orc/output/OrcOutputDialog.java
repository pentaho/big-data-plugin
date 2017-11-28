/*******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.formats.impl.orc.output;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.big.data.kettle.plugins.formats.impl.NullableValuesEnum;
import org.pentaho.big.data.kettle.plugins.formats.impl.orc.BaseOrcStepDialog;
import org.pentaho.big.data.kettle.plugins.formats.orc.OrcFormatInputOutputField;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

public class OrcOutputDialog extends BaseOrcStepDialog<OrcOutputMeta> implements StepDialogInterface {

  private static final Class<?> PKG = OrcOutputMeta.class;

  private static final int SHELL_WIDTH = 698;
  private static final int SHELL_HEIGHT = 554;

  private ComboVar wCompression;
  private TextVar wStripeSize;
  private TextVar wCompressSize;
  private Button wInlineIndexes;
  private TextVar wRowsBetweenEntries;
  private Button wDateInFileName;
  private Button wTimeInFileName;
  private Button wSpecifyDateTimeFormat;
  private ComboVar wDateTimeFormat;
  private int startingRowsBetweenEntries = OrcOutputMeta.DEFAULT_ROWS_BETWEEN_ENTRIES;


  private TableView wOutputFields;


  public OrcOutputDialog( Shell parent, Object orcOutputMeta, TransMeta transMeta, String sname ) {
    this( parent, (OrcOutputMeta) orcOutputMeta, transMeta, sname );
  }

  public OrcOutputDialog( Shell parent, OrcOutputMeta orcOutputMeta, TransMeta transMeta, String sname ) {
    super( parent, orcOutputMeta, transMeta, sname );
    this.meta = orcOutputMeta;
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
    return BaseMessages.getString( PKG, "OrcOutputDialog.Shell.Title" );
  }

  private void addFieldsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "OrcOutputDialog.FieldsTab.TabTitle" ) );

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
    wGetFields.setText( BaseMessages.getString( PKG, "OrcOutputDialog.Fields.Get" ) );
    props.setLook( wGetFields );
    new FD( wGetFields ).bottom( 100, 0 ).right( 100, 0 ).apply();

    wGetFields.addListener( SWT.Selection, lsGet );

    ColumnInfo[] parameterColumns = new ColumnInfo[]{
      new ColumnInfo( BaseMessages.getString( PKG, "OrcOutputDialog.Fields.column.Path" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "OrcOutputDialog.Fields.column.Name" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "OrcOutputDialog.Fields.column.Type" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ),
      new ColumnInfo( BaseMessages.getString( PKG, "OrcOutputDialog.Fields.column.Default" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "OrcOutputDialog.Fields.column.Null" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, NullableValuesEnum.getValuesArr(), true )};
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
    wTab.setText( BaseMessages.getString( PKG, "OrcOutputDialog.Options.TabTitle" ) );
    Composite wGrid = new Composite( wTabFolder, SWT.NONE );
    wTab.setControl( wGrid );
    props.setLook( wGrid );

    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = MARGIN;
    formLayout.marginWidth = MARGIN;

    wGrid.setLayout( formLayout );

    Label wLabel = createLabel( wGrid, "OrcOutputDialog.Options.Compression" );
    FormData formData = new FormData();
    formData.top = new FormAttachment( 0, 0 );
    wLabel.setLayoutData( formData );

    wCompression = createComboVar( wGrid, meta.getCompressionTypes() );
    formData = new FormData();
    formData.top = new FormAttachment( wLabel, 5 );
    formData.width = FIELD_SMALL + VAR_EXTRA_WIDTH;
    wCompression.setLayoutData( formData );
    props.setLook( wCompression );

    wLabel = createLabel( wGrid, "OrcOutputDialog.Options.StripeSize" );
    formData = new FormData();
    formData.top = new FormAttachment( wCompression, 10 );
    wLabel.setLayoutData( formData );

    wStripeSize = new TextVar( transMeta, wGrid, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStripeSize );
    formData = new FormData();
    formData.top = new FormAttachment( wLabel, 5 );
    formData.width = FIELD_SMALL + VAR_EXTRA_WIDTH;
    wStripeSize.setLayoutData( formData );
    setIntegerOnly( wStripeSize );
    wStripeSize.addModifyListener( lsMod );

    wLabel = createLabel( wGrid, "OrcOutputDialog.Options.CompressSize" );
    formData = new FormData();
    formData.top = new FormAttachment( wStripeSize, 10 );
    wLabel.setLayoutData( formData );

    wCompressSize = new TextVar( transMeta, wGrid, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCompressSize );
    formData = new FormData();
    formData.top = new FormAttachment( wLabel, 5 );
    formData.width = FIELD_SMALL + VAR_EXTRA_WIDTH;
    wCompressSize.setLayoutData( formData );
    wCompressSize.getTextWidget().addModifyListener( lsMod );
    setIntegerOnly( wCompressSize );
    wCompressSize.addModifyListener( lsMod );

    wInlineIndexes = new Button( wGrid, SWT.CHECK );
    props.setLook( wInlineIndexes );
    wInlineIndexes.setText( BaseMessages.getString( PKG, "OrcOutputDialog.Options.InlineIndexes" ) );
    formData = new FormData();
    formData.top = new FormAttachment( 0, 0 );
    formData.left = new FormAttachment( wCompressSize, 50 );
    wInlineIndexes.setLayoutData( formData );
    wInlineIndexes.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
        boolean isSelected = wInlineIndexes.getSelection();
        if ( isSelected ) {
          wRowsBetweenEntries.setEnabled( true );
          wRowsBetweenEntries.setText( Integer.toString( startingRowsBetweenEntries ) );
        } else {
          wRowsBetweenEntries.setEnabled( false );
          wRowsBetweenEntries.setText( "" );
        }
      }
    } );

    wLabel = createLabel( wGrid, "OrcOutputDialog.Options.RowsBetweenEntries" );
    formData = new FormData();
    formData.top = new FormAttachment( wInlineIndexes, 10 );
    formData.left = new FormAttachment( wCompressSize, 70 );
    wLabel.setLayoutData( formData );

    wRowsBetweenEntries = new TextVar( transMeta, wGrid, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowsBetweenEntries );
    formData = new FormData();
    formData.top = new FormAttachment( wLabel, 5 );
    formData.left = new FormAttachment( wCompressSize, 70 );
    formData.width = FIELD_SMALL + VAR_EXTRA_WIDTH;
    wRowsBetweenEntries.setLayoutData( formData );
    setIntegerOnly( wRowsBetweenEntries );
    wRowsBetweenEntries.addModifyListener( lsMod );

    wDateInFileName = new Button( wGrid, SWT.CHECK );
    props.setLook( wDateInFileName );
    wDateInFileName.setText( BaseMessages.getString( PKG, "OrcOutputDialog.Options.DateInFileName" ) );
    formData = new FormData();
    formData.top = new FormAttachment( wRowsBetweenEntries, 10 );
    formData.left = new FormAttachment( wCompressSize, 50 );
    wDateInFileName.setLayoutData( formData );
    wDateInFileName.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
        boolean isSelected = wDateInFileName.getSelection();
        if ( isSelected ) {
          wSpecifyDateTimeFormat.setSelection( false );
          wDateTimeFormat.setText( "" );
          wDateTimeFormat.setEnabled( false );
        }
      }
    } );


    wTimeInFileName = new Button( wGrid, SWT.CHECK );
    props.setLook( wTimeInFileName );
    wTimeInFileName.setText( BaseMessages.getString( PKG, "OrcOutputDialog.Options.TimeInFileName" ) );
    formData = new FormData();
    formData.top = new FormAttachment( wDateInFileName, 10 );
    formData.left = new FormAttachment( wCompressSize, 50 );
    wTimeInFileName.setLayoutData( formData );
    wTimeInFileName.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
        boolean isSelected = wTimeInFileName.getSelection();
        if ( isSelected ) {
          wSpecifyDateTimeFormat.setSelection( false );
          wDateTimeFormat.setText( "" );
          wDateTimeFormat.setEnabled( false );
        }
      }
    } );


    wSpecifyDateTimeFormat = new Button( wGrid, SWT.CHECK );
    wSpecifyDateTimeFormat.setText( BaseMessages.getString( PKG, "OrcOutputDialog.Options.SpecifyDateTimeFormat" ) );
    props.setLook( wSpecifyDateTimeFormat );
    formData = new FormData();
    formData.top = new FormAttachment( wTimeInFileName, 10 );
    formData.left = new FormAttachment( wCompressSize, 50 );
    wSpecifyDateTimeFormat.setLayoutData( formData );
    wSpecifyDateTimeFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        meta.setChanged();
        boolean isSelected = wSpecifyDateTimeFormat.getSelection();
        wDateTimeFormat.setEnabled( isSelected );
        if ( !isSelected ) {
          wDateTimeFormat.setText( "" );
        } else {
          wTimeInFileName.setSelection( false );
          wDateInFileName.setSelection( false );
        }
      }
    } );

    String[] dates = Const.getDateFormats();
    dates =
      Arrays.stream( dates ).filter( d -> d.indexOf( '/' ) < 0 && d.indexOf( '\\' ) < 0 && d.indexOf( ':' ) < 0 )
        .toArray( String[]::new ); // remove formats with slashes and colons
    wDateTimeFormat = createComboVar( wGrid, dates );
    props.setLook( wDateTimeFormat );
    formData = new FormData();
    formData.top = new FormAttachment( wSpecifyDateTimeFormat, 5 );
    formData.left = new FormAttachment( wCompressSize, 70 );
    wDateTimeFormat.setLayoutData( formData );
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
    stepname = wStepname.getText();

    List<String> validationErrorFields = validateOutputFields( wOutputFields, meta );

    if ( validationErrorFields != null && !validationErrorFields.isEmpty() ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "OrcOutput.MissingDefaultFields.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "OrcOutput.MissingDefaultFields.Msg" ) );
      mb.open();
      return;
    }

    getInfo( meta, false );
    dispose();
  }

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  protected void getData( OrcOutputMeta meta ) {
    if ( meta.getFilename() != null ) {
      wPath.setText( meta.getFilename() );
    }
    populateFieldsUI( meta, wOutputFields );
    wCompression.setText( meta.getCompressionType() );
    wCompressSize.setText( meta.getCompressSize() );

    String rowsBetweenEntries = coalesce( meta.getRowsBetweenEntries() );
    if ( !rowsBetweenEntries.isEmpty() ) {
      startingRowsBetweenEntries = Integer.parseInt( rowsBetweenEntries );
      wInlineIndexes.setSelection( true );
      wRowsBetweenEntries.setText( rowsBetweenEntries );
      wRowsBetweenEntries.setEnabled( true );
    } else {
      startingRowsBetweenEntries = OrcOutputMeta.DEFAULT_ROWS_BETWEEN_ENTRIES;
      wInlineIndexes.setSelection( false );
      wRowsBetweenEntries.setText( "" );
      wRowsBetweenEntries.setEnabled( false );
    }

    wStripeSize.setText( meta.getStripeSize() );

    String dateTimeFormat = coalesce( meta.getDateTimeFormat() );
    if ( !dateTimeFormat.isEmpty() ) {
      wTimeInFileName.setSelection( false );
      wDateInFileName.setSelection( false );
      wSpecifyDateTimeFormat.setSelection( true );
      wDateTimeFormat.setText( dateTimeFormat );
      wDateTimeFormat.setEnabled( true );
    } else {
      wTimeInFileName.setSelection( meta.isTimeInFileName() );
      wDateInFileName.setSelection( meta.isDateInFileName() );
      wSpecifyDateTimeFormat.setSelection( false );
      wDateTimeFormat.setEnabled( false );
      wDateTimeFormat.setText( "" );
    }

  }

  // ui -> meta
  @Override
  protected void getInfo( OrcOutputMeta meta, boolean preview ) {
    meta.setFilename( wPath.getText() );
    meta.setCompressionType( wCompression.getText() );
    meta.setCompressSize( wCompressSize.getText() );
    meta.setStripeSize( wStripeSize.getText() );
    meta.setRowsBetweenEntries( wRowsBetweenEntries.getText().trim() );
    if ( wSpecifyDateTimeFormat.getSelection() ) {
      meta.setTimeInFileName( false );
      meta.setDateInFileName( false );
      meta.setDateTimeFormat( wDateTimeFormat.getText().trim() );
    } else {
      meta.setTimeInFileName( wTimeInFileName.getSelection() );
      meta.setDateInFileName( wDateInFileName.getSelection() );
      meta.setDateTimeFormat( "" );
    }
    saveOutputFields( wOutputFields, meta );
  }

  private void saveOutputFields( TableView wFields, OrcOutputMeta meta ) {
    int nrFields = wFields.nrNonEmpty();

    List<OrcFormatInputOutputField> outputFields = new ArrayList<>();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      int j = 1;
      OrcFormatInputOutputField field = new OrcFormatInputOutputField();
      field.setPath( item.getText( j++ ) );
      field.setName( item.getText( j++ ) );
      field.setType( item.getText( j++ ) );
      field.setIfNullValue( item.getText( j++ ) );
      field.setNullString( getNullableValue( item.getText( j++ ) ) );

      outputFields.add( field );
    }
    meta.setOutputFields( outputFields );
  }

  private List<String> validateOutputFields( TableView wFields, OrcOutputMeta meta ) {
    int nrFields = wFields.nrNonEmpty();
    List<String> validationErrorFields = new ArrayList<String>();

    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      int j = 1;

      String name = item.getText( j++ );
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

  private void populateFieldsUI( OrcOutputMeta meta, TableView wOutputFields ) {
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

  private void populateFieldsUI( List<OrcFormatInputOutputField> fields, TableView wFields,
                                 BiConsumer<OrcFormatInputOutputField, TableItem> converter ) {
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
        BaseStepDialog.getFieldsFromPrevious( r, wOutputFields, 1, new int[]{1, 2}, new int[]{3}, -1, -1, listener );
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

