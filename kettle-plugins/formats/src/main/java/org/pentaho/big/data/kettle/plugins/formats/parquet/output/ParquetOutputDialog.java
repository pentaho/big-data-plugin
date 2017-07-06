package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.step.TableItemInsertListener;
import org.pentaho.di.ui.util.SwtSvgImageUtil;

public class ParquetOutputDialog extends BaseStepDialog implements StepDialogInterface {

  private static final Class<?> PKG = ParquetOutputMetaBase.class;

  private ParquetOutputMetaBase parquetOutputMeta;

  protected boolean transModified;

  private ModifyListener lsMod;

  private TableView wOutputFields;

  private Label wlPath;

  private TextVar wPath;

  private Button wbBrowse;
  
  private Label wlWriterVersion;
  
  private CCombo wWriterVersion;
  
  private Label wlRowGroupSize;

  private TextVar wRowGroupSize;
  
  private Label wlPageSize;

  private TextVar wPageSize;
  
  private Label wlEncoding;
  
  private CCombo wEncoding;
  
  private Label wlDictionaryPageSize;

  private TextVar wDictionaryPageSize;

  public ParquetOutputDialog( Shell parent, Object parquetOutputMeta, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) parquetOutputMeta, transMeta, sname );
    this.parquetOutputMeta = (ParquetOutputMetaBase) parquetOutputMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, parquetOutputMeta );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        parquetOutputMeta.setChanged();
      }
    };
    changed = parquetOutputMeta.hasChanged();

    createUI();

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( parquetOutputMeta );
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected void cancel() {
    stepname = null;
    parquetOutputMeta.setChanged( changed );
    dispose();
  }

  protected void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    getInfo( parquetOutputMeta, false );
    dispose();
  }

  protected void createUI() {
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Shell.Title" ) );

    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    props.setLook( wicon );
    new FD( wicon ).top( 0, 0 ).right( 100, 0 ).apply();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.StepName.Label" ) );
    props.setLook( wlStepname );
    new FD( wlStepname ).left( 0, 0 ).top( 0, 0 ).apply();

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    new FD( wStepname ).left( 0, 0 ).top( wlStepname, 5 ).width( 250 ).apply();

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    new FD( spacer ).height( 1 ).left( 0, 0 ).top( wStepname, 15 ).right( 100, 0 ).apply();

    CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addFilesTab( wTabFolder );
    addFieldsTab( wTabFolder );
    addOptionsTab( wTabFolder );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, lsCancel );
    new FD( wCancel ).right( 100, 0 ).bottom( 100, 0 ).apply();

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, lsOK );
    new FD( wOK ).right( wCancel, -5 ).bottom( 100, 0 ).apply();

    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Preview.Button" ) );
    new FD( wPreview ).right( wOK, -50 ).bottom( 100, 0 ).apply();

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    new FD( hSpacer ).height( 1 ).left( 0, 0 ).bottom( wCancel, -15 ).right( 100, 0 ).apply();

    new FD( wTabFolder ).left( 0, 0 ).top( spacer, 15 ).right( 100, 0 ).bottom( hSpacer, -15 ).apply();
    wTabFolder.setSelection( 0 );

  }

  private void addFilesTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FileTab.TabTitle" ) );

    ScrolledComposite wSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wSComp.setLayout( new FillLayout() );

    Composite wComp = new Composite( wSComp, SWT.NONE );
    props.setLook( wComp );

    FormLayout layout = new FormLayout();
    layout.marginWidth = 15;
    layout.marginHeight = 15;
    wComp.setLayout( layout );

    wlPath = new Label( wComp, SWT.LEFT );
    props.setLook( wlPath );
    wlPath.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Filename.Label" ) );
    new FD( wlPath ).left( 0, 0 ).right( 50, 0 ).top( 0, 0 ).apply();

    wbBrowse = new Button( wComp, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    new FD( wbBrowse ).top( wlPath, 5 ).right( 100, 0 ).apply();

    wPath = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    new FD( wPath ).left( 0, 0 ).top( wlPath, 5 ).right( wbBrowse, -10 ).apply();

    new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply();
    wComp.pack();

    Rectangle bounds = wComp.getBounds();
    wSComp.setContent( wComp );
    wSComp.setExpandHorizontal( true );
    wSComp.setExpandVertical( true );
    wSComp.setMinWidth( bounds.width );
    wSComp.setMinHeight( bounds.height );

    wTab.setControl( wSComp );

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        DirectoryDialog dialog = new DirectoryDialog( shell, SWT.OPEN );
        if ( wPath.getText() != null ) {
          String fpath = transMeta.environmentSubstitute( wPath.getText() );
          dialog.setFilterPath( fpath );
        }

        if ( dialog.open() != null ) {
          String str = dialog.getFilterPath();
          wPath.setText( str );
        }
      }
    } );
  }

  private void addFieldsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.FieldsTab.TabTitle" ) );

    ScrolledComposite wSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wSComp.setLayout( new FillLayout() );

    Composite wComp = new Composite( wSComp, SWT.NONE );
    props.setLook( wComp );

    FormLayout layout = new FormLayout();
    layout.marginWidth = 15;
    layout.marginHeight = 15;
    wComp.setLayout( layout );

    Button wIgnore = new Button( wComp, SWT.CHECK );
    wIgnore.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.Ignore" ) );
    props.setLook( wIgnore );
    new FD( wIgnore ).left( 0, 0 ).top( 0, 0 ).apply();
    // wIgnore.setSelection( jobExecutorMeta.getParameters().isInheritingAllVariables() );

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

    ColumnInfo[] parameterColumns =
        new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Name" ),
            ColumnInfo.COLUMN_TYPE_TEXT, false, false ), new ColumnInfo( BaseMessages.getString( PKG,
                "ParquetOutputDialog.Fields.column.Path" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {}, false ),
          new ColumnInfo( BaseMessages.getString( PKG, "ParquetOutputDialog.Fields.column.Type" ),
              ColumnInfo.COLUMN_TYPE_TEXT, false, false ), new ColumnInfo( BaseMessages.getString( PKG,
                  "ParquetOutputDialog.Fields.column.Indexed" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ), };
    parameterColumns[1].setUsingVariables( true );

    wOutputFields =
        new TableView( transMeta, wComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, parameterColumns, 3, lsMod,
            props );
    props.setLook( wOutputFields );
    new FD( wOutputFields ).left( 0, 0 ).right( 100, 0 ).top( wIgnore, 10 ).bottom( wGetFields, -10 ).apply();

    wOutputFields.setRowNums();
    wOutputFields.optWidth( true );

    new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply();
    wComp.pack();

    Rectangle bounds = wComp.getBounds();
    wSComp.setContent( wComp );
    wSComp.setExpandHorizontal( true );
    wSComp.setExpandVertical( true );
    wSComp.setMinWidth( bounds.width );
    wSComp.setMinHeight( bounds.height );

    wTab.setControl( wSComp );

  }
  
  private void addOptionsTab( CTabFolder wTabFolder ) {
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Options.TabTitle" ) );

    ScrolledComposite wSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wSComp.setLayout( new FillLayout() );

    Composite wComp = new Composite( wSComp, SWT.NONE );
    props.setLook( wComp );

    FormLayout layout = new FormLayout();
    layout.marginWidth = 15;
    layout.marginHeight = 15;
    wComp.setLayout( layout );
    
    wlWriterVersion = new Label( wComp, SWT.LEFT );
    props.setLook( wlWriterVersion );
    wlWriterVersion.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.WriterVersion.Label" ) );
    new FD( wlWriterVersion ).left( 0, 0 ).right( 50, 0 ).top( 0, 0 ).apply();
    
    wWriterVersion = new CCombo( wComp, SWT.BORDER );
    props.setLook( wWriterVersion );
    wWriterVersion.add( "Parquet 1.0" );
    new FD( wWriterVersion ).left( 0, 0 ).right( 50, 0 ).top( wlWriterVersion, 5 ).apply();
    
    wlRowGroupSize = new Label( wComp, SWT.LEFT );
    props.setLook( wlRowGroupSize );
    wlRowGroupSize.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.RowGroupSize.Label" ) );
    new FD( wlRowGroupSize ).left( 0, 0 ).right( 50, 0 ).top( wWriterVersion, 5 ).apply();

    wRowGroupSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wRowGroupSize );
    new FD( wRowGroupSize ).left( 0, 0 ).top( wlRowGroupSize, 5 ).right( 100, 0 ).apply();
    
    wlPageSize = new Label( wComp, SWT.LEFT );
    props.setLook( wlPageSize );
    wlPageSize.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.PageSize.Label" ) );
    new FD( wlPageSize ).left( 0, 0 ).right( 50, 0 ).top( wRowGroupSize, 5 ).apply();

    wPageSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPageSize );
    new FD( wPageSize ).left( 0, 0 ).top( wlPageSize, 5 ).right( 100, 0 ).apply();
    
    wlEncoding = new Label( wComp, SWT.LEFT );
    props.setLook( wlEncoding );
    wlEncoding.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.Encoding.Label" ) );
    new FD( wlEncoding ).left( 0, 0 ).right( 50, 0 ).top( wPageSize, 5 ).apply();
    
    wEncoding = new CCombo( wComp, SWT.BORDER );
    props.setLook( wEncoding );
    wEncoding.add( "Plain" );
    new FD( wEncoding ).left( 0, 0 ).right( 50, 0 ).top( wlEncoding, 5 ).apply();
    
    wlDictionaryPageSize = new Label( wComp, SWT.LEFT );
    props.setLook( wlDictionaryPageSize );
    wlDictionaryPageSize.setText( BaseMessages.getString( PKG, "ParquetOutputDialog.DictionaryPageSize.Label" ) );
    new FD( wlDictionaryPageSize ).left( 0, 0 ).right( 50, 0 ).top( wEncoding, 5 ).apply();

    wDictionaryPageSize = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDictionaryPageSize );
    new FD( wDictionaryPageSize ).left( 0, 0 ).top( wlDictionaryPageSize, 5 ).right( 100, 0 ).apply();

    new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply();
    wComp.pack();
    
    Rectangle bounds = wComp.getBounds();
    wSComp.setContent( wComp );
    wSComp.setExpandHorizontal( true );
    wSComp.setExpandVertical( true );
    wSComp.setMinWidth( bounds.width );
    wSComp.setMinHeight( bounds.height );

    wTab.setControl( wSComp );
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), "JOBEx.svg", ConstUI.ICON_SIZE,
        ConstUI.ICON_SIZE );
  }

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  protected void getData( ParquetOutputMetaBase meta ) {
    if ( meta.getFilename() != null ) {
      wPath.setText( meta.getFilename() );
    }

    int nrFields = meta.getOutputFields().size();
    for ( int i = 0; i < nrFields; i++ ) {
      FormatInputField outputField = meta.getOutputFields().get( i );
      TableItem item = wOutputFields.table.getItem( i );
      if ( outputField.getName() != null ) {
        item.setText( 1, outputField.getName() );
      }
      item.setText( 3, outputField.getTypeDesc() );
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

  /**
   * Fill meta object from UI options.
   */
  protected void getInfo( ParquetOutputMetaBase meta, boolean preview ) {
    meta.setFilename( wPath.getText() );

    int nrFields = wOutputFields.nrNonEmpty();

    List<FormatInputField> outputFields = new ArrayList<>();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wOutputFields.getNonEmpty( i );

      FormatInputField outputField = new FormatInputField();
      outputField.setName( item.getText( 1 ) );
      outputField.setType( item.getText( 3 ) );

      outputFields.add( outputField );
    }
    meta.setOutputFields( outputFields );
  }

  /**
   * Class for apply layout settings to SWT controls.
   */
  public static class FD {
    private final Control control;
    private final FormData fd;

    public FD( Control control ) {
      this.control = control;
      fd = new FormData();
    }

    public FD width( int width ) {
      fd.width = width;
      return this;
    }

    public FD height( int height ) {
      fd.height = height;
      return this;
    }

    public FD top( int numerator, int offset ) {
      fd.top = new FormAttachment( numerator, offset );
      return this;
    }

    public FD top( Control control, int offset ) {
      fd.top = new FormAttachment( control, offset );
      return this;
    }

    public FD bottom( int numerator, int offset ) {
      fd.bottom = new FormAttachment( numerator, offset );
      return this;
    }

    public FD bottom( Control control, int offset ) {
      fd.bottom = new FormAttachment( control, offset );
      return this;
    }

    public FD left( int numerator, int offset ) {
      fd.left = new FormAttachment( numerator, offset );
      return this;
    }

    public FD right( int numerator, int offset ) {
      fd.right = new FormAttachment( numerator, offset );
      return this;
    }

    public FD right( Control control, int offset ) {
      fd.right = new FormAttachment( control, offset );
      return this;
    }

    public void apply() {
      control.setLayoutData( fd );
    }
  }

}
