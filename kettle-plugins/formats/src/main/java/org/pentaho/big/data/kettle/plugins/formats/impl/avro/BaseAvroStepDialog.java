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

package org.pentaho.big.data.kettle.plugins.formats.impl.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.provider.UriParser;
import org.eclipse.jface.window.DefaultToolTip;
import org.eclipse.jface.window.ToolTip;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseTrackAdapter;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.events.VerifyEvent;
import org.eclipse.swt.events.VerifyListener;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowData;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.kettle.plugins.formats.avro.input.AvroInputMetaBase;
import org.pentaho.big.data.kettle.plugins.formats.impl.avro.input.AvroInputMeta;
import org.pentaho.big.data.kettle.plugins.formats.impl.parquet.input.VFSScheme;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public abstract class BaseAvroStepDialog<T extends BaseStepMeta & StepMetaInterface> extends BaseStepDialog
  implements StepDialogInterface {
  protected final Class<?> PKG = getClass();
  protected final Class<?> BPKG = BaseAvroStepDialog.class;

  protected T meta;
  protected ModifyListener lsMod;

  public static final int MARGIN = 15;
  public static final int FIELDS_SEP = 10;
  public static final int FIELD_LABEL_SEP = 5;

  public static final int FIELD_SMALL = 150;
  public static final int FIELD_MEDIUM = 250;
  public static final int FIELD_LARGE = 350;

  private static final String ELLIPSIS = "...";
  private static final int TABLE_ITEM_MARGIN = 2;
  private static final int TOOLTIP_SHOW_DELAY = 350;
  private static final int TOOLTIP_HIDE_DELAY = 2000;
  private static final String DEFAULT_LOCAL_PATH = "file:///C:/";
  // width of the icon in a varfield
  protected static final int VAR_EXTRA_WIDTH = GUIResource.getInstance().getImageVariable().getBounds().width;

  protected static final String[] FILES_FILTERS = { "*.*" };
  protected static final String[] fileFilterNames =
    new String[] { BaseMessages.getString( "System.FileType.AllFiles" ) };

  protected Image icon;

  protected TextVar wPath;
  protected Button wbBrowse;
  protected Button wbGetFileFromField;
  protected ComboVar wFieldNameCombo;
  protected VFSScheme selectedVFSScheme;
  protected CCombo wLocation;
  protected boolean isInputStep;
  private Map<String, Integer> incomingFields = new HashMap<String, Integer>();

  private static final String HDFS_SCHEME = "hdfs";

  public BaseAvroStepDialog( Shell parent, T in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    meta = in;
    if ( meta instanceof AvroInputMeta ) {
      isInputStep = true;
    }
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        meta.setChanged();
      }
    };
    changed = meta.hasChanged();

    createUI();

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    int height = Math.max( getMinHeight( shell, getWidth() ), getHeight() );
    shell.setMinimumSize( getWidth(), height );
    shell.setSize( getWidth(), height );
    getData( meta );
    updateLocation();
    shell.open();
    wStepname.setFocus();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected void createUI() {
    Control prev = createHeader();

    //main fields
    if ( !isInputStep ) {
      prev = addFileWidgets( shell, prev );
    }

    createFooter( shell );

    Composite afterFile = new Composite( shell, SWT.NONE );
    afterFile.setLayout( new FormLayout() );
    Label separator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.bottom = new FormAttachment( wCancel, -MARGIN );
    fdSpacer.right = new FormAttachment( 100, 0 );
    separator.setLayoutData( fdSpacer );

    new FD( afterFile ).left( 0, 0 ).top( prev, 0 ).right( 100, 0 ).bottom( separator, -MARGIN ).apply();

    createAfterFile( afterFile );
  }

  protected Control createFooter( Composite shell ) {

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( getMsg( "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, lsCancel );
    new FD( wCancel ).right( 100, 0 ).bottom( 100, 0 ).apply();

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( getMsg( "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, lsOK );
    new FD( wOK ).right( wCancel, -FIELD_LABEL_SEP ).bottom( 100, 0 ).apply();
    lsPreview = getPreview();
    if ( lsPreview != null ) {
      wPreview = new Button( shell, SWT.PUSH );
      wPreview.setText( getBaseMsg( "BaseStepDialog.Preview" ) );
      wPreview.pack();
      wPreview.addListener( SWT.Selection, lsPreview );
      int offset = wPreview.getBounds().width / 2;
      new FD( wPreview ).left( 50, -offset ).bottom( 100, 0 ).apply();
    }
    return wCancel;
  }

  protected void cancel() {
    stepname = null;
    meta.setChanged( changed );
    dispose();
  }

  protected void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText();

    getInfo( meta, false );
    dispose();
  }

  protected abstract Control createAfterFile( Composite container );

  protected abstract String getStepTitle();


  /**
   * Read the data from the meta object and show it in this dialog.
   *
   * @param meta The meta object to obtain the data from.
   */
  protected abstract void getData( T meta );

  /**
   * Fill meta object from UI options.
   *
   * @param meta    meta object
   * @param preview flag for preview or real options should be used. Currently, only one option is differ for preview
   *               - EOL
   *                chars. It uses as "mixed" for be able to preview any file.
   */
  protected abstract void getInfo( T meta, boolean preview );

  protected abstract int getWidth();

  protected abstract int getHeight();

  protected abstract Listener getPreview();

  protected Label createHeader() {
    // main form
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    shell.setLayout( formLayout );
    // title
    shell.setText( getStepTitle() );
    // buttons
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

    // Stepname label
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( getBaseMsg( "BaseStepDialog.StepName" ) );
    props.setLook( wlStepname );
    new FD( wlStepname ).left( 0, 0 ).top( 0, 0 ).apply();
    // Stepname field
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    new FD( wStepname ).left( 0, 0 ).top( wlStepname, FIELD_LABEL_SEP ).width( FIELD_MEDIUM ).rright().apply();

    Label separator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    separator.setLayoutData( fdSpacer );

    addIcon( separator );
    return separator;
  }

  protected void addIcon( Control bottom ) {
    String stepId = meta.getParentStepMeta().getStepID();
    icon = GUIResource.getInstance().getImagesSteps().get( stepId ).getAsBitmapForSize( shell.getDisplay(),
      ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    Composite iconCont = new Composite( shell, SWT.NONE );
    new FD( iconCont ).top( 0, 0 ).right( 100, 0 ).bottom( bottom, -MARGIN ).width( ConstUI.ICON_SIZE ).apply();
    RowLayout iconLayout = new RowLayout( SWT.VERTICAL );
    iconLayout.pack = true;
    iconLayout.justify = true;
    iconLayout.marginHeight = iconLayout.marginWidth = 0;
    iconCont.setLayout( iconLayout );
    Label wIcon = new Label( iconCont, SWT.RIGHT );
    wIcon.setImage( icon );
    wIcon.setLayoutData( new RowData( ConstUI.ICON_SIZE, ConstUI.ICON_SIZE ) );
    props.setLook( wIcon );
  }

  protected Control addFileWidgets( Composite parent, Control prev ) {
    Label wlLocation = new Label( parent, SWT.RIGHT );
    wlLocation.setText( getBaseMsg( "AvroDialog.Location.Label" ) );
    props.setLook( wlLocation );
    new FD( wlLocation ).left( 0, 0 ).top( prev, MARGIN ).apply();
    wLocation = new CCombo( parent, SWT.BORDER | SWT.READ_ONLY );
    try {
      List<VFSScheme> availableVFSSchemes = getAvailableVFSSchemes();
      availableVFSSchemes.forEach( scheme -> wLocation.add( scheme.getSchemeName() ) );
      wLocation.addListener( SWT.Selection, event -> {
        this.selectedVFSScheme = availableVFSSchemes.get( wLocation.getSelectionIndex() );
        this.wPath.setText( "" );
      } );
      if ( !availableVFSSchemes.isEmpty() ) {
        wLocation.select( 0 );
        this.selectedVFSScheme = availableVFSSchemes.get( wLocation.getSelectionIndex() );
      }
    } catch ( KettleFileException ex ) {
      log.logError( getBaseMsg( "AvroDialog.FileBrowser.KettleFileException" ) );
    } catch ( FileSystemException ex ) {
      log.logError( getBaseMsg( "AvroDialog.FileBrowser.FileSystemException" ) );
    }
    wLocation.addModifyListener( lsMod );
    new FD( wLocation ).left( 0, 0 ).top( wlLocation, FIELD_LABEL_SEP ).width( FIELD_SMALL ).apply();

    Label wlPath = new Label( parent, SWT.RIGHT );
    wlPath.setText( getBaseMsg( "AvroDialog.Filename.Label" ) );
    props.setLook( wlPath );
    new FD( wlPath ).left( 0, 0 ).top( wLocation, FIELDS_SEP ).apply();
    wPath = new TextVar( transMeta, parent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPath.addModifyListener( event -> {
      if ( wPreview != null ) {
        wPreview.setEnabled( !Utils.isEmpty( wPath.getText() ) );
      }
    } );
    props.setLook( wPath );
    wPath.addModifyListener( lsMod );
    new FD( wPath ).left( 0, 0 ).top( wlPath, FIELD_LABEL_SEP ).width( FIELD_LARGE + VAR_EXTRA_WIDTH ).rright().apply();

    wbBrowse = new Button( parent, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( getMsg( "System.Button.Browse" ) );
    wbBrowse.addListener( SWT.Selection, event -> browseForFileInputPath() );
    int bOffset = ( wbBrowse.computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y
      - wPath.computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y ) / 2;
    new FD( wbBrowse ).left( wPath, FIELD_LABEL_SEP ).top( wlPath, FIELD_LABEL_SEP - bOffset ).apply();
    return wPath;
  }

  protected void browseForFileInputPath() {
    try {
      String path = transMeta.environmentSubstitute( wPath.getText() );
      VfsFileChooserDialog fileChooserDialog;
      String fileName;
      if ( Utils.isEmpty( path ) ) {
        fileChooserDialog = getVfsFileChooserDialog( null, null );
        fileName = selectedVFSScheme.getScheme() + "://";
      } else {
        FileObject initialFile = getInitialFile( wPath.getText() );
        FileObject rootFile = initialFile.getFileSystem().getRoot();
        fileChooserDialog = getVfsFileChooserDialog( rootFile, initialFile );
        fileName = null;
      }

      FileObject selectedFile =
        fileChooserDialog.open( shell, null, selectedVFSScheme.getScheme(), true, fileName, FILES_FILTERS,
          fileFilterNames, true, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, true, true );
      if ( selectedFile != null ) {
        String filePath = selectedFile.getURL().toString();
        if ( !DEFAULT_LOCAL_PATH.equals( filePath ) ) {
          wPath.setText( filePath );
          updateLocation();
        }
      }
    } catch ( KettleFileException ex ) {
      log.logError( getBaseMsg( "AvroInputDialog.FileBrowser.KettleFileException" ) );
    } catch ( FileSystemException ex ) {
      log.logError( getBaseMsg( "AvroInputDialog.FileBrowser.FileSystemException" ) );
    }
  }

  private void updateLocation() {
    String pathText = wPath.getText();
    String scheme = pathText.isEmpty() ? HDFS_SCHEME : UriParser.extractScheme( pathText );
    if ( scheme != null ) {
      try {
        List<VFSScheme> availableVFSSchemes = getAvailableVFSSchemes();
        for ( int i = 0; i < availableVFSSchemes.size(); i++ ) {
          VFSScheme s = availableVFSSchemes.get( i );
          if ( scheme.equals( s.getScheme() ) ) {
            wLocation.select( i );
            selectedVFSScheme = s;
          }
        }
      } catch ( KettleFileException ex ) {
        log.logError( getBaseMsg( "AvroInputDialog.FileBrowser.KettleFileException" ) );
      } catch ( FileSystemException ex ) {
        log.logError( getBaseMsg( "AvroInputDialog.FileBrowser.FileSystemException" ) );
      }
    }
    // do we have preview button?
    if ( wPreview != null ) {
      //update preview button
      wPreview.setEnabled( !pathText.isEmpty() );
    }
  }

  protected String getBaseMsg( String key ) {
    return BaseMessages.getString( BPKG, key );
  }

  protected String getMsg( String key ) {
    return BaseMessages.getString( PKG, key );
  }

  /**
   * Class for apply layout settings to SWT controls.
   */
  protected class FD {
    private final Control control;
    private final FormData fd;

    public FD( Control control ) {
      this.control = control;
      props.setLook( control );
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

    public FD left( int numerator ) {
      return left( numerator, 0 );
    }

    public FD left( Control control, int offset ) {
      fd.left = new FormAttachment( control, offset );
      return this;
    }

    public FD right( int numerator, int offset ) {
      fd.right = new FormAttachment( numerator, offset );
      return this;
    }

    public FD rright() {
      fd.right = new FormAttachment( 100, -getControlOffset( control, fd.width ) );
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

  protected FileObject getInitialFile( String filePath ) throws KettleFileException {
    FileObject initialFile = null;
    if ( filePath != null && !filePath.isEmpty() ) {
      String fileName = transMeta.environmentSubstitute( filePath );
      if ( fileName != null && !fileName.isEmpty() ) {
        initialFile = KettleVFS.getFileObject( fileName );
      }
    }
    if ( initialFile == null ) {
      initialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
    }
    return initialFile;
  }

  protected List<VFSScheme> getAvailableVFSSchemes() throws KettleFileException, FileSystemException {
    VfsFileChooserDialog fileChooserDialog = getVfsFileChooserDialog( null, null );
    List<CustomVfsUiPanel> customVfsUiPanels = fileChooserDialog.getCustomVfsUiPanels();
    List<VFSScheme> vfsSchemes = new ArrayList<>();
    customVfsUiPanels.forEach( vfsPanel -> {
      VFSScheme scheme = new VFSScheme( vfsPanel.getVfsScheme(), vfsPanel.getVfsSchemeDisplayText() );
      vfsSchemes.add( scheme );
    } );
    return vfsSchemes;
  }

  protected VfsFileChooserDialog getVfsFileChooserDialog( FileObject rootFile, FileObject initialFile )
    throws KettleFileException, FileSystemException {
    return getSpoon().getVfsFileChooserDialog( rootFile, initialFile );
  }

  private Spoon getSpoon() {
    return Spoon.getInstance();
  }

  protected int getMinHeight( Composite comp, int minWidth ) {
    comp.pack();
    return comp.computeSize( minWidth, SWT.DEFAULT ).y;
  }

  protected void setTruncatedColumn( Table table, int targetColumn ) {
    table.addListener( SWT.EraseItem, new Listener() {
      public void handleEvent( Event event ) {
        if ( event.index == targetColumn ) {
          event.detail &= ~SWT.FOREGROUND;
        }
      }
    } );
    table.addListener( SWT.PaintItem, new Listener() {
      @Override
      public void handleEvent( Event event ) {
        TableItem item = (TableItem) event.item;
        int colIdx = event.index;
        if ( colIdx == targetColumn ) {
          String contents = item.getText( colIdx );
          if ( Utils.isEmpty( contents ) ) {
            return;
          }
          Point size = event.gc.textExtent( contents );
          int targetWidth = item.getBounds( colIdx ).width;
          int yOffset = Math.max( 0, ( event.height - size.y ) / 2 );
          if ( size.x > targetWidth ) {
            contents = shortenText( event.gc, contents, targetWidth );
          }
          event.gc.drawText( contents, event.x + TABLE_ITEM_MARGIN, event.y + yOffset, true );
        }
      }
    } );
  }


  protected void addColumnTooltip( Table table, int columnIndex ) {
    final DefaultToolTip toolTip = new DefaultToolTip( table, ToolTip.RECREATE, true );
    toolTip.setRespectMonitorBounds( true );
    toolTip.setRespectDisplayBounds( true );
    toolTip.setPopupDelay( TOOLTIP_SHOW_DELAY );
    toolTip.setHideDelay( TOOLTIP_HIDE_DELAY );
    toolTip.setShift( new Point( ConstUI.TOOLTIP_OFFSET, ConstUI.TOOLTIP_OFFSET ) );
    table.addMouseTrackListener( new MouseTrackAdapter() {
      @Override
      public void mouseHover( MouseEvent e ) {
        Point coord = new Point( e.x, e.y );
        TableItem item = table.getItem( coord );
        if ( item != null ) {
          if ( item.getBounds( columnIndex ).contains( coord ) ) {
            String contents = item.getText( columnIndex );
            if ( !Utils.isEmpty( contents ) ) {
              toolTip.setText( contents );
              toolTip.show( coord );
              return;
            }
          }
        }
        toolTip.hide();
      }

      @Override
      public void mouseExit( MouseEvent e ) {
        toolTip.hide();
      }
    } );
  }

  protected String shortenText( GC gc, String text, final int targetWidth ) {
    if ( Utils.isEmpty( text ) ) {
      return "";
    }
    int textWidth = gc.textExtent( text ).x;
    int extra = gc.textExtent( ELLIPSIS ).x + 2 * TABLE_ITEM_MARGIN;
    if ( targetWidth <= extra || textWidth <= targetWidth ) {
      return text;
    }
    int len = text.length();
    for ( int chomp = 1; chomp < len && textWidth + extra >= targetWidth; chomp++ ) {
      text = text.substring( 0, text.length() - 1 );
      textWidth = gc.textExtent( text ).x;
    }
    return text + ELLIPSIS;
  }

  private int getControlOffset( Control control, int controlWidth ) {
    // remaining space for min size match
    return getWidth() - getMarginWidths( control ) - controlWidth;
  }

  private int getMarginWidths( Control control ) {
    // get the width added by container margins and (wm-specific) decorations
    int extraWidth = 0;
    for ( Composite parent = control.getParent(); !parent.equals( getParent() ); parent = parent.getParent() ) {
      extraWidth += parent.computeTrim( 0, 0, 0, 0 ).width;
      if ( parent.getLayout() instanceof FormLayout ) {
        extraWidth += 2 * ( (FormLayout) parent.getLayout() ).marginWidth;
      }
    }
    return extraWidth;
  }

  protected void setIntegerOnly( TextVar textVar ) {
    textVar.getTextWidget().addVerifyListener( new VerifyListener() {
      @Override
      public void verifyText( VerifyEvent e ) {
        if ( !StringUtil.isEmpty( e.text ) && !StringUtil.isVariable( e.text ) && !StringUtil.IsInteger( e.text ) ) {
          e.doit = false;
        }
      }
    } );
  }

  /**
   * Used only for AvroInput at the moment but here because we anticipate using for output as well at a later date.
   * There is no MetaBase for avro so there is cast in this method to pull the step data into the ui.
   *
   * @param wTabFolder
   */
  protected void addFileTab( CTabFolder wTabFolder ) {
    AvroInputMetaBase avroBaseMeta = (AvroInputMetaBase) meta;

    // Create & Set up a new Tab Item
    CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
    wTab.setText( getBaseMsg( "AvroDialog.File.TabTitle" ) );
    Composite wTabComposite = new Composite( wTabFolder, SWT.NONE );
    wTab.setControl( wTabComposite );
    props.setLook( wTabComposite );
    FormLayout formLayout = new FormLayout();
    formLayout.marginHeight = MARGIN;
    wTabComposite.setLayout( formLayout );

    // Set up the File settings Group
    Group wFileSettingsGroup = new Group( wTabComposite, SWT.SHADOW_NONE );
    props.setLook( wFileSettingsGroup );
    wFileSettingsGroup.setText( getBaseMsg( "AvroDialog.File.FileSettingsTitle" ) );

    FormLayout layout = new FormLayout();
    layout.marginHeight = MARGIN;
    layout.marginWidth = MARGIN;
    wFileSettingsGroup.setLayout( layout );
    new FD( wFileSettingsGroup ).top( 0, 0 ).right( 100, -MARGIN ).left( 0, MARGIN ).apply();

    int RADIO_BUTTON_WIDTH = 150;
    Label separator = new Label( wFileSettingsGroup, SWT.SEPARATOR | SWT.VERTICAL );
    props.setLook( separator );
    new FD( separator ).left( 0, RADIO_BUTTON_WIDTH ).top( 0, 0 ).bottom( 100, 0 ).apply();

    Button wbSpecifyFileName = new Button( wFileSettingsGroup, SWT.RADIO );
    wbSpecifyFileName.setText( getBaseMsg( "AvroDialog.File.SpecifyFileName" ) );
    props.setLook( wbSpecifyFileName );
    new FD( wbSpecifyFileName ).left( 0, 0 ).top( 0, 0 ).width( RADIO_BUTTON_WIDTH ).apply();

    wbGetFileFromField = new Button( wFileSettingsGroup, SWT.RADIO );
    wbGetFileFromField.setText( getBaseMsg( "AvroDialog.File.GetDataFromField" ) );
    props.setLook( wbGetFileFromField );
    new FD( wbGetFileFromField ).left( 0, 0 ).top( wbSpecifyFileName, FIELDS_SEP ).width( RADIO_BUTTON_WIDTH ).apply();

    //Make a composite to hold the dynamic right side of the group
    Composite wFileSettingsDynamicArea = new Composite( wFileSettingsGroup, SWT.NONE );
    props.setLook( wFileSettingsDynamicArea );
    FormLayout fileSettingsDynamicAreaLayout = new FormLayout();
    wFileSettingsDynamicArea.setLayout( fileSettingsDynamicAreaLayout );
    new FD( wFileSettingsDynamicArea ).right( 100, 0 ).left( wbSpecifyFileName, 10 ).top( 0, -MARGIN ).apply();

    //Put the File selection stuff in it
    Composite wFileSetting = new Composite( wFileSettingsDynamicArea, SWT.NONE );
    FormLayout fileSettingLayout = new FormLayout();
    wFileSetting.setLayout( fileSettingLayout );
    new FD( wFileSetting ).left( 0, 0 ).right( 100, RADIO_BUTTON_WIDTH + MARGIN ).top( 0, 0 ).apply();
    addFileWidgets( wFileSetting, wFileSetting );

    //Setup StreamingFieldName
    Composite wCompStreamFieldName = new Composite( wFileSettingsDynamicArea, SWT.NONE );
    props.setLook( wCompStreamFieldName );
    FormLayout fieldNameLayout = new FormLayout();
    fieldNameLayout.marginHeight = MARGIN;
    wCompStreamFieldName.setLayout( fieldNameLayout );
    new FD( wCompStreamFieldName ).left( 0, 0 ).top( 0, 0 ).apply();

    Label fieldNameLabel = new Label( wCompStreamFieldName, SWT.NONE );
    fieldNameLabel.setText( getBaseMsg( "AvroDialog.FieldName.Label" ) );
    props.setLook( fieldNameLabel );
    new FD( fieldNameLabel ).left( 0, 0 ).top( wCompStreamFieldName, 0 ).apply();
    wFieldNameCombo = new ComboVar( transMeta, wCompStreamFieldName, SWT.LEFT | SWT.BORDER );
    updateIncomingFieldList( wFieldNameCombo );
    new FD( wFieldNameCombo ).left( 0, 0 ).top( fieldNameLabel, FIELD_LABEL_SEP ).width( FIELD_SMALL + VAR_EXTRA_WIDTH )
      .apply();

    //Setup the radio button event handler
    SelectionAdapter fileSettingRadioSelectionAdapter = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        wFileSetting.setVisible( !wbGetFileFromField.getSelection() );
        wCompStreamFieldName.setVisible( wbGetFileFromField.getSelection() );
      }
    };
    wbSpecifyFileName.addSelectionListener( fileSettingRadioSelectionAdapter );
    wbGetFileFromField.addSelectionListener( fileSettingRadioSelectionAdapter );

    //Set widgets from Meta
    wbSpecifyFileName.setSelection( !avroBaseMeta.isUseFieldAsInputStream() );
    wbGetFileFromField.setSelection( avroBaseMeta.isUseFieldAsInputStream() );
    fileSettingRadioSelectionAdapter.widgetSelected( null );
    wFieldNameCombo
      .setText( avroBaseMeta.getInputStreamFieldName() == null ? "" : avroBaseMeta.getInputStreamFieldName() );

  }

  private void updateIncomingFieldList( ComboVar comboVar ) {
    // Search the fields in the background
    StepMeta stepMeta = transMeta.findStep( stepname );
    if ( stepMeta != null ) {
      try {
        RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );
        incomingFields.clear();
        // Remember these fields...
        for ( int i = 0; i < row.size(); i++ ) {
          incomingFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
        }
        final Map<String, Integer> fields = new HashMap<String, Integer>();

        // Add the currentMeta fields...
        fields.putAll( incomingFields );

        Set<String> keySet = fields.keySet();
        List<String> entries = new ArrayList<String>( keySet );

        String[] fieldNames = entries.toArray( new String[ entries.size() ] );

        Const.sortStrings( fieldNames );
        comboVar.setItems( fieldNames );
      } catch ( KettleException e ) {
        logError( getBaseMsg( "System.Dialog.GetFieldsFailed.Message" ) );
      }
    }
  }

}
