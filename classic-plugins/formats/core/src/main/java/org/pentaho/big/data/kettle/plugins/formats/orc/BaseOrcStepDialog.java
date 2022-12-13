/*! ******************************************************************************
  *
  * Pentaho Data Integration
  *
  * Copyright (C) 2018-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.orc;

import org.eclipse.jface.window.DefaultToolTip;
import org.eclipse.jface.window.ToolTip;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseTrackAdapter;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialogTextVar;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.events.dialog.SelectionOperation;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public abstract class BaseOrcStepDialog<T extends BaseStepMeta & StepMetaInterface> extends BaseStepDialog
  implements StepDialogInterface {
  protected final Class<?> PKG = getClass();
  protected static final Class<?> BPKG = BaseOrcStepDialog.class;

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
  // width of the icon in a varfield
  protected static final int VAR_EXTRA_WIDTH = GUIResource.getInstance().getImageVariable().getBounds().width;

  protected TextVar wPath;
  protected Button wbBrowse;

  public BaseOrcStepDialog( Shell parent, T in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    meta = in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );

    lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();

    createUI();

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    int height = Math.max( getMinHeight( shell, getWidth() ), getHeight() );
    shell.setMinimumSize( getWidth(), height );
    shell.setSize( getWidth(), height );
    getData( meta );
    shell.open();
    wStepname.setFocus();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  protected abstract void createUI();

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
   * @param preview flag for preview or real options should be used. Currently, only one option is differ for preview - EOL
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
    lsOK = e -> ok();
    lsCancel = e -> cancel();

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

    // separator
    Label separator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wStepname, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    separator.setLayoutData( fdSpacer );

    addIcon();
    return separator;
  }

  protected void addIcon() {
    Label wicon = new Label( shell, SWT.RIGHT );
    String stepId = meta.getParentStepMeta().getStepID();
    wicon.setImage( GUIResource.getInstance().getImagesSteps().get( stepId ).getAsBitmapForSize( shell.getDisplay(),
        ConstUI.LARGE_ICON_SIZE, ConstUI.LARGE_ICON_SIZE ) );
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment( 0, 0 );
    fdlicon.right = new FormAttachment( 100, 0 );
    wicon.setLayoutData( fdlicon );
    props.setLook( wicon );
  }

  protected Control addFileWidgets( Control prev ) {
    Label wlPath = new Label( shell, SWT.RIGHT );
    wlPath.setText( getBaseMsg( "OrcDialog.Filename.Label" ) );
    props.setLook( wlPath );
    new FD( wlPath ).left( 0, 0 ).top( prev, MARGIN ).apply();
    wPath = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPath.addModifyListener( event -> {
      if ( wPreview != null ) {
        wPreview.setEnabled( !Utils.isEmpty( wPath.getText() ) );
      }
    } );
    props.setLook( wPath );
    wPath.addModifyListener( lsMod );
    new FD( wPath ).left( 0, 0 ).top( wlPath, FIELD_LABEL_SEP ).width( FIELD_LARGE + VAR_EXTRA_WIDTH ).rright().apply();


    wbBrowse = new Button( shell, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( getMsg( "System.Button.Browse" ) );
    wbBrowse.addSelectionListener( new SelectionAdapterFileDialogTextVar(
      log, wPath, transMeta, new SelectionAdapterOptions( selectionOperation() ) ) );
    int bOffset = ( wbBrowse.computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y
      - wPath.computeSize( SWT.DEFAULT, SWT.DEFAULT, false ).y ) / 2;
    new FD( wbBrowse ).left( wPath, FIELD_LABEL_SEP ).top( wlPath, FIELD_LABEL_SEP - bOffset ).apply();
    return wPath;
  }

  protected abstract SelectionOperation selectionOperation();

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

  protected int getMinHeight( Composite comp, int minWidth ) {
    comp.pack();
    return comp.computeSize( minWidth, SWT.DEFAULT ).y;
  }

  protected void setTruncatedColumn( Table table, int targetColumn ) {
    table.addListener( SWT.EraseItem, event -> {
      if ( event.index == targetColumn ) {
        event.detail &= ~SWT.FOREGROUND;
      }
    } );
    table.addListener( SWT.PaintItem, event -> {
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
        if ( item != null && item.getBounds( columnIndex ).contains( coord ) ) {
          String contents = item.getText( columnIndex );
          if ( !Utils.isEmpty( contents ) ) {
            toolTip.setText( contents );
            toolTip.show( coord );
            return;
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
    textVar.getTextWidget().addVerifyListener( e -> {
      if ( !StringUtil.isEmpty( e.text ) && !StringUtil.isVariable( e.text ) && !StringUtil.IsInteger( e.text ) ) {
        e.doit = false;
      }
    } );
  }

}
