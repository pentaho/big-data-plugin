/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.trans;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.eclipse.jface.window.Window;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
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
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.pentaho.di.core.annotations.PluginDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.HadoopVfsFileChooserDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.Schemes;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.compress.CompressionProvider;
import org.pentaho.di.core.compress.CompressionProviderFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.fileinput.FileInputList;
import org.pentaho.di.core.gui.TextFileInputFieldInterface;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.file.BaseFileField;
import org.pentaho.di.trans.steps.fileinput.text.BufferedInputStreamReader;
import org.pentaho.di.trans.steps.fileinput.text.EncodingType;
import org.pentaho.di.trans.steps.fileinput.text.TextFileFilter;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInputMeta;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInputUtils;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.core.widget.VariableButtonListenerFactory;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.ui.trans.steps.fileinput.text.TextFileCSVImportProgressDialog;
import org.pentaho.di.ui.trans.steps.fileinput.text.TextFileImportWizardPage1;
import org.pentaho.di.ui.trans.steps.fileinput.text.TextFileImportWizardPage2;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;

@PluginDialog( id = "HadoopFileInputPlugin", image = "HDI.svg", pluginType = PluginDialog.PluginType.STEP,
  documentationUrl = "Products/Hadoop_File_Input" )
public class HadoopFileInputDialog extends BaseStepDialog implements StepDialogInterface {
  private static final Class<?> BASE_PKG = TextFileInputMeta.class; // for i18n purposes, needed by Translator2!!
  private static final Class<?> PKG = HadoopFileInputMeta.class; // for i18n purposes, needed by Translator2!!
  private static final String[] ALL_FILES_TYPE = new String[] {
    BaseMessages.getString( PKG, "System.FileType.AllFiles" ) };

  private LogChannel log = new LogChannel( this );

  private static final String COMBO_NO = BaseMessages.getString( BASE_PKG, "System.Combo.No" );
  private static final String COMBO_YES = BaseMessages.getString( BASE_PKG, "System.Combo.Yes" );
  private static final String[] YES_NO_COMBO = new String[] { COMBO_NO, COMBO_YES };

  public static final String LOCAL_ENVIRONMENT = "Local";
  public static final String STATIC_ENVIRONMENT = "<Static>";
  public static final String S3_ENVIRONMENT = "S3";

  public static final String BUTTON_BROWSE = BaseMessages.getString( BASE_PKG, "System.Button.Browse" );
  public static final String BUTTON_VARIABLE = BaseMessages.getString( BASE_PKG, "System.Button.Variable" );
  public static final String ERROR_TITLE = BaseMessages.getString( BASE_PKG, "System.Dialog.Error.Title" );
  public static final String TOOLTIP_VARIABLE = BaseMessages.getString( BASE_PKG, "System.Tooltip.VariableToDir" );
  public static final String LABEL_EXTENSION = BaseMessages.getString( BASE_PKG, "System.Label.Extension" );

  private CTabFolder wTabFolder;

  private Button wAccFilenames;

  private Label wlPassThruFields;
  private Button wPassThruFields;

  private Label wlAccField;
  private Text wAccField;

  private Label wlAccStep;
  private CCombo wAccStep;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Button wbShowFiles;

  private Button wFirst;

  private Button wFirstHeader;

  private CCombo wFiletype;

  private Button wbSeparator;
  private TextVar wSeparator;

  private Text wEnclosure;

  private Text wEscape;

  private Button wHeader;

  private Label wlNrHeader;
  private Text wNrHeader;

  private Button wFooter;

  private Label wlNrFooter;
  private Text wNrFooter;

  private Button wWraps;

  private Label wlNrWraps;
  private Text wNrWraps;

  private Button wLayoutPaged;

  private Label wlNrLinesPerPage;
  private Text wNrLinesPerPage;

  private Label wlNrLinesDocHeader;
  private Text wNrLinesDocHeader;

  private CCombo wCompression;

  private Button wNoempty;

  private Button wInclFilename;

  private Label wlInclFilenameField;
  private Text wInclFilenameField;

  private Button wInclRownum;

  private Label wlRownumByFileField;
  private Button wRownumByFile;

  private Label wlInclRownumField;
  private Text wInclRownumField;

  private CCombo wFormat;

  private CCombo wEncoding;

  private Text wLimit;

  private Button wDateLenient;

  private CCombo wDateLocale;

  // ERROR HANDLING...
  private Button wErrorIgnored;

  private Label wlSkipErrorLines;
  private Button wSkipErrorLines;

  private Label wlErrorCount;
  private Text wErrorCount;

  private Label wlErrorFields;
  private Text wErrorFields;

  private Label wlErrorText;
  private Text wErrorText;

  // New entries for intelligent error handling AKA replay functionality
  // Bad files destination directory
  private Label wlWarnDestDir;
  private Button wbbWarnDestDir; // Browse: add file or directory
  private Button wbvWarnDestDir; // Variable
  private Text wWarnDestDir;
  private Label wlWarnExt;
  private Text wWarnExt;

  // Error messages files destination directory
  private Label wlErrorDestDir;
  private Button wbbErrorDestDir; // Browse: add file or directory
  private Button wbvErrorDestDir; // Variable
  private Text wErrorDestDir;
  private Label wlErrorExt;
  private Text wErrorExt;

  // Line numbers files destination directory
  private Label wlLineNrDestDir;
  private Button wbbLineNrDestDir; // Browse: add file or directory
  private Button wbvLineNrDestDir; // Variable
  private Text wLineNrDestDir;
  private Label wlLineNrExt;
  private Text wLineNrExt;

  private TableView wFilter;

  private TableView wFields;

  private Button wAddResult;

  private HadoopFileInputMeta input;

  // Wizard info...
  private Vector<TextFileInputFieldInterface> fields;

  private int middle;
  private int margin;
  private ModifyListener lsMod;

  public static final int[] dateLengths = new int[] { 23, 19, 14, 10, 10, 10, 10, 8, 8, 8, 8, 6, 6 };

  private boolean gotEncodings = false;

  protected boolean firstClickOnDateLocale;

  private final NamedClusterService namedClusterService;

  public HadoopFileInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (HadoopFileInputMeta) in;
    namedClusterService = input.getNamedClusterService();
    input.setVariableSpace( variables );
    firstClickOnDateLocale = true;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = e -> input.setChanged();

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "HadoopFileInputDialog.DialogTitle" ) );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( BASE_PKG, "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.top = new FormAttachment( 0, margin );
    fdlStepname.right = new FormAttachment( middle, -margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );

    addFilesTab();
    addContentTab();
    addErrorTab();
    addFiltersTabs();
    addFieldsTabs();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wStepname, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( BASE_PKG, "System.Button.OK" ) );

    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Preview.Button" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( BASE_PKG, "System.Button.Cancel" ) );

    positionBottomRightButtons( shell, new Button[] { wOK, wPreview, wCancel }, margin, wTabFolder );

    // Add listeners
    lsOK = e -> ok();
    Listener lsFirst = e -> first( false );
    Listener lsFirstHeader = e -> first( true );
    lsGet = e -> get();
    lsPreview = e -> preview();
    lsCancel = e -> cancel();

    wOK.addListener( SWT.Selection, lsOK );
    wFirst.addListener( SWT.Selection, lsFirst );
    wFirstHeader.addListener( SWT.Selection, lsFirstHeader );
    wGet.addListener( SWT.Selection, lsGet );
    wPreview.addListener( SWT.Selection, lsPreview );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wAccFilenames.addSelectionListener( lsDef );
    wStepname.addSelectionListener( lsDef );
    wSeparator.addSelectionListener( lsDef );
    wLimit.addSelectionListener( lsDef );
    wInclRownumField.addSelectionListener( lsDef );
    wInclFilenameField.addSelectionListener( lsDef );
    wNrHeader.addSelectionListener( lsDef );
    wNrFooter.addSelectionListener( lsDef );
    wNrWraps.addSelectionListener( lsDef );
    wWarnDestDir.addSelectionListener( lsDef );
    wWarnExt.addSelectionListener( lsDef );
    wErrorDestDir.addSelectionListener( lsDef );
    wErrorExt.addSelectionListener( lsDef );
    wLineNrDestDir.addSelectionListener( lsDef );
    wLineNrExt.addSelectionListener( lsDef );
    wAccField.addSelectionListener( lsDef );

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        showFiles();
      }
    } );

    // Allow the insertion of tabs as separator...
    wbSeparator.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent se ) {
        wSeparator.getTextWidget().insert( "\t" );
      }
    } );

    SelectionAdapter lsFlags = new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        setFlags();
      }
    };

    // Enable/disable the right fields...
    wInclFilename.addSelectionListener( lsFlags );
    wInclRownum.addSelectionListener( lsFlags );
    wRownumByFile.addSelectionListener( lsFlags );
    wErrorIgnored.addSelectionListener( lsFlags );
    wHeader.addSelectionListener( lsFlags );
    wFooter.addSelectionListener( lsFlags );
    wWraps.addSelectionListener( lsFlags );
    wLayoutPaged.addSelectionListener( lsFlags );
    wAccFilenames.addSelectionListener( lsFlags );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    getData( input );

    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Replaces the password present in each file URI with '***' before displaying it in the UI.
   *
   * @param files List of files to be processed
   * @return The list of files to be processed with the password replaced with '***'
   */
  protected String[] getFriendlyURIs( String[] files ) {
    for ( int i = 0; i < files.length; i++ ) {
      String userinfo = URI.create( files[ i ] ).getUserInfo();

      if ( userinfo != null ) {
        String[] credentials = userinfo.split( ":", 2 );

        if ( credentials.length == 2 ) {
          credentials[ 1 ] = "***";
          files[ i ] = files[ i ].replaceFirst( userinfo, String.join( ":", credentials ) );
        }
      }
    }

    return files;
  }

  private void showFiles() {
    HadoopFileInputMeta tfii = new HadoopFileInputMeta();
    getInfo( tfii );
    String[] files = tfii.getFilePaths( transMeta.getBowl(), transMeta );
    if ( files != null && files.length > 0 ) {
      EnterSelectionDialog esd =
        new EnterSelectionDialog( shell, getFriendlyURIs( files ), "Files read", "Files read:" );
      esd.setViewOnly();
      esd.open();
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NoFilesFound.DialogMessage" ) );
      mb.setText( ERROR_TITLE );
      mb.open();
    }
  }

  private void addFilesTab() {
    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////

    CTabItem wFileTab = new CTabItem( wTabFolder, SWT.NONE );
    wFileTab.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FileTab.TabTitle" ) );

    ScrolledComposite wFileSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wFileSComp.setLayout( new FillLayout() );

    Composite wFileComp = new Composite( wFileSComp, SWT.NONE );
    props.setLook( wFileComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout( fileLayout );

    // Filename list line
    wlFilenameList = new Label( wFileComp, SWT.RIGHT );
    wlFilenameList.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FilenameList.Label" ) );
    props.setLook( wlFilenameList );

    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment( 0, 0 );
    fdlFilenameList.top = new FormAttachment( wFileComp, 15 );
    wlFilenameList.setLayoutData( fdlFilenameList );

    ToolBar tb = new ToolBar( wFileComp, SWT.HORIZONTAL | SWT.FLAT );
    props.setLook( tb );
    FormData fdTb = new FormData();
    fdTb.right = new FormAttachment( 100, 0 );
    fdTb.top = new FormAttachment( wFileComp, margin );
    tb.setLayoutData( fdTb );

    ToolItem deleteToolItem = new ToolItem( tb, SWT.PUSH );
    deleteToolItem.setImage( GUIResource.getInstance().getImageDelete() );
    deleteToolItem
      .setToolTipText( BaseMessages.getString( JobEntryCopyFiles.class, "JobCopyFiles.FilenameDelete.Tooltip" ) );
    deleteToolItem.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {

        int[] idx = wFilenameList.getSelectionIndices();
        wFilenameList.remove( idx );
        wFilenameList.removeEmptyRows();
        wFilenameList.setRowNums();
      }
    } );

    wbShowFiles = new Button( wFileComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbShowFiles );
    wbShowFiles.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ShowFiles.Button" ) );

    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment( middle, 0 );
    fdbShowFiles.bottom = new FormAttachment( 100, 0 );
    wbShowFiles.setLayoutData( fdbShowFiles );

    wFirst = new Button( wFileComp, SWT.PUSH );
    wFirst.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.First.Button" ) );

    FormData fdFirst = new FormData();
    fdFirst.left = new FormAttachment( wbShowFiles, margin * 2 );
    fdFirst.bottom = new FormAttachment( 100, 0 );
    wFirst.setLayoutData( fdFirst );

    wFirstHeader = new Button( wFileComp, SWT.PUSH );
    wFirstHeader.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FirstHeader.Button" ) );

    FormData fdFirstHeader = new FormData();
    fdFirstHeader.left = new FormAttachment( wFirst, margin * 2 );
    fdFirstHeader.bottom = new FormAttachment( 100, 0 );
    wFirstHeader.setLayoutData( fdFirstHeader );

    // Accepting filenames group
    //

    Group gAccepting = new Group( wFileComp, SWT.SHADOW_ETCHED_IN );
    gAccepting.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptingGroup.Label" ) );
    FormLayout acceptingLayout = new FormLayout();
    acceptingLayout.marginWidth = 3;
    acceptingLayout.marginHeight = 3;
    gAccepting.setLayout( acceptingLayout );
    props.setLook( gAccepting );

    // Accept filenames from previous steps?
    //
    Label wlAccFilenames = new Label( gAccepting, SWT.RIGHT );
    wlAccFilenames.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptFilenames.Label" ) );
    props.setLook( wlAccFilenames );

    FormData fdlAccFilenames = new FormData();
    fdlAccFilenames.top = new FormAttachment( 0, margin );
    fdlAccFilenames.left = new FormAttachment( 0, 0 );
    fdlAccFilenames.right = new FormAttachment( middle, -margin );
    wlAccFilenames.setLayoutData( fdlAccFilenames );
    wAccFilenames = new Button( gAccepting, SWT.CHECK );
    wAccFilenames.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptFilenames.Tooltip" ) );
    props.setLook( wAccFilenames );

    FormData fdAccFilenames = new FormData();
    fdAccFilenames.top = new FormAttachment( 0, margin );
    fdAccFilenames.left = new FormAttachment( middle, 0 );
    fdAccFilenames.right = new FormAttachment( 100, 0 );
    wAccFilenames.setLayoutData( fdAccFilenames );

    // Accept filenames from previous steps?
    //
    wlPassThruFields = new Label( gAccepting, SWT.RIGHT );
    wlPassThruFields.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.PassThruFields.Label" ) );
    props.setLook( wlPassThruFields );

    FormData fdlPassThruFields = new FormData();
    fdlPassThruFields.top = new FormAttachment( wAccFilenames, margin );
    fdlPassThruFields.left = new FormAttachment( 0, 0 );
    fdlPassThruFields.right = new FormAttachment( middle, -margin );
    wlPassThruFields.setLayoutData( fdlPassThruFields );
    wPassThruFields = new Button( gAccepting, SWT.CHECK );
    wPassThruFields.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.PassThruFields.Tooltip" ) );
    props.setLook( wPassThruFields );

    FormData fdPassThruFields = new FormData();
    fdPassThruFields.top = new FormAttachment( wAccFilenames, margin );
    fdPassThruFields.left = new FormAttachment( middle, 0 );
    fdPassThruFields.right = new FormAttachment( 100, 0 );
    wPassThruFields.setLayoutData( fdPassThruFields );

    // Which step to read from?
    wlAccStep = new Label( gAccepting, SWT.RIGHT );
    wlAccStep.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptStep.Label" ) );
    props.setLook( wlAccStep );

    FormData fdlAccStep = new FormData();
    fdlAccStep.top = new FormAttachment( wPassThruFields, margin );
    fdlAccStep.left = new FormAttachment( 0, 0 );
    fdlAccStep.right = new FormAttachment( middle, -margin );
    wlAccStep.setLayoutData( fdlAccStep );
    wAccStep = new CCombo( gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wAccStep.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptStep.Tooltip" ) );
    props.setLook( wAccStep );

    FormData fdAccStep = new FormData();
    fdAccStep.top = new FormAttachment( wPassThruFields, margin );
    fdAccStep.left = new FormAttachment( middle, 0 );
    fdAccStep.right = new FormAttachment( 100, 0 );
    wAccStep.setLayoutData( fdAccStep );

    // Which field?
    //
    wlAccField = new Label( gAccepting, SWT.RIGHT );
    wlAccField.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptField.Label" ) );
    props.setLook( wlAccField );

    FormData fdlAccField = new FormData();
    fdlAccField.top = new FormAttachment( wAccStep, margin );
    fdlAccField.left = new FormAttachment( 0, 0 );
    fdlAccField.right = new FormAttachment( middle, -margin );
    wlAccField.setLayoutData( fdlAccField );
    wAccField = new Text( gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wAccField.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AcceptField.Tooltip" ) );
    props.setLook( wAccField );

    FormData fdAccField = new FormData();
    fdAccField.top = new FormAttachment( wAccStep, margin );
    fdAccField.left = new FormAttachment( middle, 0 );
    fdAccField.right = new FormAttachment( 100, 0 );
    wAccField.setLayoutData( fdAccField );

    // Fill in the source steps...
    List<StepMeta> prevSteps = transMeta.findPreviousSteps( transMeta.findStep( stepname ) );
    for ( StepMeta prevStep : prevSteps ) {
      wAccStep.add( prevStep.getName() );
    }

    FormData fdAccepting = new FormData();
    fdAccepting.left = new FormAttachment( 0, 0 );
    fdAccepting.right = new FormAttachment( 100, 0 );
    fdAccepting.bottom = new FormAttachment( wFirstHeader, -margin * 2 );
    gAccepting.setLayoutData( fdAccepting );

    ColumnInfo[] colinfo =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( PKG, "HadoopFileInputDialog.Environment" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, false, true ),
        new ColumnInfo( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileFolderColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT_BUTTON, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.WildcardColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RequiredColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.IncludeSubDirs.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ) };

    setComboValues( colinfo[ 0 ] );
    colinfo[ 1 ].setUsingVariables( true );
    colinfo[ 1 ].setTextVarButtonSelectionListener( getFileDirectoryListener() );
    colinfo[ 2 ].setToolTip( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RegExpColumn.Column" ) );
    colinfo[ 3 ].setToolTip( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RequiredColumn.Tooltip" ) );
    colinfo[ 4 ].setToolTip( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.IncludeSubDirs.Tooltip" ) );

    wFilenameList =
      new TableView( transMeta, wFileComp, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, colinfo, 4, lsMod, props );
    props.setLook( wFilenameList );

    FormData fdFilenameList = new FormData();
    fdFilenameList.bottom = new FormAttachment( gAccepting, 0 );
    fdFilenameList.right = new FormAttachment( 100, 0 );
    fdFilenameList.left = new FormAttachment( 0, 0 );
    fdFilenameList.top = new FormAttachment( tb, margin );
    wFilenameList.setLayoutData( fdFilenameList );

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment( 0, 0 );
    fdFileComp.top = new FormAttachment( 0, 0 );
    fdFileComp.right = new FormAttachment( 100, 0 );
    fdFileComp.bottom = new FormAttachment( 100, 0 );
    wFileComp.setLayoutData( fdFileComp );

    wFileComp.pack();
    Rectangle bounds = wFileComp.getBounds();

    wFileSComp.setContent( wFileComp );
    wFileSComp.setExpandHorizontal( true );
    wFileSComp.setExpandVertical( true );
    wFileSComp.setMinWidth( bounds.width );
    wFileSComp.setMinHeight( bounds.height );

    wFileTab.setControl( wFileSComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addContentTab() {
    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem( wTabFolder, SWT.NONE );
    wContentTab.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ContentTab.TabTitle" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    ScrolledComposite wContentSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wContentSComp.setLayout( new FillLayout() );

    Composite wContentComp = new Composite( wContentSComp, SWT.NONE );
    props.setLook( wContentComp );
    wContentComp.setLayout( contentLayout );

    // Filetype line
    Label wlFiletype = new Label( wContentComp, SWT.RIGHT );
    wlFiletype.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Filetype.Label" ) );
    props.setLook( wlFiletype );

    FormData fdlFiletype = new FormData();
    fdlFiletype.left = new FormAttachment( 0, 0 );
    fdlFiletype.top = new FormAttachment( 0, 0 );
    fdlFiletype.right = new FormAttachment( middle, -margin );
    wlFiletype.setLayoutData( fdlFiletype );
    wFiletype = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wFiletype.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Filetype.Label" ) );
    props.setLook( wFiletype );
    wFiletype.add( "CSV" );
    wFiletype.add( "Fixed" );
    wFiletype.select( 0 );
    wFiletype.addModifyListener( lsMod );

    FormData fdFiletype = new FormData();
    fdFiletype.left = new FormAttachment( middle, 0 );
    fdFiletype.top = new FormAttachment( 0, 0 );
    fdFiletype.right = new FormAttachment( 100, 0 );
    wFiletype.setLayoutData( fdFiletype );

    Label wlSeparator = new Label( wContentComp, SWT.RIGHT );
    wlSeparator.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Separator.Label" ) );
    props.setLook( wlSeparator );

    FormData fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment( 0, 0 );
    fdlSeparator.top = new FormAttachment( wFiletype, margin );
    fdlSeparator.right = new FormAttachment( middle, -margin );
    wlSeparator.setLayoutData( fdlSeparator );

    wbSeparator = new Button( wContentComp, SWT.PUSH | SWT.CENTER );
    wbSeparator.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Delimiter.Button" ) );
    props.setLook( wbSeparator );

    FormData fdbSeparator = new FormData();
    fdbSeparator.right = new FormAttachment( 100, 0 );
    fdbSeparator.top = new FormAttachment( wFiletype, 0 );
    wbSeparator.setLayoutData( fdbSeparator );
    wSeparator = new TextVar( transMeta, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeparator );
    wSeparator.addModifyListener( lsMod );

    FormData fdSeparator = new FormData();
    fdSeparator.top = new FormAttachment( wFiletype, margin );
    fdSeparator.left = new FormAttachment( middle, 0 );
    fdSeparator.right = new FormAttachment( wbSeparator, -margin );
    wSeparator.setLayoutData( fdSeparator );

    // Enclosure
    Label wlEnclosure = new Label( wContentComp, SWT.RIGHT );
    wlEnclosure.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Enclosure.Label" ) );
    props.setLook( wlEnclosure );

    FormData fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment( 0, 0 );
    fdlEnclosure.top = new FormAttachment( wSeparator, margin );
    fdlEnclosure.right = new FormAttachment( middle, -margin );
    wlEnclosure.setLayoutData( fdlEnclosure );
    wEnclosure = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEnclosure );
    wEnclosure.addModifyListener( lsMod );

    FormData fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment( middle, 0 );
    fdEnclosure.top = new FormAttachment( wSeparator, margin );
    fdEnclosure.right = new FormAttachment( 100, 0 );
    wEnclosure.setLayoutData( fdEnclosure );

    // Allow Enclosure breaks checkbox
    Label wlEnclBreaks = new Label( wContentComp, SWT.RIGHT );
    wlEnclBreaks.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.EnclBreaks.Label" ) );
    props.setLook( wlEnclBreaks );

    FormData fdlEnclBreaks = new FormData();
    fdlEnclBreaks.left = new FormAttachment( 0, 0 );
    fdlEnclBreaks.top = new FormAttachment( wEnclosure, margin );
    fdlEnclBreaks.right = new FormAttachment( middle, -margin );
    wlEnclBreaks.setLayoutData( fdlEnclBreaks );

    Button wEnclBreaks = new Button( wContentComp, SWT.CHECK );
    props.setLook( wEnclBreaks );

    FormData fdEnclBreaks = new FormData();
    fdEnclBreaks.left = new FormAttachment( middle, 0 );
    fdEnclBreaks.top = new FormAttachment( wEnclosure, margin );
    wEnclBreaks.setLayoutData( fdEnclBreaks );

    // Disable until the logic works...
    wlEnclBreaks.setEnabled( false );
    wEnclBreaks.setEnabled( false );

    // Escape
    Label wlEscape = new Label( wContentComp, SWT.RIGHT );
    wlEscape.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Escape.Label" ) );
    props.setLook( wlEscape );

    FormData fdlEscape = new FormData();
    fdlEscape.left = new FormAttachment( 0, 0 );
    fdlEscape.top = new FormAttachment( wEnclBreaks, margin );
    fdlEscape.right = new FormAttachment( middle, -margin );
    wlEscape.setLayoutData( fdlEscape );
    wEscape = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wEscape );
    wEscape.addModifyListener( lsMod );

    FormData fdEscape = new FormData();
    fdEscape.left = new FormAttachment( middle, 0 );
    fdEscape.top = new FormAttachment( wEnclBreaks, margin );
    fdEscape.right = new FormAttachment( 100, 0 );
    wEscape.setLayoutData( fdEscape );

    // Header checkbox
    Label wlHeader = new Label( wContentComp, SWT.RIGHT );
    wlHeader.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Header.Label" ) );
    props.setLook( wlHeader );

    FormData fdlHeader = new FormData();
    fdlHeader.left = new FormAttachment( 0, 0 );
    fdlHeader.top = new FormAttachment( wEscape, margin );
    fdlHeader.right = new FormAttachment( middle, -margin );
    wlHeader.setLayoutData( fdlHeader );
    wHeader = new Button( wContentComp, SWT.CHECK );
    props.setLook( wHeader );

    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment( middle, 0 );
    fdHeader.top = new FormAttachment( wEscape, margin );
    wHeader.setLayoutData( fdHeader );

    // NrHeader
    wlNrHeader = new Label( wContentComp, SWT.RIGHT );
    wlNrHeader.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NrHeader.Label" ) );
    props.setLook( wlNrHeader );

    FormData fdlNrHeader = new FormData();
    fdlNrHeader.left = new FormAttachment( wHeader, margin );
    fdlNrHeader.top = new FormAttachment( wEscape, margin );
    wlNrHeader.setLayoutData( fdlNrHeader );
    wNrHeader = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNrHeader.setTextLimit( 3 );
    props.setLook( wNrHeader );
    wNrHeader.addModifyListener( lsMod );

    FormData fdNrHeader = new FormData();
    fdNrHeader.left = new FormAttachment( wlNrHeader, margin );
    fdNrHeader.top = new FormAttachment( wEscape, margin );
    fdNrHeader.right = new FormAttachment( 100, 0 );
    wNrHeader.setLayoutData( fdNrHeader );

    Label wlFooter = new Label( wContentComp, SWT.RIGHT );
    wlFooter.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Footer.Label" ) );
    props.setLook( wlFooter );

    FormData fdlFooter = new FormData();
    fdlFooter.left = new FormAttachment( 0, 0 );
    fdlFooter.top = new FormAttachment( wHeader, margin );
    fdlFooter.right = new FormAttachment( middle, -margin );
    wlFooter.setLayoutData( fdlFooter );
    wFooter = new Button( wContentComp, SWT.CHECK );
    props.setLook( wFooter );

    FormData fdFooter = new FormData();
    fdFooter.left = new FormAttachment( middle, 0 );
    fdFooter.top = new FormAttachment( wHeader, margin );
    wFooter.setLayoutData( fdFooter );

    // NrFooter
    wlNrFooter = new Label( wContentComp, SWT.RIGHT );
    wlNrFooter.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NrFooter.Label" ) );
    props.setLook( wlNrFooter );

    FormData fdlNrFooter = new FormData();
    fdlNrFooter.left = new FormAttachment( wFooter, margin );
    fdlNrFooter.top = new FormAttachment( wHeader, margin );
    wlNrFooter.setLayoutData( fdlNrFooter );
    wNrFooter = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNrFooter.setTextLimit( 3 );
    props.setLook( wNrFooter );
    wNrFooter.addModifyListener( lsMod );

    FormData fdNrFooter = new FormData();
    fdNrFooter.left = new FormAttachment( wlNrFooter, margin );
    fdNrFooter.top = new FormAttachment( wHeader, margin );
    fdNrFooter.right = new FormAttachment( 100, 0 );
    wNrFooter.setLayoutData( fdNrFooter );

    // Wraps
    Label wlWraps = new Label( wContentComp, SWT.RIGHT );
    wlWraps.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Wraps.Label" ) );
    props.setLook( wlWraps );

    FormData fdlWraps = new FormData();
    fdlWraps.left = new FormAttachment( 0, 0 );
    fdlWraps.top = new FormAttachment( wFooter, margin );
    fdlWraps.right = new FormAttachment( middle, -margin );
    wlWraps.setLayoutData( fdlWraps );
    wWraps = new Button( wContentComp, SWT.CHECK );
    props.setLook( wWraps );

    FormData fdWraps = new FormData();
    fdWraps.left = new FormAttachment( middle, 0 );
    fdWraps.top = new FormAttachment( wFooter, margin );
    wWraps.setLayoutData( fdWraps );

    // NrWraps
    wlNrWraps = new Label( wContentComp, SWT.RIGHT );
    wlNrWraps.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NrWraps.Label" ) );
    props.setLook( wlNrWraps );

    FormData fdlNrWraps = new FormData();
    fdlNrWraps.left = new FormAttachment( wWraps, margin );
    fdlNrWraps.top = new FormAttachment( wFooter, margin );
    wlNrWraps.setLayoutData( fdlNrWraps );
    wNrWraps = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNrWraps.setTextLimit( 3 );
    props.setLook( wNrWraps );
    wNrWraps.addModifyListener( lsMod );

    FormData fdNrWraps = new FormData();
    fdNrWraps.left = new FormAttachment( wlNrWraps, margin );
    fdNrWraps.top = new FormAttachment( wFooter, margin );
    fdNrWraps.right = new FormAttachment( 100, 0 );
    wNrWraps.setLayoutData( fdNrWraps );

    // Pages
    Label wlLayoutPaged = new Label( wContentComp, SWT.RIGHT );
    wlLayoutPaged.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LayoutPaged.Label" ) );
    props.setLook( wlLayoutPaged );

    FormData fdlLayoutPaged = new FormData();
    fdlLayoutPaged.left = new FormAttachment( 0, 0 );
    fdlLayoutPaged.top = new FormAttachment( wWraps, margin );
    fdlLayoutPaged.right = new FormAttachment( middle, -margin );
    wlLayoutPaged.setLayoutData( fdlLayoutPaged );
    wLayoutPaged = new Button( wContentComp, SWT.CHECK );
    props.setLook( wLayoutPaged );

    FormData fdLayoutPaged = new FormData();
    fdLayoutPaged.left = new FormAttachment( middle, 0 );
    fdLayoutPaged.top = new FormAttachment( wWraps, margin );
    wLayoutPaged.setLayoutData( fdLayoutPaged );

    // Nr of lines per page
    wlNrLinesPerPage = new Label( wContentComp, SWT.RIGHT );
    wlNrLinesPerPage.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NrLinesPerPage.Label" ) );
    props.setLook( wlNrLinesPerPage );

    FormData fdlNrLinesPerPage = new FormData();
    fdlNrLinesPerPage.left = new FormAttachment( wLayoutPaged, margin );
    fdlNrLinesPerPage.top = new FormAttachment( wWraps, margin );
    wlNrLinesPerPage.setLayoutData( fdlNrLinesPerPage );
    wNrLinesPerPage = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNrLinesPerPage.setTextLimit( 3 );
    props.setLook( wNrLinesPerPage );
    wNrLinesPerPage.addModifyListener( lsMod );

    FormData fdNrLinesPerPage = new FormData();
    fdNrLinesPerPage.left = new FormAttachment( wlNrLinesPerPage, margin );
    fdNrLinesPerPage.top = new FormAttachment( wWraps, margin );
    fdNrLinesPerPage.right = new FormAttachment( 100, 0 );
    wNrLinesPerPage.setLayoutData( fdNrLinesPerPage );

    // NrPages
    wlNrLinesDocHeader = new Label( wContentComp, SWT.RIGHT );
    wlNrLinesDocHeader.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NrLinesDocHeader.Label" ) );
    props.setLook( wlNrLinesDocHeader );

    FormData fdlNrLinesDocHeader = new FormData();
    fdlNrLinesDocHeader.left = new FormAttachment( wLayoutPaged, margin );
    fdlNrLinesDocHeader.top = new FormAttachment( wNrLinesPerPage, margin );
    wlNrLinesDocHeader.setLayoutData( fdlNrLinesDocHeader );
    wNrLinesDocHeader = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wNrLinesDocHeader.setTextLimit( 3 );
    props.setLook( wNrLinesDocHeader );
    wNrLinesDocHeader.addModifyListener( lsMod );

    FormData fdNrLinesDocHeader = new FormData();
    fdNrLinesDocHeader.left = new FormAttachment( wlNrLinesPerPage, margin );
    fdNrLinesDocHeader.top = new FormAttachment( wNrLinesPerPage, margin );
    fdNrLinesDocHeader.right = new FormAttachment( 100, 0 );
    wNrLinesDocHeader.setLayoutData( fdNrLinesDocHeader );

    // Compression type (None, Zip or GZip
    Label wlCompression = new Label( wContentComp, SWT.RIGHT );
    wlCompression.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Compression.Label" ) );
    props.setLook( wlCompression );

    FormData fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment( 0, 0 );
    fdlCompression.top = new FormAttachment( wNrLinesDocHeader, margin );
    fdlCompression.right = new FormAttachment( middle, -margin );
    wlCompression.setLayoutData( fdlCompression );
    wCompression = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wCompression.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Compression.Label" ) );
    wCompression.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Compression.Tooltip" ) );
    props.setLook( wCompression );
    wCompression.setItems( CompressionProviderFactory.getInstance().getCompressionProviderNames() );
    wCompression.addModifyListener( lsMod );

    FormData fdCompression = new FormData();
    fdCompression.left = new FormAttachment( middle, 0 );
    fdCompression.top = new FormAttachment( wNrLinesDocHeader, margin );
    fdCompression.right = new FormAttachment( 100, 0 );
    wCompression.setLayoutData( fdCompression );

    Label wlNoempty = new Label( wContentComp, SWT.RIGHT );
    wlNoempty.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NoEmpty.Label" ) );
    props.setLook( wlNoempty );

    FormData fdlNoempty = new FormData();
    fdlNoempty.left = new FormAttachment( 0, 0 );
    fdlNoempty.top = new FormAttachment( wCompression, margin );
    fdlNoempty.right = new FormAttachment( middle, -margin );
    wlNoempty.setLayoutData( fdlNoempty );
    wNoempty = new Button( wContentComp, SWT.CHECK );
    props.setLook( wNoempty );
    wNoempty.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NoEmpty.Tooltip" ) );

    FormData fdNoempty = new FormData();
    fdNoempty.left = new FormAttachment( middle, 0 );
    fdNoempty.top = new FormAttachment( wCompression, margin );
    fdNoempty.right = new FormAttachment( 100, 0 );
    wNoempty.setLayoutData( fdNoempty );

    Label wlInclFilename = new Label( wContentComp, SWT.RIGHT );
    wlInclFilename.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.InclFilename.Label" ) );
    props.setLook( wlInclFilename );

    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment( 0, 0 );
    fdlInclFilename.top = new FormAttachment( wNoempty, margin );
    fdlInclFilename.right = new FormAttachment( middle, -margin );
    wlInclFilename.setLayoutData( fdlInclFilename );
    wInclFilename = new Button( wContentComp, SWT.CHECK );
    props.setLook( wInclFilename );
    wInclFilename.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.InclFilename.Tooltip" ) );

    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment( middle, 0 );
    fdInclFilename.top = new FormAttachment( wNoempty, margin );
    wInclFilename.setLayoutData( fdInclFilename );

    wlInclFilenameField = new Label( wContentComp, SWT.LEFT );
    wlInclFilenameField.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.InclFilenameField.Label" ) );
    props.setLook( wlInclFilenameField );

    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment( wInclFilename, margin );
    fdlInclFilenameField.top = new FormAttachment( wNoempty, margin );
    wlInclFilenameField.setLayoutData( fdlInclFilenameField );
    wInclFilenameField = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclFilenameField );
    wInclFilenameField.addModifyListener( lsMod );

    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment( wlInclFilenameField, margin );
    fdInclFilenameField.top = new FormAttachment( wNoempty, margin );
    fdInclFilenameField.right = new FormAttachment( 100, 0 );
    wInclFilenameField.setLayoutData( fdInclFilenameField );

    Label wlInclRownum = new Label( wContentComp, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );

    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wInclFilenameField, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wContentComp, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.InclRownum.Tooltip" ) );

    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wInclFilenameField, margin );
    wInclRownum.setLayoutData( fdRownum );

    wlInclRownumField = new Label( wContentComp, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );

    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );

    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wInclFilenameField, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    wlRownumByFileField = new Label( wContentComp, SWT.RIGHT );
    wlRownumByFileField.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RownumByFile.Label" ) );
    props.setLook( wlRownumByFileField );

    FormData fdlRownumByFile = new FormData();
    fdlRownumByFile.left = new FormAttachment( wInclRownum, margin );
    fdlRownumByFile.top = new FormAttachment( wInclRownumField, margin );
    wlRownumByFileField.setLayoutData( fdlRownumByFile );
    wRownumByFile = new Button( wContentComp, SWT.CHECK );
    props.setLook( wRownumByFile );
    wRownumByFile.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RownumByFile.Tooltip" ) );

    FormData fdRownumByFile = new FormData();
    fdRownumByFile.left = new FormAttachment( wlRownumByFileField, margin );
    fdRownumByFile.top = new FormAttachment( wInclRownumField, margin );
    wRownumByFile.setLayoutData( fdRownumByFile );

    Label wlFormat = new Label( wContentComp, SWT.RIGHT );
    wlFormat.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Format.Label" ) );
    props.setLook( wlFormat );

    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment( 0, 0 );
    fdlFormat.top = new FormAttachment( wRownumByFile, margin * 2 );
    fdlFormat.right = new FormAttachment( middle, -margin );
    wlFormat.setLayoutData( fdlFormat );
    wFormat = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wFormat.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Format.Label" ) );
    props.setLook( wFormat );
    wFormat.add( "DOS" );
    wFormat.add( "Unix" );
    wFormat.add( "mixed" );
    wFormat.select( 0 );
    wFormat.addModifyListener( lsMod );

    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment( middle, 0 );
    fdFormat.top = new FormAttachment( wRownumByFile, margin * 2 );
    fdFormat.right = new FormAttachment( 100, 0 );
    wFormat.setLayoutData( fdFormat );

    Label wlEncoding = new Label( wContentComp, SWT.RIGHT );
    wlEncoding.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Encoding.Label" ) );
    props.setLook( wlEncoding );

    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment( 0, 0 );
    fdlEncoding.top = new FormAttachment( wFormat, margin );
    fdlEncoding.right = new FormAttachment( middle, -margin );
    wlEncoding.setLayoutData( fdlEncoding );
    wEncoding = new CCombo( wContentComp, SWT.BORDER | SWT.READ_ONLY );
    wEncoding.setEditable( true );
    props.setLook( wEncoding );
    wEncoding.addModifyListener( lsMod );

    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment( middle, 0 );
    fdEncoding.top = new FormAttachment( wFormat, margin );
    fdEncoding.right = new FormAttachment( 100, 0 );
    wEncoding.setLayoutData( fdEncoding );
    wEncoding.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
        // No-Op Necessary
      }

      @Override
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    Label wlLimit = new Label( wContentComp, SWT.RIGHT );
    wlLimit.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Limit.Label" ) );
    props.setLook( wlLimit );

    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment( 0, 0 );
    fdlLimit.top = new FormAttachment( wEncoding, margin );
    fdlLimit.right = new FormAttachment( middle, -margin );
    wlLimit.setLayoutData( fdlLimit );
    wLimit = new Text( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLimit );
    wLimit.addModifyListener( lsMod );

    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment( middle, 0 );
    fdLimit.top = new FormAttachment( wEncoding, margin );
    fdLimit.right = new FormAttachment( 100, 0 );
    wLimit.setLayoutData( fdLimit );

    // Date Lenient checkbox
    Label wlDateLenient = new Label( wContentComp, SWT.RIGHT );
    wlDateLenient.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.DateLenient.Label" ) );
    props.setLook( wlDateLenient );

    FormData fdlDateLenient = new FormData();
    fdlDateLenient.left = new FormAttachment( 0, 0 );
    fdlDateLenient.top = new FormAttachment( wLimit, margin );
    fdlDateLenient.right = new FormAttachment( middle, -margin );
    wlDateLenient.setLayoutData( fdlDateLenient );
    wDateLenient = new Button( wContentComp, SWT.CHECK );
    wDateLenient.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.DateLenient.Tooltip" ) );
    props.setLook( wDateLenient );

    FormData fdDateLenient = new FormData();
    fdDateLenient.left = new FormAttachment( middle, 0 );
    fdDateLenient.top = new FormAttachment( wLimit, margin );
    wDateLenient.setLayoutData( fdDateLenient );

    Label wlDateLocale = new Label( wContentComp, SWT.RIGHT );
    wlDateLocale.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.DateLocale.Label" ) );
    props.setLook( wlDateLocale );

    FormData fdlDateLocale = new FormData();
    fdlDateLocale.left = new FormAttachment( 0, 0 );
    fdlDateLocale.top = new FormAttachment( wDateLenient, margin );
    fdlDateLocale.right = new FormAttachment( middle, -margin );
    wlDateLocale.setLayoutData( fdlDateLocale );
    wDateLocale = new CCombo( wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDateLocale.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.DateLocale.Tooltip" ) );
    props.setLook( wDateLocale );
    wDateLocale.addModifyListener( lsMod );

    FormData fdDateLocale = new FormData();
    fdDateLocale.left = new FormAttachment( middle, 0 );
    fdDateLocale.top = new FormAttachment( wDateLenient, margin );
    fdDateLocale.right = new FormAttachment( 100, 0 );
    wDateLocale.setLayoutData( fdDateLocale );
    wDateLocale.addFocusListener( new FocusListener() {
      @Override
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
        // No-Op Necessary
      }

      @Override
      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setLocales();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group( wContentComp, SWT.SHADOW_NONE );
    props.setLook( wAddFileResult );
    wAddFileResult.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.wAddFileResult.Label" ) );

    FormLayout addFileResultgroupLayout = new FormLayout();
    addFileResultgroupLayout.marginWidth = 10;
    addFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout( addFileResultgroupLayout );

    Label wlAddResult = new Label( wAddFileResult, SWT.RIGHT );
    wlAddResult.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AddResult.Label" ) );
    props.setLook( wlAddResult );

    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment( 0, 0 );
    fdlAddResult.top = new FormAttachment( wDateLocale, margin );
    fdlAddResult.right = new FormAttachment( middle, -margin );
    wlAddResult.setLayoutData( fdlAddResult );
    wAddResult = new Button( wAddFileResult, SWT.CHECK );
    props.setLook( wAddResult );
    wAddResult.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.AddResult.Tooltip" ) );

    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment( middle, 0 );
    fdAddResult.top = new FormAttachment( wDateLocale, margin );
    wAddResult.setLayoutData( fdAddResult );

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment( 0, margin );
    fdAddFileResult.top = new FormAttachment( wDateLocale, margin );
    fdAddFileResult.right = new FormAttachment( 100, -margin );
    wAddFileResult.setLayoutData( fdAddFileResult );

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    wContentComp.pack();
    // What's the size:
    Rectangle bounds = wContentComp.getBounds();

    wContentSComp.setContent( wContentComp );
    wContentSComp.setExpandHorizontal( true );
    wContentSComp.setExpandVertical( true );
    wContentSComp.setMinWidth( bounds.width );
    wContentSComp.setMinHeight( bounds.height );

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment( 0, 0 );
    fdContentComp.top = new FormAttachment( 0, 0 );
    fdContentComp.right = new FormAttachment( 100, 0 );
    fdContentComp.bottom = new FormAttachment( 100, 0 );
    wContentComp.setLayoutData( fdContentComp );

    wContentTab.setControl( wContentSComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

  }

  protected void setLocales() {
    Locale[] locale = Locale.getAvailableLocales();
    String[] dateLocale = new String[ locale.length ];
    for ( int i = 0; i < locale.length; i++ ) {
      dateLocale[ i ] = locale[ i ].toString();
    }
    wDateLocale.setItems( dateLocale );
  }

  private void addErrorTab() {
    // ////////////////////////
    // START OF ERROR TAB ///
    // /
    CTabItem wErrorTab = new CTabItem( wTabFolder, SWT.NONE );
    wErrorTab.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorTab.TabTitle" ) );

    ScrolledComposite wErrorSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wErrorSComp.setLayout( new FillLayout() );

    FormLayout errorLayout = new FormLayout();
    errorLayout.marginWidth = 3;
    errorLayout.marginHeight = 3;

    Composite wErrorComp = new Composite( wErrorSComp, SWT.NONE );
    props.setLook( wErrorComp );
    wErrorComp.setLayout( errorLayout );

    // ERROR HANDLING...
    // ErrorIgnored?
    Label wlErrorIgnored = new Label( wErrorComp, SWT.RIGHT );
    wlErrorIgnored.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorIgnored.Label" ) );
    props.setLook( wlErrorIgnored );

    FormData fdlErrorIgnored = new FormData();
    fdlErrorIgnored.left = new FormAttachment( 0, 0 );
    fdlErrorIgnored.top = new FormAttachment( 0, margin );
    fdlErrorIgnored.right = new FormAttachment( middle, -margin );
    wlErrorIgnored.setLayoutData( fdlErrorIgnored );
    wErrorIgnored = new Button( wErrorComp, SWT.CHECK );
    props.setLook( wErrorIgnored );
    wErrorIgnored.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorIgnored.Tooltip" ) );

    FormData fdErrorIgnored = new FormData();
    fdErrorIgnored.left = new FormAttachment( middle, 0 );
    fdErrorIgnored.top = new FormAttachment( 0, margin );
    wErrorIgnored.setLayoutData( fdErrorIgnored );

    // Skip error lines?
    wlSkipErrorLines = new Label( wErrorComp, SWT.RIGHT );
    wlSkipErrorLines.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.SkipErrorLines.Label" ) );
    props.setLook( wlSkipErrorLines );

    FormData fdlSkipErrorLines = new FormData();
    fdlSkipErrorLines.left = new FormAttachment( 0, 0 );
    fdlSkipErrorLines.top = new FormAttachment( wErrorIgnored, margin );
    fdlSkipErrorLines.right = new FormAttachment( middle, -margin );
    wlSkipErrorLines.setLayoutData( fdlSkipErrorLines );
    wSkipErrorLines = new Button( wErrorComp, SWT.CHECK );
    props.setLook( wSkipErrorLines );
    wSkipErrorLines.setToolTipText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.SkipErrorLines.Tooltip" ) );

    FormData fdSkipErrorLines = new FormData();
    fdSkipErrorLines.left = new FormAttachment( middle, 0 );
    fdSkipErrorLines.top = new FormAttachment( wErrorIgnored, margin );
    wSkipErrorLines.setLayoutData( fdSkipErrorLines );

    wlErrorCount = new Label( wErrorComp, SWT.RIGHT );
    wlErrorCount.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorCount.Label" ) );
    props.setLook( wlErrorCount );

    FormData fdlErrorCount = new FormData();
    fdlErrorCount.left = new FormAttachment( 0, 0 );
    fdlErrorCount.top = new FormAttachment( wSkipErrorLines, margin );
    fdlErrorCount.right = new FormAttachment( middle, -margin );
    wlErrorCount.setLayoutData( fdlErrorCount );
    wErrorCount = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorCount );
    wErrorCount.addModifyListener( lsMod );

    FormData fdErrorCount = new FormData();
    fdErrorCount.left = new FormAttachment( middle, 0 );
    fdErrorCount.top = new FormAttachment( wSkipErrorLines, margin );
    fdErrorCount.right = new FormAttachment( 100, 0 );
    wErrorCount.setLayoutData( fdErrorCount );

    wlErrorFields = new Label( wErrorComp, SWT.RIGHT );
    wlErrorFields.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorFields.Label" ) );
    props.setLook( wlErrorFields );

    FormData fdlErrorFields = new FormData();
    fdlErrorFields.left = new FormAttachment( 0, 0 );
    fdlErrorFields.top = new FormAttachment( wErrorCount, margin );
    fdlErrorFields.right = new FormAttachment( middle, -margin );
    wlErrorFields.setLayoutData( fdlErrorFields );
    wErrorFields = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorFields );
    wErrorFields.addModifyListener( lsMod );

    FormData fdErrorFields = new FormData();
    fdErrorFields.left = new FormAttachment( middle, 0 );
    fdErrorFields.top = new FormAttachment( wErrorCount, margin );
    fdErrorFields.right = new FormAttachment( 100, 0 );
    wErrorFields.setLayoutData( fdErrorFields );

    wlErrorText = new Label( wErrorComp, SWT.RIGHT );
    wlErrorText.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorText.Label" ) );
    props.setLook( wlErrorText );

    FormData fdlErrorText = new FormData();
    fdlErrorText.left = new FormAttachment( 0, 0 );
    fdlErrorText.top = new FormAttachment( wErrorFields, margin );
    fdlErrorText.right = new FormAttachment( middle, -margin );
    wlErrorText.setLayoutData( fdlErrorText );
    wErrorText = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorText );
    wErrorText.addModifyListener( lsMod );

    FormData fdErrorText = new FormData();
    fdErrorText.left = new FormAttachment( middle, 0 );
    fdErrorText.top = new FormAttachment( wErrorFields, margin );
    fdErrorText.right = new FormAttachment( 100, 0 );
    wErrorText.setLayoutData( fdErrorText );

    // Bad lines files directory + extension
    Control previous = wErrorText;

    // BadDestDir line
    wlWarnDestDir = new Label( wErrorComp, SWT.RIGHT );
    wlWarnDestDir.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.WarnDestDir.Label" ) );
    props.setLook( wlWarnDestDir );

    FormData fdlWarnDestDir = new FormData();
    fdlWarnDestDir.left = new FormAttachment( 0, 0 );
    fdlWarnDestDir.top = new FormAttachment( previous, margin * 4 );
    fdlWarnDestDir.right = new FormAttachment( middle, -margin );
    wlWarnDestDir.setLayoutData( fdlWarnDestDir );

    wbbWarnDestDir = new Button( wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbWarnDestDir );
    wbbWarnDestDir.setText( BUTTON_BROWSE );
    wbbWarnDestDir.setToolTipText( BaseMessages.getString( BASE_PKG, "System.Tooltip.BrowseForDir" ) );

    FormData fdbBadDestDir = new FormData();
    fdbBadDestDir.right = new FormAttachment( 100, 0 );
    fdbBadDestDir.top = new FormAttachment( previous, margin * 4 );
    wbbWarnDestDir.setLayoutData( fdbBadDestDir );

    wbvWarnDestDir = new Button( wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbvWarnDestDir );
    wbvWarnDestDir.setText( BUTTON_VARIABLE );
    wbvWarnDestDir.setToolTipText( TOOLTIP_VARIABLE );

    FormData fdbvWarnDestDir = new FormData();
    fdbvWarnDestDir.right = new FormAttachment( wbbWarnDestDir, -margin );
    fdbvWarnDestDir.top = new FormAttachment( previous, margin * 4 );
    wbvWarnDestDir.setLayoutData( fdbvWarnDestDir );

    wWarnExt = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWarnExt );
    wWarnExt.addModifyListener( lsMod );

    FormData fdWarnDestExt = new FormData();
    fdWarnDestExt.left = new FormAttachment( wbvWarnDestDir, -150 );
    fdWarnDestExt.right = new FormAttachment( wbvWarnDestDir, -margin );
    fdWarnDestExt.top = new FormAttachment( previous, margin * 4 );
    wWarnExt.setLayoutData( fdWarnDestExt );

    wlWarnExt = new Label( wErrorComp, SWT.RIGHT );
    wlWarnExt.setText( LABEL_EXTENSION );
    props.setLook( wlWarnExt );

    FormData fdlWarnDestExt = new FormData();
    fdlWarnDestExt.top = new FormAttachment( previous, margin * 4 );
    fdlWarnDestExt.right = new FormAttachment( wWarnExt, -margin );
    wlWarnExt.setLayoutData( fdlWarnDestExt );

    wWarnDestDir = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWarnDestDir );
    wWarnDestDir.addModifyListener( lsMod );

    FormData fdBadDestDir = new FormData();
    fdBadDestDir.left = new FormAttachment( middle, 0 );
    fdBadDestDir.right = new FormAttachment( wlWarnExt, -margin );
    fdBadDestDir.top = new FormAttachment( previous, margin * 4 );
    wWarnDestDir.setLayoutData( fdBadDestDir );

    // Listen to the Browse... button
    wbbWarnDestDir.addSelectionListener( new DirectoryBrowserAdapter( wWarnDestDir ) );

    // Listen to the Variable... button
    wbvWarnDestDir.addSelectionListener( VariableButtonListenerFactory.getSelectionAdapter( shell, wWarnDestDir,
      transMeta ) );

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wWarnDestDir.addModifyListener( getModifyListenerTooltipText( wWarnDestDir ) );

    // Error lines files directory + extension
    previous = wWarnDestDir;

    // ErrorDestDir line
    wlErrorDestDir = new Label( wErrorComp, SWT.RIGHT );
    wlErrorDestDir.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ErrorDestDir.Label" ) );
    props.setLook( wlErrorDestDir );

    FormData fdlErrorDestDir = new FormData();
    fdlErrorDestDir.left = new FormAttachment( 0, 0 );
    fdlErrorDestDir.top = new FormAttachment( previous, margin );
    fdlErrorDestDir.right = new FormAttachment( middle, -margin );
    wlErrorDestDir.setLayoutData( fdlErrorDestDir );

    wbbErrorDestDir = new Button( wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbErrorDestDir );
    wbbErrorDestDir.setText( BUTTON_BROWSE );
    wbbErrorDestDir.setToolTipText( BaseMessages.getString( BASE_PKG, "System.Tooltip.BrowseForDir" ) );

    FormData fdbErrorDestDir = new FormData();
    fdbErrorDestDir.right = new FormAttachment( 100, 0 );
    fdbErrorDestDir.top = new FormAttachment( previous, margin );
    wbbErrorDestDir.setLayoutData( fdbErrorDestDir );

    wbvErrorDestDir = new Button( wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbvErrorDestDir );
    wbvErrorDestDir.setText( BUTTON_VARIABLE );
    wbvErrorDestDir.setToolTipText( TOOLTIP_VARIABLE );

    FormData fdbvErrorDestDir = new FormData();
    fdbvErrorDestDir.right = new FormAttachment( wbbErrorDestDir, -margin );
    fdbvErrorDestDir.left = new FormAttachment( wbvWarnDestDir, 0, SWT.LEFT );
    fdbvErrorDestDir.top = new FormAttachment( previous, margin );
    wbvErrorDestDir.setLayoutData( fdbvErrorDestDir );

    wErrorExt = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorExt );
    wErrorExt.addModifyListener( lsMod );

    FormData fdErrorDestExt = new FormData();
    fdErrorDestExt.left = new FormAttachment( wWarnExt, 0, SWT.LEFT );
    fdErrorDestExt.right = new FormAttachment( wWarnExt, 0, SWT.RIGHT );
    fdErrorDestExt.top = new FormAttachment( previous, margin );
    wErrorExt.setLayoutData( fdErrorDestExt );

    wlErrorExt = new Label( wErrorComp, SWT.RIGHT );
    wlErrorExt.setText( LABEL_EXTENSION );
    props.setLook( wlErrorExt );

    FormData fdlErrorDestExt = new FormData();
    fdlErrorDestExt.top = new FormAttachment( previous, margin );
    fdlErrorDestExt.right = new FormAttachment( wErrorExt, -margin );
    wlErrorExt.setLayoutData( fdlErrorDestExt );

    wErrorDestDir = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wErrorDestDir );
    wErrorDestDir.addModifyListener( lsMod );

    FormData fdErrorDestDir = new FormData();
    fdErrorDestDir.left = new FormAttachment( middle, 0 );
    fdErrorDestDir.right = new FormAttachment( wlErrorExt, -margin );
    fdErrorDestDir.top = new FormAttachment( previous, margin );
    wErrorDestDir.setLayoutData( fdErrorDestDir );

    // Listen to the Browse... button
    wbbErrorDestDir.addSelectionListener( new DirectoryBrowserAdapter( wErrorDestDir ) );

    // Listen to the Variable... button
    wbvErrorDestDir.addSelectionListener( VariableButtonListenerFactory.getSelectionAdapter( shell, wErrorDestDir,
      transMeta ) );

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wErrorDestDir.addModifyListener( getModifyListenerTooltipText( wErrorDestDir ) );

    // Data Error lines files directory + extention
    previous = wErrorDestDir;

    // LineNrDestDir line
    wlLineNrDestDir = new Label( wErrorComp, SWT.RIGHT );
    wlLineNrDestDir.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LineNrDestDir.Label" ) );
    props.setLook( wlLineNrDestDir );

    FormData fdlLineNrDestDir = new FormData();
    fdlLineNrDestDir.left = new FormAttachment( 0, 0 );
    fdlLineNrDestDir.top = new FormAttachment( previous, margin );
    fdlLineNrDestDir.right = new FormAttachment( middle, -margin );
    wlLineNrDestDir.setLayoutData( fdlLineNrDestDir );

    wbbLineNrDestDir = new Button( wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbLineNrDestDir );
    wbbLineNrDestDir.setText( BUTTON_BROWSE );
    wbbLineNrDestDir.setToolTipText( BaseMessages.getString( BASE_PKG, "System.Tooltip.Browse" ) );

    FormData fdbLineNrDestDir = new FormData();
    fdbLineNrDestDir.right = new FormAttachment( 100, 0 );
    fdbLineNrDestDir.top = new FormAttachment( previous, margin );
    wbbLineNrDestDir.setLayoutData( fdbLineNrDestDir );

    wbvLineNrDestDir = new Button( wErrorComp, SWT.PUSH | SWT.CENTER );
    props.setLook( wbvLineNrDestDir );
    wbvLineNrDestDir.setText( BUTTON_VARIABLE );
    wbvLineNrDestDir.setToolTipText( TOOLTIP_VARIABLE );

    FormData fdbvLineNrDestDir = new FormData();
    fdbvLineNrDestDir.right = new FormAttachment( wbbLineNrDestDir, -margin );
    fdbvLineNrDestDir.left = new FormAttachment( wbvErrorDestDir, 0, SWT.LEFT );
    fdbvLineNrDestDir.top = new FormAttachment( previous, margin );
    wbvLineNrDestDir.setLayoutData( fdbvLineNrDestDir );

    wLineNrExt = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLineNrExt );
    wLineNrExt.addModifyListener( lsMod );

    FormData fdLineNrDestExt = new FormData();
    fdLineNrDestExt.left = new FormAttachment( wErrorExt, 0, SWT.LEFT );
    fdLineNrDestExt.right = new FormAttachment( wErrorExt, 0, SWT.RIGHT );
    fdLineNrDestExt.top = new FormAttachment( previous, margin );
    wLineNrExt.setLayoutData( fdLineNrDestExt );

    wlLineNrExt = new Label( wErrorComp, SWT.RIGHT );
    wlLineNrExt.setText( LABEL_EXTENSION );
    props.setLook( wlLineNrExt );

    FormData fdlLineNrDestExt = new FormData();
    fdlLineNrDestExt.top = new FormAttachment( previous, margin );
    fdlLineNrDestExt.right = new FormAttachment( wLineNrExt, -margin );
    wlLineNrExt.setLayoutData( fdlLineNrDestExt );

    wLineNrDestDir = new Text( wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLineNrDestDir );
    wLineNrDestDir.addModifyListener( lsMod );

    FormData fdLineNrDestDir = new FormData();
    fdLineNrDestDir.left = new FormAttachment( middle, 0 );
    fdLineNrDestDir.right = new FormAttachment( wlLineNrExt, -margin );
    fdLineNrDestDir.top = new FormAttachment( previous, margin );
    wLineNrDestDir.setLayoutData( fdLineNrDestDir );

    // Listen to the Browse... button
    wbbLineNrDestDir.addSelectionListener( new DirectoryBrowserAdapter( wLineNrDestDir ) );

    // Listen to the Variable... button
    wbvLineNrDestDir.addSelectionListener( VariableButtonListenerFactory.getSelectionAdapter( shell, wLineNrDestDir,
      transMeta ) );

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wLineNrDestDir.addModifyListener( getModifyListenerTooltipText( wLineNrDestDir ) );

    FormData fdErrorComp = new FormData();
    fdErrorComp.left = new FormAttachment( 0, 0 );
    fdErrorComp.top = new FormAttachment( 0, 0 );
    fdErrorComp.right = new FormAttachment( 100, 0 );
    fdErrorComp.bottom = new FormAttachment( 100, 0 );
    wErrorComp.setLayoutData( fdErrorComp );

    wErrorComp.pack();
    // What's the size:
    Rectangle bounds = wErrorComp.getBounds();

    wErrorSComp.setContent( wErrorComp );
    wErrorSComp.setExpandHorizontal( true );
    wErrorSComp.setExpandVertical( true );
    wErrorSComp.setMinWidth( bounds.width );
    wErrorSComp.setMinHeight( bounds.height );

    wErrorTab.setControl( wErrorSComp );

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

  }

  private void addFiltersTabs() {
    // Filters tab...
    CTabItem wFilterTab = new CTabItem( wTabFolder, SWT.NONE );
    wFilterTab.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FilterTab.TabTitle" ) );

    FormLayout filterLayout = new FormLayout();
    filterLayout.marginWidth = Const.FORM_MARGIN;
    filterLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFilterComp = new Composite( wTabFolder, SWT.NONE );
    wFilterComp.setLayout( filterLayout );
    props.setLook( wFilterComp );

    final int FilterRows = input.getFilter().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FilterStringColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FilterPositionColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.StopOnFilterColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FilterPositiveColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, YES_NO_COMBO ) };

    colinf[ 2 ].setToolTip( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.StopOnFilterColumn.Tooltip" ) );
    colinf[ 3 ].setToolTip( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FilterPositiveColumn.Tooltip" ) );

    wFilter = new TableView( transMeta, wFilterComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FilterRows, lsMod, props );

    FormData fdFilter = new FormData();
    fdFilter.left = new FormAttachment( 0, 0 );
    fdFilter.top = new FormAttachment( 0, 0 );
    fdFilter.right = new FormAttachment( 100, 0 );
    fdFilter.bottom = new FormAttachment( 100, 0 );
    wFilter.setLayoutData( fdFilter );

    FormData fdFilterComp = new FormData();
    fdFilterComp.left = new FormAttachment( 0, 0 );
    fdFilterComp.top = new FormAttachment( 0, 0 );
    fdFilterComp.right = new FormAttachment( 100, 0 );
    fdFilterComp.bottom = new FormAttachment( 100, 0 );
    wFilterComp.setLayoutData( fdFilterComp );

    wFilterComp.layout();
    wFilterTab.setControl( wFilterComp );
  }

  private void addFieldsTabs() {
    // Fields tab...
    CTabItem wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FieldsTab.TabTitle" ) );

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    wFieldsComp.setLayout( fieldsLayout );
    props.setLook( wFieldsComp );

    wGet = new Button( wFieldsComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( BASE_PKG, "System.Button.GetFields" ) );
    fdGet = new FormData();
    fdGet.left = new FormAttachment( 50, 0 );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    final int FieldsRows = input.inputFields.length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NameColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.TypeColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.getTypes(), true ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FormatColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 2 ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.PositionColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LengthColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.PrecisionColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.CurrencyColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.DecimalColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.GroupColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NullIfColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.IfNullColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.TrimTypeColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaBase.trimTypeDesc, true ),
        new ColumnInfo( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RepeatColumn.Column" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { COMBO_YES, COMBO_NO }, true ) };

    colinf[ 12 ].setToolTip( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.RepeatColumn.Tooltip" ) );

    wFields = new TableView( transMeta, wFieldsComp, SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );
  }

  public void setFlags() {
    boolean accept = wAccFilenames.getSelection();
    wlPassThruFields.setEnabled( accept );
    wPassThruFields.setEnabled( accept );
    if ( !wAccFilenames.getSelection() ) {
      wPassThruFields.setSelection( false );
    }
    wlAccField.setEnabled( accept );
    wAccField.setEnabled( accept );
    wlAccStep.setEnabled( accept );
    wAccStep.setEnabled( accept );
    wlFilenameList.setEnabled( !accept );
    wFilenameList.setEnabled( !accept );
    wbShowFiles.setEnabled( !accept );

    wFirst.setEnabled( !accept );
    wFirstHeader.setEnabled( !accept );

    wlInclFilenameField.setEnabled( wInclFilename.getSelection() );
    wInclFilenameField.setEnabled( wInclFilename.getSelection() );

    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
    wlRownumByFileField.setEnabled( wInclRownum.getSelection() );
    wRownumByFile.setEnabled( wInclRownum.getSelection() );

    // Error handling tab...
    wlSkipErrorLines.setEnabled( wErrorIgnored.getSelection() );
    wSkipErrorLines.setEnabled( wErrorIgnored.getSelection() );
    wlErrorCount.setEnabled( wErrorIgnored.getSelection() );
    wErrorCount.setEnabled( wErrorIgnored.getSelection() );
    wlErrorFields.setEnabled( wErrorIgnored.getSelection() );
    wErrorFields.setEnabled( wErrorIgnored.getSelection() );
    wlErrorText.setEnabled( wErrorIgnored.getSelection() );
    wErrorText.setEnabled( wErrorIgnored.getSelection() );

    wlWarnDestDir.setEnabled( wErrorIgnored.getSelection() );
    wWarnDestDir.setEnabled( wErrorIgnored.getSelection() );
    wlWarnExt.setEnabled( wErrorIgnored.getSelection() );
    wWarnExt.setEnabled( wErrorIgnored.getSelection() );
    wbbWarnDestDir.setEnabled( wErrorIgnored.getSelection() );
    wbvWarnDestDir.setEnabled( wErrorIgnored.getSelection() );

    wlErrorDestDir.setEnabled( wErrorIgnored.getSelection() );
    wErrorDestDir.setEnabled( wErrorIgnored.getSelection() );
    wlErrorExt.setEnabled( wErrorIgnored.getSelection() );
    wErrorExt.setEnabled( wErrorIgnored.getSelection() );
    wbbErrorDestDir.setEnabled( wErrorIgnored.getSelection() );
    wbvErrorDestDir.setEnabled( wErrorIgnored.getSelection() );

    wlLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
    wLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
    wlLineNrExt.setEnabled( wErrorIgnored.getSelection() );
    wLineNrExt.setEnabled( wErrorIgnored.getSelection() );
    wbbLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );
    wbvLineNrDestDir.setEnabled( wErrorIgnored.getSelection() );

    wlNrHeader.setEnabled( wHeader.getSelection() );
    wNrHeader.setEnabled( wHeader.getSelection() );
    wlNrFooter.setEnabled( wFooter.getSelection() );
    wNrFooter.setEnabled( wFooter.getSelection() );
    wlNrWraps.setEnabled( wWraps.getSelection() );
    wNrWraps.setEnabled( wWraps.getSelection() );

    wlNrLinesPerPage.setEnabled( wLayoutPaged.getSelection() );
    wNrLinesPerPage.setEnabled( wLayoutPaged.getSelection() );
    wlNrLinesDocHeader.setEnabled( wLayoutPaged.getSelection() );
    wNrLinesDocHeader.setEnabled( wLayoutPaged.getSelection() );
  }

  /**
   * Read the data from the HadoopFileInputMeta object and show it in this dialog.
   *
   * @param meta The HadoopFileInputMeta object to obtain the data from.
   */
  public void getData( HadoopFileInputMeta meta ) {
    final HadoopFileInputMeta in = meta;

    wAccFilenames.setSelection( in.isAcceptingFilenames() );
    wPassThruFields.setSelection( in.inputFiles.passingThruFields );
    if ( in.getAcceptingField() != null ) {
      wAccField.setText( in.getAcceptingField() );
    }
    if ( in.getAcceptingStep() != null ) {
      wAccStep.setText( in.getAcceptingStep().getName() );
    }
    if ( in.getFileName() != null ) {
      wFilenameList.removeAll();

      for ( int i = 0; i < in.getFileName().length; i++ ) {
        String sourceUrl = in.getFileName()[ i ];
        String clusterName = input.getClusterNameBy( sourceUrl );
        String environment = STATIC_ENVIRONMENT;
        if ( in.environment != null && i < in.environment.length && in.environment[ i ] != null ) {
          environment = in.environment[ i ];
        }
        if ( clusterName != null ) {
          clusterName =
            clusterName.startsWith( HadoopFileInputMeta.LOCAL_SOURCE_FILE ) ? LOCAL_ENVIRONMENT : clusterName;
          clusterName =
            clusterName.startsWith( HadoopFileInputMeta.STATIC_SOURCE_FILE ) ? STATIC_ENVIRONMENT : clusterName;
          clusterName = clusterName.startsWith( HadoopFileInputMeta.S3_SOURCE_FILE ) ? S3_ENVIRONMENT : clusterName;
          if ( clusterName.equals( LOCAL_ENVIRONMENT ) || clusterName.equals( STATIC_ENVIRONMENT )
            || clusterName.equals( S3_ENVIRONMENT ) ) {
            environment = clusterName;
          } else {
            sourceUrl = input.getUrlPath( sourceUrl );
            NamedCluster c = namedClusterService.getNamedClusterByName( clusterName, metaStore );
            environment = c == null ? "" : clusterName;
          }
        }

        wFilenameList
          .add( environment, sourceUrl, in.inputFiles.fileMask[ i ],
            in.getRequiredFilesDesc( in.inputFiles.fileRequired[ i ] ),
            in.getRequiredFilesDesc( in.inputFiles.includeSubFolders[ i ] ) );
      }
      wFilenameList.removeEmptyRows();
      wFilenameList.setRowNums();
      wFilenameList.optWidth( true );
    }
    if ( in.content.fileType != null ) {
      wFiletype.setText( in.content.fileType );
    }
    if ( in.content.separator != null ) {
      wSeparator.setText( in.content.separator );
    }
    if ( in.content.enclosure != null ) {
      wEnclosure.setText( in.content.enclosure );
    }
    if ( in.content.escapeCharacter != null ) {
      wEscape.setText( in.content.escapeCharacter );
    }
    wHeader.setSelection( in.content.header );
    wNrHeader.setText( "" + in.content.nrHeaderLines );
    wFooter.setSelection( in.content.footer );
    wNrFooter.setText( "" + in.content.nrFooterLines );
    wWraps.setSelection( in.content.lineWrapped );
    wNrWraps.setText( "" + in.content.nrWraps );
    wLayoutPaged.setSelection( in.content.layoutPaged );
    wNrLinesPerPage.setText( "" + in.content.nrLinesPerPage );
    wNrLinesDocHeader.setText( "" + in.content.nrLinesDocHeader );
    if ( in.content.fileCompression != null ) {
      wCompression.setText( in.content.fileCompression );
    }
    wNoempty.setSelection( in.content.noEmptyLines );
    wInclFilename.setSelection( in.content.includeFilename );
    wInclRownum.setSelection( in.content.includeRowNumber );
    wRownumByFile.setSelection( in.content.rowNumberByFile );
    wDateLenient.setSelection( in.content.dateFormatLenient );
    wAddResult.setSelection( in.inputFiles.isaddresult );

    if ( in.content.filenameField != null ) {
      wInclFilenameField.setText( in.content.filenameField );
    }
    if ( in.content.rowNumberField != null ) {
      wInclRownumField.setText( in.content.rowNumberField );
    }
    if ( in.content.fileFormat != null ) {
      wFormat.setText( in.content.fileFormat );
    }
    wLimit.setText( "" + in.content.rowLimit );

    logDebug( "getting fields info..." );
    getFieldsData( in, false );

    if ( in.getEncoding() != null ) {
      wEncoding.setText( in.getEncoding() );
    }

    // Error handling fields...
    wErrorIgnored.setSelection( in.errorHandling.errorIgnored );
    wSkipErrorLines.setSelection( in.isErrorLineSkipped() );
    if ( in.getErrorCountField() != null ) {
      wErrorCount.setText( in.getErrorCountField() );
    }
    if ( in.getErrorFieldsField() != null ) {
      wErrorFields.setText( in.getErrorFieldsField() );
    }
    if ( in.getErrorTextField() != null ) {
      wErrorText.setText( in.getErrorTextField() );
    }
    if ( in.errorHandling.warningFilesDestinationDirectory != null ) {
      wWarnDestDir.setText( in.errorHandling.warningFilesDestinationDirectory );
    }
    if ( in.errorHandling.warningFilesExtension != null ) {
      wWarnExt.setText( in.errorHandling.warningFilesExtension );
    }
    if ( in.errorHandling.errorFilesDestinationDirectory != null ) {
      wErrorDestDir.setText( in.errorHandling.errorFilesDestinationDirectory );
    }
    if ( in.errorHandling.errorFilesExtension != null ) {
      wErrorExt.setText( in.errorHandling.errorFilesExtension );
    }
    if ( in.errorHandling.lineNumberFilesDestinationDirectory != null ) {
      wLineNrDestDir.setText( in.errorHandling.lineNumberFilesDestinationDirectory );
    }
    if ( in.errorHandling.lineNumberFilesExtension != null ) {
      wLineNrExt.setText( in.errorHandling.lineNumberFilesExtension );
    }
    for ( int i = 0; i < in.getFilter().length; i++ ) {
      TableItem item = wFilter.table.getItem( i );

      TextFileFilter filter = in.getFilter()[ i ];
      if ( filter.getFilterString() != null ) {
        item.setText( 1, filter.getFilterString() );
      }
      if ( filter.getFilterPosition() >= 0 ) {
        item.setText( 2, "" + filter.getFilterPosition() );
      }
      item.setText( 3, filter.isFilterLastLine() ? COMBO_YES : COMBO_NO );
      item.setText( 4, filter.isFilterPositive() ? COMBO_YES : COMBO_NO );
    }

    // Date locale
    wDateLocale.setText( in.content.dateFormatLocale.toString() );

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );

    wFilter.removeEmptyRows();
    wFilter.setRowNums();
    wFilter.optWidth( true );

    setFlags();

    wStepname.selectAll();
  }

  private void getFieldsData( HadoopFileInputMeta in, boolean insertAtTop ) {
    for ( int i = 0; i < in.inputFields.length; i++ ) {
      BaseFileField field = in.inputFields[ i ];

      TableItem item;

      if ( insertAtTop ) {
        item = new TableItem( wFields.table, SWT.NONE, i );
      } else {
        if ( i >= wFields.table.getItemCount() ) {
          item = wFields.table.getItem( i );
        } else {
          item = new TableItem( wFields.table, SWT.NONE );
        }
      }

      item.setText( 1, field.getName() );
      String type = field.getTypeDesc();
      String format = field.getFormat();
      String position = "" + field.getPosition();
      String length = "" + field.getLength();
      String prec = "" + field.getPrecision();
      String curr = field.getCurrencySymbol();
      String group = field.getGroupSymbol();
      String decim = field.getDecimalSymbol();
      String def = field.getNullString();
      String ifNull = field.getIfNullValue();
      String trim = field.getTrimTypeDesc();
      String rep =
        field.isRepeated() ? COMBO_YES : COMBO_NO;

      if ( type != null ) {
        item.setText( 2, type );
      }
      if ( format != null ) {
        item.setText( 3, format );
      }
      if ( position != null && !"-1".equals( position ) ) {
        item.setText( 4, position );
      }
      if ( length != null && !"-1".equals( length ) ) {
        item.setText( 5, length );
      }
      if ( prec != null && !"-1".equals( prec ) ) {
        item.setText( 6, prec );
      }
      if ( curr != null ) {
        item.setText( 7, curr );
      }
      if ( decim != null ) {
        item.setText( 8, decim );
      }
      if ( group != null ) {
        item.setText( 9, group );
      }
      if ( def != null ) {
        item.setText( 10, def );
      }
      if ( ifNull != null ) {
        item.setText( 11, ifNull );
      }
      if ( trim != null ) {
        item.setText( 12, trim );
      }
      if ( rep != null ) {
        item.setText( 13, rep );
      }
    }

  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wEncoding.removeAll();
      List<Charset> values = new ArrayList<>( Charset.availableCharsets().values() );
      for ( int i = 0; i < values.size(); i++ ) {
        Charset charSet = values.get( i );
        wEncoding.add( charSet.displayName() );
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wEncoding.getItems() );
      if ( idx >= 0 ) {
        wEncoding.select( idx );
      }
    }
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    getInfo( input );
    dispose();
  }

  private void getInfo( HadoopFileInputMeta meta ) {
    stepname = wStepname.getText(); // return value

    // copy info to HadoopFileInputMeta class (input)
    meta.inputFiles.acceptingFilenames = wAccFilenames.getSelection();
    meta.inputFiles.passingThruFields = wPassThruFields.getSelection();
    meta.inputFiles.acceptingField = wAccField.getText();
    meta.inputFiles.acceptingStepName = wAccStep.getText();
    meta.setAcceptingStep( transMeta.findStep( wAccStep.getText() ) );

    meta.content.fileType = wFiletype.getText();
    meta.content.fileFormat = wFormat.getText();
    meta.content.separator = wSeparator.getText();
    meta.content.enclosure = wEnclosure.getText();
    meta.content.escapeCharacter = wEscape.getText();
    meta.content.rowLimit = Const.toLong( wLimit.getText(), 0L );
    meta.content.filenameField = wInclFilenameField.getText();
    meta.content.rowNumberField = wInclRownumField.getText();
    meta.inputFiles.isaddresult = wAddResult.getSelection();

    meta.content.includeFilename = wInclFilename.getSelection();
    meta.content.includeRowNumber = wInclRownum.getSelection();
    meta.content.rowNumberByFile = wRownumByFile.getSelection();
    meta.content.header = wHeader.getSelection();
    meta.content.nrHeaderLines = Const.toInt( wNrHeader.getText(), 1 );
    meta.content.footer = wFooter.getSelection();
    meta.content.nrFooterLines = Const.toInt( wNrFooter.getText(), 1 );
    meta.content.lineWrapped = wWraps.getSelection();
    meta.content.nrWraps = Const.toInt( wNrWraps.getText(), 1 );
    meta.content.layoutPaged = wLayoutPaged.getSelection();
    meta.content.nrLinesPerPage = Const.toInt( wNrLinesPerPage.getText(), 80 );
    meta.content.nrLinesDocHeader = Const.toInt( wNrLinesDocHeader.getText(), 0 );
    meta.content.fileCompression = wCompression.getText();
    meta.content.dateFormatLenient = wDateLenient.getSelection();
    meta.content.noEmptyLines = wNoempty.getSelection();
    meta.content.encoding = wEncoding.getText();

    int nrfiles = wFilenameList.getItemCount();
    int nrfields = wFields.nrNonEmpty();
    int nrfilters = wFilter.nrNonEmpty();
    meta.allocate( nrfiles, nrfields, nrfilters );

    Map<String, String> namedClusterURLMappings = new HashMap<>();
    String[] fileNames = new String[ wFilenameList.getItems( 1 ).length ];
    meta.environment = wFilenameList.getItems( 0 );

    for ( int i = 0; i < meta.environment.length; i++ ) {
      String sourceNc = meta.environment[ i ];
      sourceNc = sourceNc.equals( LOCAL_ENVIRONMENT ) ? HadoopFileInputMeta.LOCAL_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( STATIC_ENVIRONMENT ) ? HadoopFileInputMeta.STATIC_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( S3_ENVIRONMENT ) ? HadoopFileInputMeta.S3_SOURCE_FILE + i : sourceNc;
      String source = wFilenameList.getItems( 1 )[ i ];
      if ( !Utils.isEmpty( source ) ) {
        fileNames[ i ] = input.loadUrl( source, sourceNc, getMetaStore(), namedClusterURLMappings );
      } else {
        fileNames[ i ] = "";
      }
    }

    meta.setFileName( fileNames );
    meta.inputFiles.fileMask = wFilenameList.getItems( 2 );
    meta.inputFiles.setFileRequired( wFilenameList.getItems( 3 ) );
    meta.inputFiles.setIncludeSubFolders( wFilenameList.getItems( 4 ) );

    input.setNamedClusterURLMapping( namedClusterURLMappings );

    for ( int i = 0; i < nrfields; i++ ) {
      BaseFileField field = new BaseFileField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 1 ) );
      field.setType( ValueMetaBase.getType( item.getText( 2 ) ) );
      field.setFormat( item.getText( 3 ) );
      field.setPosition( Const.toInt( item.getText( 4 ), -1 ) );
      field.setLength( Const.toInt( item.getText( 5 ), -1 ) );
      field.setPrecision( Const.toInt( item.getText( 6 ), -1 ) );
      field.setCurrencySymbol( item.getText( 7 ) );
      field.setDecimalSymbol( item.getText( 8 ) );
      field.setGroupSymbol( item.getText( 9 ) );
      field.setNullString( item.getText( 10 ) );
      field.setIfNullValue( item.getText( 11 ) );
      field.setTrimType( ValueMetaBase.getTrimTypeByDesc( item.getText( 12 ) ) );
      field.setRepeated( COMBO_YES.equalsIgnoreCase( item.getText( 13 ) ) );

      ( meta.inputFields )[ i ] = field;
    }

    for ( int i = 0; i < nrfilters; i++ ) {
      TableItem item = wFilter.getNonEmpty( i );
      TextFileFilter filter = new TextFileFilter();
      ( meta.getFilter() )[ i ] = filter;

      filter.setFilterString( item.getText( 1 ) );
      filter.setFilterPosition( Const.toInt( item.getText( 2 ), -1 ) );
      filter.setFilterLastLine( COMBO_YES.equalsIgnoreCase( item.getText( 3 ) ) );
      filter.setFilterPositive( COMBO_YES.equalsIgnoreCase( item.getText( 4 ) ) );
    }
    // Error handling fields...
    meta.errorHandling.errorIgnored = wErrorIgnored.getSelection();
    meta.setErrorLineSkipped( wSkipErrorLines.getSelection() );
    meta.setErrorCountField( wErrorCount.getText() );
    meta.setErrorFieldsField( wErrorFields.getText() );
    meta.setErrorTextField( wErrorText.getText() );

    meta.errorHandling.warningFilesDestinationDirectory = wWarnDestDir.getText();
    meta.errorHandling.warningFilesExtension = wWarnExt.getText();
    meta.errorHandling.errorFilesDestinationDirectory = wErrorDestDir.getText();
    meta.errorHandling.errorFilesExtension = wErrorExt.getText();
    meta.errorHandling.lineNumberFilesDestinationDirectory = wLineNrDestDir.getText();
    meta.errorHandling.lineNumberFilesExtension = wLineNrExt.getText();

    // Date format Locale
    Locale locale = EnvUtil.createLocale( wDateLocale.getText() );
    if ( !locale.equals( Locale.getDefault() ) ) {
      meta.content.dateFormatLocale = locale;
    } else {
      meta.content.dateFormatLocale = Locale.getDefault();
    }
  }

  private void get() {
    if ( wFiletype.getText().equalsIgnoreCase( "CSV" ) ) {
      getCSV();
    } else {
      getFixed();
    }
  }

  // Get the data layout
  private void getCSV() {
    HadoopFileInputMeta meta = new HadoopFileInputMeta();
    getInfo( meta );
    HadoopFileInputMeta previousMeta = (HadoopFileInputMeta) meta.clone();
    FileInputList textFileList = meta.getTextFileList( transMeta.getBowl(), transMeta );
    InputStream fileInputStream = null;
    InputStream inputStream = null;
    StringBuilder lineStringBuilder = new StringBuilder( 256 );
    int fileFormatType = meta.getFileFormatTypeNr();

    String delimiter = transMeta.environmentSubstitute( meta.content.separator );

    if ( textFileList.nrOfFiles() > 0 ) {
      int clearFields = meta.content.header ? SWT.YES : SWT.NO;
      int nrInputFields = meta.inputFields.length;

      if ( meta.content.header && nrInputFields > 0 ) {
        MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION );
        mb.setMessage( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ClearFieldList.DialogMessage" ) );
        mb.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.ClearFieldList.DialogTitle" ) );
        clearFields = mb.open();
        if ( clearFields == SWT.CANCEL ) {
          return;
        }
      }

      try {
        wFields.table.removeAll();
        Table table = wFields.table;
        inputStream = getInputStream( meta, textFileList );
        InputStreamReader reader = getInputStreamReader( meta, inputStream );


        if ( clearFields == SWT.YES || !meta.content.header || nrInputFields > 0 ) {
          // Scan the header-line, determine fields...
          String line = null;

          if ( meta.content.header || meta.inputFields.length == 0 ) {
            line = getLine( meta, textFileList );
            if ( line != null ) {
              // Estimate the number of input fields...
              // Chop up the line using the delimiter
              String[] guessedFields =
                TextFileInputUtils.guessStringsFromLine( new Variables(), log, line, meta, delimiter, StringUtil
                  .substituteHex( meta.content.enclosure ), StringUtil.substituteHex( meta.content.escapeCharacter ) );

              for ( int i = 0; i < guessedFields.length; i++ ) {
                String field = guessedFields[ i ];
                if ( field == null || field.length() == 0 || ( nrInputFields == 0 && !meta.content.header ) ) {
                  field = "Field" + ( i + 1 );
                } else {
                  // Trim the field
                  field = Const.trim( field );
                  // Replace all spaces & - with underscore _
                  field = Const.replace( field, " ", "_" );
                  field = Const.replace( field, "-", "_" );
                }

                TableItem item = new TableItem( table, SWT.NONE );
                item.setText( 1, field );
                item.setText( 2, "String" ); // The default type is String...
              }

              wFields.setRowNums();
              wFields.optWidth( true );

              // Copy it...
              getInfo( meta );
            }
          }

          // Sample a few lines to determine the correct type of the fields...
          String shellText = BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LinesToSample.DialogTitle" );
          String lineText = BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LinesToSample.DialogMessage" );
          EnterNumberDialog end = new EnterNumberDialog( shell, 100, shellText, lineText );
          int samples = end.open();
          if ( samples >= 0 ) {
            getInfo( meta );

            TextFileCSVImportProgressDialog pd =
              new TextFileCSVImportProgressDialog( shell, meta, transMeta, reader, samples, clearFields == SWT.YES );
            String message = pd.open();
            if ( message != null ) {
              wFields.removeAll();

              // OK, what's the result of our search?
              getData( meta );

              // If we didn't want the list to be cleared, we need to re-inject the previous values...
              //
              if ( clearFields == SWT.NO ) {
                getFieldsData( previousMeta, true );
                wFields.table.setSelection( previousMeta.inputFields.length, wFields.table.getItemCount() - 1 );
              }

              wFields.removeEmptyRows();
              wFields.setRowNums();
              wFields.optWidth( true );

              EnterTextDialog etd =
                new EnterTextDialog( shell, BaseMessages.getString( BASE_PKG,
                  "TextFileInputDialog.ScanResults.DialogTitle" ), BaseMessages.getString( BASE_PKG,
                  "TextFileInputDialog.ScanResults.DialogMessage" ), message, true );
              etd.setReadOnly();
              etd.open();
            }
          }
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage(
            BaseMessages.getString( BASE_PKG, "TextFileInputDialog.UnableToReadHeaderLine.DialogMessage" ) );
          mb.setText( ERROR_TITLE );
          mb.open();
        }
      } catch ( IOException e ) {
        new ErrorDialog( shell, BaseMessages.getString( BASE_PKG, "TextFileInputDialog.IOError.DialogTitle" ),
          BaseMessages.getString( BASE_PKG, "TextFileInputDialog.IOError.DialogMessage" ), e );
      } catch ( KettleException e ) {
        new ErrorDialog( shell, ERROR_TITLE, BaseMessages
          .getString( BASE_PKG, "TextFileInputDialog.ErrorGettingFileDesc.DialogMessage" ), e );
      } finally {
        try {
          inputStream.close();
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NoValidFileFound.DialogMessage" ) );
      mb.setText( ERROR_TITLE );
      mb.open();
    }
  }

  public static final int guessPrecision( double d ) {
    // Round numbers
    long frac = Math.round( ( d - Math.floor( d ) ) * 1E10 ); // max precision : 10
    int precision = 10;

    // 0,34 --> 3400000000
    // 0 to the right --> precision -1!
    // 0 to the right means frac%10 == 0

    while ( precision >= 0 && ( frac % 10 ) == 0 ) {
      frac /= 10;
      precision--;
    }
    precision++;

    return precision;
  }

  public static final int guessIntLength( double d ) {
    double flr = Math.floor( d );
    int len = 1;

    while ( flr > 9 ) {
      flr /= 10;
      flr = Math.floor( flr );
      len++;
    }

    return len;
  }

  public static final int guessLength( double d ) {
    int intlen = guessIntLength( d );
    int precis = guessPrecision( d );
    int length = 1;

    if ( precis > 0 ) {
      length = intlen + 1 + precis;
    } else {
      length = intlen;
    }

    return length;
  }

  // Preview the data
  private void preview() {
    // Create the XML input step
    HadoopFileInputMeta oneMeta = new HadoopFileInputMeta();
    getInfo( oneMeta );

    if ( oneMeta.isAcceptingFilenames() ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );

      // Nothing found that matches your criteria
      mb.setMessage( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Dialog.SpecifyASampleFile.Message" ) );

      // Sorry!
      mb.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.Dialog.SpecifyASampleFile.Title" ) );
      mb.open();
      return;
    }

    TransMeta previewMeta =
      TransPreviewFactory.generatePreviewTransformation( transMeta, oneMeta, wStepname.getText() );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString( BASE_PKG,
        "TextFileInputDialog.PreviewSize.DialogTitle" ), BaseMessages.getString( BASE_PKG,
        "TextFileInputDialog.PreviewSize.DialogMessage" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog =
        new TransPreviewProgressDialog( shell, previewMeta, new String[] { wStepname.getText() },
          new int[] { previewSize } );
      progressDialog.open();

      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( trans.getResult() != null && trans.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog( shell, BaseMessages.getString( BASE_PKG, "System.Dialog.PreviewError.Title" ),
              BaseMessages.getString( BASE_PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
        new PreviewRowsDialog( shell, transMeta, SWT.NONE, wStepname.getText(), progressDialog
          .getPreviewRowsMeta( wStepname.getText() ), progressDialog.getPreviewRows( wStepname.getText() ),
          loggingText );
      prd.open();
    }
  }

  // Get the first x lines
  private void first( boolean skipHeaders ) {
    HadoopFileInputMeta info = new HadoopFileInputMeta();
    getInfo( info );

    try {
      if ( info.getTextFileList( transMeta.getBowl(), transMeta ).nrOfFiles() > 0 ) {
        String shellText = BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LinesToView.DialogTitle" );
        String lineText = BaseMessages.getString( BASE_PKG, "TextFileInputDialog.LinesToView.DialogMessage" );
        EnterNumberDialog end = new EnterNumberDialog( shell, 100, shellText, lineText );
        int nrLines = end.open();
        if ( nrLines >= 0 ) {
          List<String> linesList = getFirst( nrLines, skipHeaders );
          if ( linesList != null && linesList.size() > 0 ) {
            String firstlines = "";
            for ( int i = 0; i < linesList.size(); i++ ) {
              firstlines += linesList.get( i ) + Const.CR;
            }
            EnterTextDialog etd =
              new EnterTextDialog( shell, BaseMessages.getString( BASE_PKG,
                "TextFileInputDialog.ContentOfFirstFile.DialogTitle" ),
                ( nrLines == 0 ? BaseMessages.getString( BASE_PKG,
                  "TextFileInputDialog.ContentOfFirstFile.AllLines.DialogMessage" ) : BaseMessages.getString(
                  BASE_PKG, "TextFileInputDialog.ContentOfFirstFile.NLines.DialogMessage", "" + nrLines ) ),
                firstlines, true );
            etd.setReadOnly();
            etd.open();
          } else {
            MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
            mb.setMessage( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.UnableToReadLines.DialogMessage" ) );
            mb.setText( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.UnableToReadLines.DialogTitle" ) );
            mb.open();
          }
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( BASE_PKG, "TextFileInputDialog.NoValidFile.DialogMessage" ) );
        mb.setText( ERROR_TITLE );
        mb.open();
      }
    } catch ( KettleException e ) {
      new ErrorDialog( shell, ERROR_TITLE, BaseMessages.getString(
        BASE_PKG, "TextFileInputDialog.ErrorGettingData.DialogMessage" ), e );
    }
  }

  // Get the first x lines
  private List<String> getFirst( int nrlines, boolean skipHeaders ) throws KettleException {
    HadoopFileInputMeta meta = new HadoopFileInputMeta();
    getInfo( meta );
    FileInputList textFileList = meta.getTextFileList( transMeta.getBowl(), transMeta );

    InputStream fi = null;
    InputStream f = null;
    StringBuilder lineStringBuilder = new StringBuilder( 256 );
    int fileFormatType = meta.getFileFormatTypeNr();

    List<String> retval = new ArrayList<>();

    if ( textFileList.nrOfFiles() > 0 ) {
      FileObject file = textFileList.getFile( 0 );
      try {
        fi = KettleVFS.getInputStream( file );

        CompressionProvider provider =
          CompressionProviderFactory.getInstance().createCompressionProviderInstance( meta.content.fileCompression );
        f = provider.createInputStream( fi );

        BufferedInputStreamReader reader;
        if ( meta.getEncoding() != null && meta.getEncoding().length() > 0 ) {
          reader = new BufferedInputStreamReader( new InputStreamReader( f, meta.getEncoding() ) );
        } else {
          reader = new BufferedInputStreamReader( new InputStreamReader( f ) );
        }

        int linenr = 0;
        int maxnr = nrlines + ( meta.content.header ? meta.content.nrHeaderLines : 0 );

        if ( skipHeaders ) {
          // Skip the header lines first if more then one, it helps us position
          if ( meta.content.layoutPaged && meta.content.nrLinesDocHeader > 0 ) {
            int skipped = 0;
            String line = TextFileInputUtils.getLine( log, reader, fileFormatType, lineStringBuilder );
            while ( line != null && skipped < meta.content.nrLinesDocHeader - 1 ) {
              skipped++;
              line = TextFileInputUtils.getLine( log, reader, fileFormatType, lineStringBuilder );
            }
          }

          // Skip the header lines first if more then one, it helps us position
          if ( meta.content.header && meta.content.nrHeaderLines > 0 ) {
            int skipped = 0;
            String line = TextFileInputUtils.getLine( log, reader, fileFormatType, lineStringBuilder );
            while ( line != null && skipped < meta.content.nrHeaderLines - 1 ) {
              skipped++;
              line = TextFileInputUtils.getLine( log, reader, fileFormatType, lineStringBuilder );
            }
          }
        }

        String line = TextFileInputUtils.getLine( log, reader, fileFormatType, lineStringBuilder );
        while ( line != null && ( linenr < maxnr || nrlines == 0 ) ) {
          retval.add( line );
          linenr++;
          line = TextFileInputUtils.getLine( log, reader, fileFormatType, lineStringBuilder );
        }
      } catch ( Exception e ) {
        throw new KettleException( BaseMessages.getString( BASE_PKG,
          "TextFileInputDialog.Exception.ErrorGettingFirstLines", "" + nrlines, file.getName().getURI() ), e );
      } finally {
        try {
          f.close();
        } catch ( Exception e ) {
          // Ignore errors
        }
      }
    }

    return retval;
  }

  private void getFixed() {
    HadoopFileInputMeta info = new HadoopFileInputMeta();
    getInfo( info );

    Shell sh = new Shell( shell, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );

    try {
      List<String> rows = getFirst( 50, false );
      fields = getFields( info, rows );

      final TextFileImportWizardPage1 page1 = new TextFileImportWizardPage1( "1", props, rows, fields );
      page1.createControl( sh );
      final TextFileImportWizardPage2 page2 = new TextFileImportWizardPage2( "2", props, rows, fields );
      page2.createControl( sh );

      Wizard wizard = new Wizard() {
        @Override
        public boolean performFinish() {
          wFields.clearAll( false );

          for ( int i = 0; i < fields.size(); i++ ) {
            BaseFileField field = (BaseFileField) fields.get( i );
            if ( !field.isIgnored() && field.getLength() > 0 ) {
              TableItem item = new TableItem( wFields.table, SWT.NONE );
              item.setText( 1, field.getName() );
              item.setText( 2, "" + field.getTypeDesc() );
              item.setText( 3, "" + field.getFormat() );
              item.setText( 4, "" + field.getPosition() );
              item.setText( 5, field.getLength() < 0 ? "" : "" + field.getLength() );
              item.setText( 6, field.getPrecision() < 0 ? "" : "" + field.getPrecision() );
              item.setText( 7, "" + field.getCurrencySymbol() );
              item.setText( 8, "" + field.getDecimalSymbol() );
              item.setText( 9, "" + field.getGroupSymbol() );
              item.setText( 10, "" + field.getNullString() );
              item.setText( 11, "" + field.getIfNullValue() );
              item.setText( 12, "" + field.getTrimTypeDesc() );
              item.setText( 13, field.isRepeated() ? COMBO_YES : COMBO_NO );
            }

          }
          int size = wFields.table.getItemCount();
          if ( size == 0 ) {
            new TableItem( wFields.table, SWT.NONE );
          }

          wFields.removeEmptyRows();
          wFields.setRowNums();
          wFields.optWidth( true );

          input.setChanged();

          return true;
        }
      };

      wizard.addPage( page1 );
      wizard.addPage( page2 );

      WizardDialog wd = new WizardDialog( shell, wizard );
      Window.setDefaultImage( GUIResource.getInstance().getImageWizard() );
      wd.setMinimumPageSize( 700, 375 );
      wd.updateSize();
      wd.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( BASE_PKG,
        "TextFileInputDialog.ErrorShowingFixedWizard.DialogTitle" ), BaseMessages.getString( BASE_PKG,
        "TextFileInputDialog.ErrorShowingFixedWizard.DialogMessage" ), e );
    }
  }

  private Vector<TextFileInputFieldInterface> getFields( HadoopFileInputMeta info, List<String> rows ) {
    Vector<TextFileInputFieldInterface> result = new Vector<>();

    int maxsize = 0;
    for ( int i = 0; i < rows.size(); i++ ) {
      int len = rows.get( i ).length();
      if ( len > maxsize ) {
        maxsize = len;
      }
    }

    int prevEnd = 0;
    int dummynr = 1;

    for ( int i = 0; i < info.inputFields.length; i++ ) {
      BaseFileField f = info.inputFields[ i ];

      // See if positions are skipped, if this is the case, add dummy fields...
      if ( f.getPosition() != prevEnd ) {
        // gap
        BaseFileField field = new BaseFileField( "Dummy" + dummynr, prevEnd, f.getPosition() - prevEnd );
        field.setIgnored( true ); // don't include in result by default.
        result.add( field );
        dummynr++;
      }

      BaseFileField field = new BaseFileField( f.getName(), f.getPosition(), f.getLength() );
      field.setType( f.getType() );
      field.setIgnored( false );
      field.setFormat( f.getFormat() );
      field.setPrecision( f.getPrecision() );
      field.setTrimType( f.getTrimType() );
      field.setDecimalSymbol( f.getDecimalSymbol() );
      field.setGroupSymbol( f.getGroupSymbol() );
      field.setCurrencySymbol( f.getCurrencySymbol() );
      field.setRepeated( f.isRepeated() );
      field.setNullString( f.getNullString() );

      result.add( field );

      prevEnd = field.getPosition() + field.getLength();
    }

    if ( info.inputFields.length == 0 ) {
      BaseFileField field = new BaseFileField( "Field1", 0, maxsize );
      result.add( field );
    } else {
      // Take the last field and see if it reached until the maximum...
      BaseFileField f = info.inputFields[ info.inputFields.length - 1 ];

      int pos = f.getPosition();
      int len = f.getLength();
      if ( pos + len < maxsize ) {
        // If not, add an extra trailing field!
        BaseFileField field = new BaseFileField( "Dummy" + dummynr, pos + len, maxsize - pos - len );
        field.setIgnored( true ); // don't include in result by default.
        result.add( field );
        dummynr++;
      }
    }

    Collections.sort( result );

    return result;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  private SelectionAdapter getFileDirectoryListener() {

    return new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        try {
          // Setup file type filtering
          String[] fileFilters = null;
          String[] fileFilterNames = null;
          if ( !wCompression.getText().equals( "None" ) ) {
            fileFilters = new String[] { "*.zip;*.gz", "*.txt;*.csv", "*.csv", "*.txt", "*" };
            fileFilterNames =
              new String[] { BaseMessages.getString( BASE_PKG, "System.FileType.ZIPFiles" ),
                BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FileType.TextAndCSVFiles" ),
                BaseMessages.getString( BASE_PKG, "System.FileType.CSVFiles" ),
                BaseMessages.getString( BASE_PKG, "System.FileType.TextFiles" ),
                BaseMessages.getString( BASE_PKG, "System.FileType.AllFiles" ) };
          } else {
            fileFilters = new String[] { "*", "*.txt;*.csv", "*.csv", "*.txt" };
            fileFilterNames =
              new String[] { BaseMessages.getString( BASE_PKG, "System.FileType.AllFiles" ),
                BaseMessages.getString( BASE_PKG, "TextFileInputDialog.FileType.TextAndCSVFiles" ),
                BaseMessages.getString( BASE_PKG, "System.FileType.CSVFiles" ),
                BaseMessages.getString( BASE_PKG, "System.FileType.TextFiles" ) };
          }

          String clusterName = wFilenameList.getActiveTableItem().getText( wFilenameList.getActiveTableColumn() - 1 );
          String path = wFilenameList.getActiveTableItem().getText( wFilenameList.getActiveTableColumn() );

          if ( clusterName.equals( S3_ENVIRONMENT ) && !path.startsWith( Schemes.S3_SCHEME + "://" ) ) {
            path = Schemes.S3_SCHEME + "://";
          }

          // Get current file
          FileObject rootFile = null;
          FileObject initialFile = null;
          FileObject defaultInitialFile = null;

          boolean isCluster = false;
          if ( !clusterName.equals( LOCAL_ENVIRONMENT ) && !clusterName.equals( S3_ENVIRONMENT ) ) {
            if ( Const.isEmpty( path ) ) {
              path = "/";
            }
            NamedCluster namedCluster = namedClusterService.getNamedClusterByName( clusterName, getMetaStore() );
            if ( namedCluster == null ) {
              return;
            }
            isCluster = true;
            path = namedCluster.processURLsubstitution( path, getMetaStore(), transMeta );
          }

          boolean resolvedInitialFile = false;

          if ( path != null ) {
            String fileName = transMeta.environmentSubstitute( path );
            if ( fileName != null && !fileName.equals( "" ) ) {
              try {
                initialFile = KettleVFS.getFileObject( fileName );
                resolvedInitialFile = true;
              } catch ( Exception ex ) {
                showMessageAndLog( BaseMessages.getString( PKG, "HadoopFileInputDialog.Connection.Error.title" ),
                  BaseMessages.getString( PKG, "HadoopFileInputDialog.Connection.error" ), ex.getMessage() );
                return;
              }
              File startFile = new File( System.getProperty( "user.home" ) );
              defaultInitialFile = KettleVFS.getFileObject( startFile.getAbsolutePath() );
              rootFile = initialFile.getFileSystem().getRoot();
            } else {
              defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
            }
          }

          if ( rootFile == null ) {
            if ( defaultInitialFile == null ) {
              return;
            }
            rootFile = defaultInitialFile.getFileSystem().getRoot();
            initialFile = defaultInitialFile;
          }

          VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
          fileChooserDialog.defaultInitialFile = defaultInitialFile;

          NamedClusterWidgetImpl namedClusterWidget = null;

          FileObject selectedFile = null;

          if ( clusterName.equals( LOCAL_ENVIRONMENT ) ) {
            selectedFile =
              fileChooserDialog.open( shell, new String[] { "file" }, "file", true, path, fileFilters,
                fileFilterNames, false, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );
          } else if ( clusterName.equals( S3_ENVIRONMENT ) ) {
            selectedFile =
              fileChooserDialog.open( shell, new String[] { Schemes.S3_SCHEME }, Schemes.S3_SCHEME, true,
                path, fileFilters, fileFilterNames, false, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY,
                false, true );
          } else {
            NamedCluster namedCluster = namedClusterService.getNamedClusterByName( clusterName, getMetaStore() );
            if ( namedCluster != null ) {
              if ( namedCluster.isMapr() ) {
                selectedFile =
                  fileChooserDialog.open( shell, new String[] { Schemes.MAPRFS_SCHEME },
                    Schemes.MAPRFS_SCHEME, true, path, fileFilters, fileFilterNames, true,
                    VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );
              } else {
                List<CustomVfsUiPanel> customPanels = fileChooserDialog.getCustomVfsUiPanels();
                for ( CustomVfsUiPanel panel : customPanels ) {
                  if ( panel instanceof HadoopVfsFileChooserDialog ) {
                    HadoopVfsFileChooserDialog hadoopDialog = ( (HadoopVfsFileChooserDialog) panel );
                    namedClusterWidget = hadoopDialog.getNamedClusterWidget();
                    namedClusterWidget.initiate();
                    hadoopDialog.setNamedCluster( clusterName );
                    hadoopDialog.initializeConnectionPanel( initialFile );
                  }
                }
                if ( resolvedInitialFile ) {
                  fileChooserDialog.initialFile = initialFile;
                }
                selectedFile =
                  fileChooserDialog.open( shell, new String[] { Schemes.HDFS_SCHEME },
                    Schemes.HDFS_SCHEME, false, path, fileFilters, fileFilterNames, true,
                    VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );
              }

            }
          }

          CustomVfsUiPanel currentPanel = fileChooserDialog.getCurrentPanel();
          if ( currentPanel instanceof HadoopVfsFileChooserDialog ) {
            namedClusterWidget = ( (HadoopVfsFileChooserDialog) currentPanel ).getNamedClusterWidget();
          }

          if ( selectedFile != null ) {
            String url = selectedFile.getURL().toString();
            if ( currentPanel != null ) {
              if ( currentPanel.getVfsSchemeDisplayText().equals( LOCAL_ENVIRONMENT ) ) {
                wFilenameList.getActiveTableItem()
                  .setText( wFilenameList.getActiveTableColumn() - 1, LOCAL_ENVIRONMENT );
              } else if ( currentPanel.getVfsSchemeDisplayText().equals( S3_ENVIRONMENT ) ) {
                wFilenameList.getActiveTableItem().setText( wFilenameList.getActiveTableColumn() - 1, S3_ENVIRONMENT );
              } else if ( isCluster ) {
                url = input.getUrlPath( url );
                wFilenameList.getActiveTableItem().setText( wFilenameList.getActiveTableColumn() - 1,
                  clusterName );
              }
            }

            wFilenameList.getActiveTableItem().setText( wFilenameList.getActiveTableColumn(), url );
          }
        } catch ( KettleFileException ex ) {
          log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.KettleFileException" ) );
        } catch ( FileSystemException ex ) {
          log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.FileSystemException" ) );
        }
      }
    };
  }

  protected void setComboValues( ColumnInfo colInfo ) {
    try {
      String[] comboValues = { LOCAL_ENVIRONMENT, STATIC_ENVIRONMENT, S3_ENVIRONMENT };
      String[] namedClusters = namedClusterService.listNames( getMetaStore() ).toArray( new String[ 0 ] );
      String[] values = (String[]) ArrayUtils.addAll( comboValues, namedClusters );
      colInfo.setComboValues( values );
    } catch ( MetaStoreException e ) {
      log.logError( e.getMessage() );
    }
  }

  private InputStream getInputStream( HadoopFileInputMeta meta, FileInputList textFileList ) throws IOException {

    FileObject fileObject = textFileList.getFile( 0 );
    InputStream fileInputStream = KettleVFS.getInputStream( fileObject );
    CompressionProvider provider =
      CompressionProviderFactory.getInstance().createCompressionProviderInstance( meta.content.fileCompression );

    return provider.createInputStream( fileInputStream );
  }

  private InputStreamReader getInputStreamReader( HadoopFileInputMeta meta, InputStream inputStream )
    throws IOException {

    if ( meta.getEncoding() != null && meta.getEncoding().length() > 0 ) {
      return new InputStreamReader( inputStream, meta.getEncoding() );
    }

    return new InputStreamReader( inputStream );
  }

  private BufferedInputStreamReader getBufferedInputStreamReader( HadoopFileInputMeta meta, InputStream inputStream )
    throws IOException {
    return new BufferedInputStreamReader( getInputStreamReader( meta, inputStream ) );
  }

  private void showMessageAndLog( String title, String message, String messageToLog ) {
    MessageBox box = new MessageBox( shell );
    box.setText( title ); //$NON-NLS-1$
    box.setMessage( message );
    log.logError( messageToLog );
    box.open();
  }

  private String getLine( HadoopFileInputMeta meta, FileInputList textFileList )
    throws IOException, KettleFileException {

    InputStream inputStream = null;
    BufferedInputStreamReader reader = null;

    inputStream = getInputStream( meta, textFileList );
    reader = getBufferedInputStreamReader( meta, inputStream );

    EncodingType encodingType = EncodingType.guessEncodingType( reader.getEncoding() );
    StringBuilder lineStringBuilder = new StringBuilder( 256 );
    String enclosure = StringUtil.substituteHex( meta.content.enclosure );
    String sLine =
      TextFileInputUtils.getLine( log, reader, encodingType, meta.getFileFormatTypeNr(), lineStringBuilder, enclosure );
    inputStream.close();

    return sLine;
  }

  private class DirectoryBrowserAdapter extends SelectionAdapter {
    private Text widget;

    /**
     * Create a new Directory Browser Adapter that reads/sets the text of {@code widget} to the directory chosen.
     *
     * @param widget Text widget linked to the VFS browser
     */
    public DirectoryBrowserAdapter( Text widget ) {
      this.widget = widget;
    }

    @Override
    public void widgetSelected( SelectionEvent e ) {
      try {
        // Get current file
        FileObject rootFile = null;
        FileObject initialFile = null;
        FileObject defaultInitialFile = null;

        if ( widget.getText() != null ) {
          String fileName = transMeta.environmentSubstitute( widget.getText() );

          if ( fileName != null && !fileName.equals( "" ) ) {
            initialFile = KettleVFS.getFileObject( fileName );
            rootFile = initialFile.getFileSystem().getRoot();
          } else {
            defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
          }
        }

        defaultInitialFile = KettleVFS.getFileObject( "file:///c:/" );
        if ( rootFile == null ) {
          rootFile = defaultInitialFile.getFileSystem().getRoot();
          initialFile = defaultInitialFile;
        }

        VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
        fileChooserDialog.defaultInitialFile = defaultInitialFile;
        FileObject selectedFile =
          fileChooserDialog.open( shell, null, Schemes.HDFS_SCHEME, false, null, new String[] { "*.*" },
            ALL_FILES_TYPE, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY );
        if ( selectedFile != null ) {
          if ( !selectedFile.getType().equals( FileType.FOLDER ) ) {
            selectedFile = selectedFile.getParent();
          }
          widget.setText( selectedFile.getURL().toString() );
        }
      } catch ( KettleFileException ex ) {
        log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.KettleFileException" ) );
      } catch ( FileSystemException ex ) {
        log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.FileSystemException" ) );
      }
    }
  }
}
