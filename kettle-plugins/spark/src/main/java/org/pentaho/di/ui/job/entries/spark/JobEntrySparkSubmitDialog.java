/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.ui.job.entries.spark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.spark.JobEntrySparkSubmit;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.ControlSpaceKeyAdapter;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.core.widget.TextVarButtonRenderCallback;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * Dialog that allows you to enter the settings for a Spark submit job entry.
 *
 * @author Alexander Buloichik
 * @author jdixon
 * @since Dec-4-2014
 */
public class JobEntrySparkSubmitDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntrySparkSubmit.class; // for i18n purposes, needed by Translator2!!
  private static final int SHELL_MINIMUM_WIDTH = 400;

  private static final String[] FILEFORMATS =
      new String[] { BaseMessages.getString( PKG, "JobEntrySparkSubmit.Fileformat.All" ) };

  private static final String[] MASTER_URLS = new String[] { "yarn-cluster", "yarn-client" };

  public static final String LOCAL_ENVIRONMENT = "Local";
  public static final String STATIC_ENVIRONMENT = "<Static>";

  protected static final String[] FILETYPES =
      new String[] { BaseMessages.getString( PKG, "JobEntrySparkSubmit.Fileformat.All" ) };

  protected Shell shell;
  private Text txtEntryName;
  private TextVar txtSparkSubmitUtility;
  private Button btnSparkSubmitUtility;
  private Button btnOK;
  private Button btnCancel;
  private TextVar txtClass;
  private TextVar txtFilesApplicationJar;
  private Text txtArguments;
  private Button btnFilesApplicationJar;
  private TextVar txtFilesPyFile;
  private Button btnFilesPyFile;
  private TableView tblFilesSupportingDocs;
  private TableView tblUtilityParameters;
  private Combo cmbType;
  private ComboVar cmbMasterURL;
  private Composite filesHeader;
  private Composite tabFilesComposite;
  private TextVar txtExecutorMemory;
  private TextVar txtDriverMemory;
  private Button chkEnableBlocking;

  private JobEntrySparkSubmit jobEntry;
  private boolean backupChanged;

  public static void main( String[] a ) {
    Display display = new Display();
    PropsUI.init( display, Props.TYPE_PROPERTIES_SPOON );
    Shell shell = new Shell( display );

    JobEntrySparkSubmitDialog sh =
        new JobEntrySparkSubmitDialog( shell, new JobEntrySparkSubmit( "Spark submit job entry" ), null,
            new JobMeta() );

    sh.open();
  }

  public JobEntrySparkSubmitDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (JobEntrySparkSubmit) jobEntryInt;
  }

  @Override
  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );

    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    backupChanged = jobEntry.hasChanged();

    createContents();

    getData();
    BaseStepDialog.setSize( shell );

    shell.pack();
    shell.setMinimumSize( SHELL_MINIMUM_WIDTH, shell.getSize().y );
    shell.setSize( 530, 652 );
    shell.open();
    props.setDialogSize( shell, "JobEntrySparkSubmitDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  /**
   * Create contents of the shell.
   */
  private void createContents() {
    shell.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Title" ) );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    shell.setLayout( formLayout );

    // shell = new Shell( getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.TITLE | SWT.APPLICATION_MODAL );
    // shell.setSize( 621, 557 );

    Label lblIcon = new Label( shell, SWT.NONE );
    props.setLook( lblIcon );
    lblIcon.setImage( GUIResource.getInstance().getImage( "org/pentaho/di/ui/job/entries/spark/img/spark.svg",
        getClass().getClassLoader(), ConstUI.LARGE_ICON_SIZE, ConstUI.LARGE_ICON_SIZE ) );
    lblIcon.setLayoutData( fd( null, fa( 0, 0 ), fa( 100, 0 ) ) );

    Label lblEntryName = new Label( shell, SWT.NONE );
    props.setLook( lblEntryName );
    lblEntryName.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Name.Label" ) );
    lblEntryName.setLayoutData( fd() );

    txtEntryName = new Text( shell, SWT.BORDER );
    props.setLook( txtEntryName );
    txtEntryName.setLayoutData( fdwidth( 300, null, fa( lblEntryName, 5 ) ) );
    txtEntryName.addModifyListener( lsMod );

    Label sep = new Label( shell, SWT.SEPARATOR | SWT.HORIZONTAL );
    props.setLook( sep );
    sep.setLayoutData( fd( fa( 0, 0 ), fa( txtEntryName, 15 ), fa( 100, 0 ) ) );

    Label lblSparkSubmitUtility = new Label( shell, SWT.NONE );
    props.setLook( lblSparkSubmitUtility );
    lblSparkSubmitUtility.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.ScriptPath.Label" ) );
    lblSparkSubmitUtility.setLayoutData( fd( null, fa( sep, 15 ) ) );

    txtSparkSubmitUtility = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( txtSparkSubmitUtility );
    txtSparkSubmitUtility.setLayoutData( fdwidth( 300, null, fa( lblSparkSubmitUtility, 5 ) ) );

    btnSparkSubmitUtility = new Button( shell, SWT.PUSH );
    props.setLook( btnSparkSubmitUtility );
    btnSparkSubmitUtility.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    btnSparkSubmitUtility.setLayoutData( fd( fa( txtSparkSubmitUtility, 10 ), fa( txtSparkSubmitUtility, 0, SWT.TOP ),
        null ) );
    btnSparkSubmitUtility.addSelectionListener( btnSparkSubmitUtilityListener );

    Label lblMasterURL = new Label( shell, SWT.NONE );
    props.setLook( lblMasterURL );
    lblMasterURL.setLayoutData( fd( null, fa( txtSparkSubmitUtility, 10 ) ) );
    lblMasterURL.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.SparkMaster.Label" ) );

    cmbMasterURL = new ComboVar( jobMeta, shell, SWT.BORDER );
    props.setLook( cmbMasterURL );
    cmbMasterURL.setLayoutData( fd( fa( 0, 0 ), fa( lblMasterURL, 5 ), fa( txtSparkSubmitUtility, 0, SWT.RIGHT ) ) );

    Label lblType = new Label( shell, SWT.NONE );
    props.setLook( lblType );
    lblType.setLayoutData( fd( fa( cmbMasterURL, 10 ), fa( txtSparkSubmitUtility, 10 ) ) );
    lblType.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Type.Label" ) );

    cmbType = new Combo( shell, SWT.NONE | SWT.READ_ONLY );
    props.setLook( cmbType );
    cmbType.setLayoutData( fdwidth( 300, fa( cmbMasterURL, 10 ), fa( lblType, 5 ), fa( 100, 0 ) ) );
    cmbType.addSelectionListener( typeSelectionListener );

    btnCancel = new Button( shell, SWT.PUSH );
    props.setLook( btnCancel );
    btnCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    btnCancel.setLayoutData( fd( null, null, fa( 100, 0 ), fa( 100, 0 ) ) );

    btnOK = new Button( shell, SWT.PUSH );
    props.setLook( btnOK );
    btnOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    btnOK.setLayoutData( fd( null, null, fa( btnCancel, -5 ), fa( 100, 0 ) ) );

    Label sep2 = new Label( shell, SWT.SEPARATOR | SWT.HORIZONTAL );
    props.setLook( sep2 );
    sep2.setLayoutData( fd( fa( 0, 0 ), null, fa( 100, 0 ), fa( btnOK, -15 ) ) );

    chkEnableBlocking = new Button( shell, SWT.CHECK );
    props.setLook( chkEnableBlocking );
    chkEnableBlocking.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.BlockExecution.Label" ) );
    chkEnableBlocking.setLayoutData( fd( fa( 0, 0 ), null, null, fa( sep2, -15 ) ) );

    CTabFolder tabs = new CTabFolder( shell, SWT.BORDER );
    props.setLook( tabs, Props.WIDGET_STYLE_TAB );
    props.setLook( tabs );
    tabs.setLayoutData( fd( fa( 0, 0 ), fa( cmbMasterURL, 15 ), fa( 100, 0 ), fa( chkEnableBlocking, -15 ) ) );

    CTabItem tabFiles = new CTabItem( tabs, SWT.NONE );
    tabFiles.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Files.TabLabel" ) );
    tabFilesComposite = new Composite( tabs, SWT.NONE );
    props.setLook( tabFilesComposite );
    tabFiles.setControl( tabFilesComposite );
    addOnFilesTab( tabFilesComposite );

    CTabItem tabArguments = new CTabItem( tabs, SWT.NONE );
    tabArguments.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Arguments.TabLabel" ) );
    Composite tabArgumentsComposite = new Composite( tabs, SWT.NONE );
    props.setLook( tabArgumentsComposite );
    tabArguments.setControl( tabArgumentsComposite );
    addOnArgumentsTab( tabArgumentsComposite );

    CTabItem tabOptions = new CTabItem( tabs, SWT.NONE );
    tabOptions.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Options.TabLabel" ) );
    Composite tabOptionsComposite = new Composite( tabs, SWT.NONE );
    props.setLook( tabOptionsComposite );
    tabOptions.setControl( tabOptionsComposite );
    addOnOptionsTab( tabOptionsComposite );

    tabs.setSelection( tabFiles );

    typeSelectionListener.widgetSelected( null );

    btnOK.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    } );
    btnCancel.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    } );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );
  }

  private void addOnArgumentsTab( Composite tab ) {
    tab.setLayout( new FormLayout() );

    Label lblArguments = new Label( tab, SWT.NONE );
    props.setLook( lblArguments );
    lblArguments.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Args.Label" ) );
    lblArguments.setLayoutData( fd( fa( 0, 15 ), fa( 0, 15 ) ) );

    txtArguments = new Text( tab, SWT.BORDER | SWT.MULTI | SWT.WRAP );
    props.setLook( txtArguments );
    txtArguments.setLayoutData( fd( fa( 0, 15 ), fa( lblArguments, 5 ), fa( 100, -15 ), fa( 100, -15 ) ) );
    ControlSpaceKeyAdapter controlSpaceKeyAdapter = new ControlSpaceKeyAdapter( jobMeta, txtArguments, null, null );
    txtArguments.addKeyListener( controlSpaceKeyAdapter );

    Label txtArgumentsVar = new Label( tab, SWT.NONE );
    txtArgumentsVar.setImage( GUIResource.getInstance().getImageVariable() );
    txtArgumentsVar.setToolTipText( BaseMessages.getString( TextVar.class, "TextVar.tooltip.InsertVariable" ) );
    props.setLook( txtArgumentsVar );
    FormData fdArgumentsVar = new FormData();
    fdArgumentsVar.right = new FormAttachment( 100, -15 );
    fdArgumentsVar.bottom = new FormAttachment( txtArguments, 0, SWT.TOP );
    txtArgumentsVar.setLayoutData( fdArgumentsVar );
  }

  private void addOnOptionsTab( Composite tab ) {
    tab.setLayout( new FormLayout() );

    Label lblExecutorMemory = new Label( tab, SWT.NONE );
    props.setLook( lblExecutorMemory );
    lblExecutorMemory.setLayoutData( fd( fa( 0, 15 ), fa( 0, 15 ) ) );
    lblExecutorMemory.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.MemoryAllocation.Executor.Label" ) );

    txtExecutorMemory = new TextVar( jobMeta, tab, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( txtExecutorMemory );
    txtExecutorMemory.setLayoutData( fdwidth( 200, fa( 0, 15 ), fa( lblExecutorMemory, 5 ) ) );

    Label lblDriverMemory = new Label( tab, SWT.NONE );
    props.setLook( lblDriverMemory );
    lblDriverMemory.setLayoutData( fd( fa( txtExecutorMemory, 50 ), fa( 0, 15 ) ) );
    lblDriverMemory.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.MemoryAllocation.Driver.Label" ) );

    txtDriverMemory = new TextVar( jobMeta, tab, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( txtDriverMemory );
    txtDriverMemory.setLayoutData( fdwidth( 200, fa( txtExecutorMemory, 50 ), fa( lblDriverMemory, 5 ) ) );

    Label lblUtilityParameters = new Label( tab, SWT.NONE );
    props.setLook( lblUtilityParameters );
    lblUtilityParameters.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.UtilityParameters.Label" ) );
    lblUtilityParameters.setLayoutData( fd( fa( 0, 15 ), fa( txtExecutorMemory, 10 ) ) );

    ColumnInfo[] columns =
        new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "JobEntrySparkSubmit.NameColumn.Label" ),
            ColumnInfo.COLUMN_TYPE_TEXT ), new ColumnInfo( BaseMessages.getString( PKG,
                "JobEntrySparkSubmit.ValueColumn.Label" ), ColumnInfo.COLUMN_TYPE_TEXT ) };

    tblUtilityParameters =
        new TableView( jobEntry, tab, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE, columns, jobEntry.getConfigParams()
            .size(), false, null, props, false );
    props.setLook( tblUtilityParameters );
    tblUtilityParameters.setLayoutData( fd( fa( 0, 15 ), fa( lblUtilityParameters, 5 ), fa( 100, -15 ), fa( 100,
        -15 ) ) );
    tblUtilityParameters.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 50, 50 ) );
  }

  private void addOnFilesTab( Composite tab ) {
    tab.setLayout( new FormLayout() );

    filesHeader = new Composite( tab, SWT.NONE );
    props.setLook( filesHeader );
    filesHeader.setLayoutData( fd( fa( 0, 15 ), fa( 0, 15 ), fa( 100, -15 ) ) );

    if ( JobEntrySparkSubmit.JOB_TYPE_PYTHON.equals( jobEntry.getJobType() ) ) {
      addOnFilesTabPython( filesHeader );
    } else {
      addOnFilesTabJavaScala( filesHeader );
    }

    Label lblSupportingDocs = new Label( tab, SWT.NONE );
    props.setLook( lblSupportingDocs );
    lblSupportingDocs.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.SupportingDocuments.Label" ) );
    lblSupportingDocs.setLayoutData( fd( fa( 0, 15 ), fa( filesHeader, 10 ) ) );

    ColumnInfo[] columns =
        new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "JobEntrySparkSubmit.EnvironmentColumn.Label" ),
            ColumnInfo.COLUMN_TYPE_CCOMBO ), new ColumnInfo( BaseMessages.getString( PKG,
                "JobEntrySparkSubmit.PathColumn.Label" ), ColumnInfo.COLUMN_TYPE_TEXT_BUTTON ) };
    columns[0].setComboValues( new String[] { LOCAL_ENVIRONMENT, STATIC_ENVIRONMENT } );
    columns[0].setReadOnly( true );
    columns[1].setUsingVariables( true );
    columns[1].setTextVarButtonSelectionListener( pathSelection );

    TextVarButtonRenderCallback callback = new TextVarButtonRenderCallback() {
      public boolean shouldRenderButton() {
        String
            envType =
            tblFilesSupportingDocs.getActiveTableItem().getText( tblFilesSupportingDocs.getActiveTableColumn() - 1 );
        return !STATIC_ENVIRONMENT.equalsIgnoreCase( envType );
      }
    };

    columns[1].setRenderTextVarButtonCallback( callback );

    tblFilesSupportingDocs =
        new TableView( jobEntry, tab, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE, columns, jobEntry
            .getLibs().size(), false, null, props, false );
    props.setLook( tblFilesSupportingDocs );
    tblFilesSupportingDocs.setLayoutData( fd( fa( 0, 15 ), fa( lblSupportingDocs, 5 ), fa( 100, -15 ), fa( 100,
        -15 ) ) );
    tblFilesSupportingDocs.getTable().addListener( SWT.Resize, new ColumnsResizer( 0, 25, 75 ) );
  }

  private void addOnFilesTabJavaScala( Composite panel ) {
    panel.setLayout( new FormLayout() );

    Label lblClass = new Label( panel, SWT.NONE );
    props.setLook( lblClass );
    lblClass.setLayoutData( fd( fa( 0, 0 ), fa( 0, 0 ) ) );
    lblClass.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Class.Label" ) );

    txtClass = new TextVar( jobMeta, panel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( txtClass );
    txtClass.setLayoutData( fdwidth( 300, fa( 0, 0 ), fa( lblClass, 5 ) ) );
    txtClass.setText( Const.nullToEmpty( jobEntry.getClassName() ) );

    Label lblApplicationJar = new Label( panel, SWT.NONE );
    props.setLook( lblApplicationJar );
    lblApplicationJar.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Jar.Label" ) );
    lblApplicationJar.setLayoutData( fd( fa( 0, 0 ), fa( txtClass, 10 ) ) );

    txtFilesApplicationJar = new TextVar( jobMeta, panel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( txtFilesApplicationJar );
    txtFilesApplicationJar.setLayoutData( fdwidth( 300, fa( 0, 0 ), fa( lblApplicationJar, 5 ) ) );
    txtFilesApplicationJar.setText( Const.nullToEmpty( jobEntry.getJar() ) );

    btnFilesApplicationJar = new Button( panel, SWT.PUSH );
    props.setLook( btnFilesApplicationJar );
    btnFilesApplicationJar.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    btnFilesApplicationJar.setLayoutData( fd( fa( txtFilesApplicationJar, 10 ), fa( txtFilesApplicationJar, 0,
        SWT.TOP ), null ) );
    btnFilesApplicationJar.addSelectionListener( btnFilesApplicationJarListener );
  }

  private void addOnFilesTabPython( Composite panel ) {
    panel.setLayout( new FormLayout() );

    Label lblPyFile = new Label( panel, SWT.NONE );
    props.setLook( lblPyFile );
    lblPyFile.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.PyFile.Label" ) );
    lblPyFile.setLayoutData( fd( fa( 0, 0 ), fa( 0, 0 ) ) );

    txtFilesPyFile = new TextVar( jobMeta, panel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( txtFilesPyFile );
    txtFilesPyFile.setLayoutData( fdwidth( 300, fa( 0, 0 ), fa( lblPyFile, 5 ) ) );
    txtFilesPyFile.setText( Const.nullToEmpty( jobEntry.getPyFile() ) );

    btnFilesPyFile = new Button( panel, SWT.PUSH );
    props.setLook( btnFilesPyFile );
    btnFilesPyFile.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    btnFilesPyFile.setLayoutData( fd( fa( txtFilesPyFile, 10 ), fa( txtFilesPyFile, 0, SWT.TOP ), null ) );
    btnFilesPyFile.addSelectionListener( btnFilesPyFileListener );
  }

  private SelectionAdapter btnSparkSubmitUtilityListener = new SelectionAdapter() {
    public void widgetSelected( SelectionEvent e ) {
      FileDialog dialog = new FileDialog( shell, SWT.OPEN );
      dialog.setFilterExtensions( new String[] { "*;*.*" } );
      dialog.setFilterNames( FILEFORMATS );

      if ( txtSparkSubmitUtility.getText() != null ) {
        dialog.setFileName( txtSparkSubmitUtility.getText() );
      }

      if ( dialog.open() != null ) {
        txtSparkSubmitUtility.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
      }
    }
  };

  private ModifyListener lsMod = new ModifyListener() {
    public void modifyText( ModifyEvent e ) {
      jobEntry.setChanged();
    }
  };

  SelectionAdapter pathSelection = new SelectionAdapter() {
    public void widgetSelected( SelectionEvent e ) {

      FileObject selectedFile = null;

      try {
        // Get current file
        FileObject rootFile = null;
        FileObject initialFile = null;
        FileObject defaultInitialFile = null;

        String original =
            tblFilesSupportingDocs.getActiveTableItem().getText( tblFilesSupportingDocs.getActiveTableColumn() );

        if ( original != null ) {

          String fileName = jobMeta.environmentSubstitute( original );

          if ( fileName != null && !fileName.equals( "" ) ) {
            try {
              initialFile = KettleVFS.getFileObject( fileName );
            } catch ( KettleException ex ) {
              initialFile = KettleVFS.getFileObject( "" );
            }
            defaultInitialFile = KettleVFS.getFileObject( "file:///c:/" );
            rootFile = initialFile.getFileSystem().getRoot();
          } else {
            defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
          }
        }

        if ( rootFile == null ) {
          rootFile = defaultInitialFile.getFileSystem().getRoot();
          initialFile = defaultInitialFile;
        }
        VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
        fileChooserDialog.defaultInitialFile = defaultInitialFile;

        selectedFile =
            fileChooserDialog.open( shell, new String[] { "file" }, "file", true, null, new String[] { "*.*" },
                FILETYPES, true, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );

        if ( selectedFile != null ) {
          String url = selectedFile.getURL().toString();
          tblFilesSupportingDocs.getActiveTableItem().setText( tblFilesSupportingDocs.getActiveTableColumn(), url );
        }

      } catch ( KettleFileException ex ) {
        // Ignore
      } catch ( FileSystemException ex ) {
        // Ignore
      }
    }
  };

  private SelectionAdapter btnFilesApplicationJarListener = new SelectionAdapter() {
    public void widgetSelected( SelectionEvent e ) {
      FileDialog dialog = new FileDialog( shell, SWT.OPEN );
      dialog.setFilterExtensions( new String[] { "*;*.*" } );
      dialog.setFilterNames( FILEFORMATS );

      if ( txtFilesApplicationJar.getText() != null ) {
        dialog.setFileName( txtFilesApplicationJar.getText() );
      }

      if ( dialog.open() != null ) {
        txtFilesApplicationJar.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
      }
    }
  };

  private SelectionAdapter btnFilesPyFileListener = new SelectionAdapter() {
    public void widgetSelected( SelectionEvent e ) {
      FileDialog dialog = new FileDialog( shell, SWT.OPEN );
      dialog.setFilterExtensions( new String[] { "*;*.*" } );
      dialog.setFilterNames( FILEFORMATS );

      if ( txtFilesPyFile.getText() != null ) {
        dialog.setFileName( txtFilesPyFile.getText() );
      }

      if ( dialog.open() != null ) {
        txtFilesPyFile.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
      }
    }
  };

  private SelectionAdapter typeSelectionListener = new SelectionAdapter() {
    @Override
    public void widgetSelected( SelectionEvent event ) {
      String text = cmbType.getText();

      if ( Const.isEmpty( text ) ) {
        return;
      }

      for ( Control c : filesHeader.getChildren() ) {
        c.dispose();
      }
      if ( JobEntrySparkSubmit.JOB_TYPE_PYTHON.equals( text ) ) {
        addOnFilesTabPython( filesHeader );
        jobEntry.setPyFile( txtFilesPyFile.getText() );
        jobEntry.setJobType( JobEntrySparkSubmit.JOB_TYPE_PYTHON );
      } else {
        addOnFilesTabJavaScala( filesHeader );
        jobEntry.setJar( txtFilesApplicationJar.getText() );
        jobEntry.setClassName( txtClass.getText() );
        jobEntry.setJobType( JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA );
      }
      tabFilesComposite.layout( true );
      filesHeader.layout( true );
    }
  };

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  private void cancel() {
    jobEntry.setChanged( backupChanged );
    jobEntry = null;
    dispose();
  }

  public void getData() {
    txtEntryName.setText( Const.nullToEmpty( jobEntry.getName() ) );
    txtSparkSubmitUtility.setText( Const.nullToEmpty( jobEntry.getScriptPath() ) );

    for ( String url : MASTER_URLS ) {
      cmbMasterURL.add( url );
    }
    cmbMasterURL.setText( Const.nullToEmpty( jobEntry.getMaster() ) );

    cmbType.add( JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA );
    cmbType.add( JobEntrySparkSubmit.JOB_TYPE_PYTHON );

    if ( JobEntrySparkSubmit.JOB_TYPE_PYTHON.equals( jobEntry.getJobType() ) ) {
      cmbType.select( 1 );
    } else {
      cmbType.select( 0 );
    }

    txtArguments.setText( Const.nullToEmpty( jobEntry.getArgs() ) );
    chkEnableBlocking.setSelection( jobEntry.isBlockExecution() );

    List<String> params = jobEntry.getConfigParams();
    for ( int i = 0; i < params.size(); i++ ) {
      TableItem ti = tblUtilityParameters.table.getItem( i );
      String[] nameValue = params.get( i ).split( "=", 2 );
      ti.setText( 1, nameValue[0] );
      ti.setText( 2, nameValue[1] );
    }
    tblUtilityParameters.setRowNums();
    tblUtilityParameters.optWidth( true );

    Map<String, String> docs = jobEntry.getLibs();

    int i = 0;
    for ( String path : docs.keySet() ) {
      TableItem ti = tblFilesSupportingDocs.table.getItem( i++ );
      ti.setText( 1, docs.get( path ) );
      ti.setText( 2, path );
    }

    tblFilesSupportingDocs.setRowNums();
    tblFilesSupportingDocs.optWidth( true );

    txtExecutorMemory.setText( Const.nullToEmpty( jobEntry.getExecutorMemory() ) );
    txtDriverMemory.setText( Const.nullToEmpty( jobEntry.getDriverMemory() ) );

    txtEntryName.selectAll();
    txtEntryName.setFocus();
  }

  protected void ok() {
    if ( Const.isEmpty( txtEntryName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( txtEntryName.getText() );
    jobEntry.setScriptPath( txtSparkSubmitUtility.getText() );
    jobEntry.setMaster( cmbMasterURL.getText() );
    switch ( jobEntry.getJobType() ) {
      case JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA: {
        jobEntry.setJar( txtFilesApplicationJar.getText() );
        jobEntry.setClassName( txtClass.getText() );
        jobEntry.setPyFile( null );

        break;
      }
      case JobEntrySparkSubmit.JOB_TYPE_PYTHON: {
        jobEntry.setPyFile( txtFilesPyFile.getText() );
        jobEntry.setJar( null );
        jobEntry.setClassName( null );

        break;
      }
    }
    jobEntry.setArgs( txtArguments.getText() );
    jobEntry.setBlockExecution( chkEnableBlocking.getSelection() );

    List<String> configParams = new ArrayList<String>( this.tblUtilityParameters.getItemCount() );
    for ( int i = 0; i < this.tblUtilityParameters.getItemCount(); i++ ) {
      String[] item = this.tblUtilityParameters.getItem( i );
      if ( !Const.isEmpty( item[0] ) && !Const.isEmpty( item[1] ) ) {
        configParams.add( item[0].trim() + "=" + item[1].trim() );
      }
    }
    jobEntry.setConfigParams( configParams );

    Map<String, String> supportingDocuments = new LinkedHashMap<>();
    for ( int i = 0; i < this.tblFilesSupportingDocs.getItemCount(); i++ ) {
      String[] item = this.tblFilesSupportingDocs.getItem( i );
      if ( !Const.isEmpty( item[0] ) && !Const.isEmpty( item[1] ) ) {
        supportingDocuments.put( item[1].trim(), item[0].trim() );
      }
    }
    jobEntry.setLibs( supportingDocuments );

    jobEntry.setDriverMemory( txtDriverMemory.getText() );
    jobEntry.setExecutorMemory( txtExecutorMemory.getText() );

    dispose();
  }

  private FormData fd( FormAttachment... att ) {
    FormData fd = new FormData();
    if ( att.length >= 1 ) {
      fd.left = att[0];
    }
    if ( att.length >= 2 ) {
      fd.top = att[1];
    }
    if ( att.length >= 3 ) {
      fd.right = att[2];
    }
    if ( att.length >= 4 ) {
      fd.bottom = att[3];
    }
    return fd;
  }

  private FormData fdwidth( int width, FormAttachment... att ) {
    FormData fd = fd( att );
    fd.width = width;
    return fd;
  }

  private FormAttachment fa( int numerator, int offset ) {
    return new FormAttachment( numerator, offset );
  }

  private FormAttachment fa( Control control, int offset ) {
    return new FormAttachment( control, offset );
  }

  private FormAttachment fa( Control control, int offset, int alignment ) {
    return new FormAttachment( control, offset, alignment );
  }

  public class ColumnsResizer implements Listener {
    private int[] weights;

    public ColumnsResizer( int... weights ) {
      this.weights = weights;
    }

    @Override
    public void handleEvent( Event event ) {
      Table table = (Table) event.widget;
      float width = table.getSize().x - 2;
      TableColumn[] columns = table.getColumns();

      int f = 0;
      for ( int w : weights ) {
        f += w;
      }
      for ( int i = 0; i < weights.length; i++ ) {
        int cw = weights[i] == 0 ? 0 : Math.round( width / f * weights[i] );
        width -= cw + 1;
        columns[i].setWidth( cw );
        f -= weights[i];
      }
    }
  }
}
