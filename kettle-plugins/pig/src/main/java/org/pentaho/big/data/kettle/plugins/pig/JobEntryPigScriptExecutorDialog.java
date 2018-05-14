/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.pig;

import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.util.HashMap;
import java.util.Map;

/**
 * Job entry dialog for the PigScriptExecutor - job entry that executes a Pig script either on a hadoop cluster or
 * locally.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class JobEntryPigScriptExecutorDialog extends JobEntryDialog implements JobEntryDialogInterface {

  public static final String PIG_FILE_EXT = ".pig";
  private static final Class<?> PKG = JobEntryPigScriptExecutor.class;
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  protected JobEntryPigScriptExecutor m_jobEntry;
  NamedClusterWidgetImpl namedClusterWidgetImpl;
  private Display m_display;
  private boolean m_backupChanged;
  private Text m_wName;
  private TextVar m_pigScriptText;
  private Button m_pigScriptBrowseBut;
  private Button m_enableBlockingBut;
  private Button m_localExecutionBut;
  private TableView m_scriptParams;
  private boolean m_isMapR = false;

  /**
   * Constructor.
   *
   * @param parent      parent shell
   * @param jobEntryInt the job entry that this dialog edits
   * @param rep         a repository
   * @param jobMeta     job meta data
   */
  public JobEntryPigScriptExecutorDialog(
    Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    m_jobEntry = (JobEntryPigScriptExecutor) jobEntryInt;
    namedClusterService = m_jobEntry.getNamedClusterService();
    runtimeTestActionService = m_jobEntry.getRuntimeTestActionService();
    runtimeTester = m_jobEntry.getRuntimeTester();
  }

  public JobEntryInterface open() {

    Shell parent = getParent();
    m_display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, m_jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_jobEntry.setChanged();
      }
    };

    m_backupChanged = m_jobEntry.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( "Pig script executor" );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label nameLineL = new Label( shell, SWT.RIGHT );
    nameLineL.setText( BaseMessages.getString( PKG, "JobEntryDialog.Title" ) );
    props.setLook( nameLineL );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( middle, -margin );
    nameLineL.setLayoutData( fd );

    m_wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_wName );
    m_wName.addModifyListener( lsMod );
    fd = new FormData();
    fd.top = new FormAttachment( 0, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.right = new FormAttachment( 100, 0 );
    m_wName.setLayoutData( fd );

    // named config line
    Label namedClusterLabel = new Label( shell, SWT.RIGHT );
    props.setLook( namedClusterLabel );
    namedClusterLabel.setText( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.NamedCluster.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_wName, 10 );
    fd.right = new FormAttachment( middle, -margin );
    namedClusterLabel.setLayoutData( fd );

    namedClusterWidgetImpl = new NamedClusterWidgetImpl( shell, false, namedClusterService, runtimeTestActionService,
      runtimeTester );
    namedClusterWidgetImpl.initiate();
    props.setLook( namedClusterWidgetImpl );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( m_wName, margin );
    fd.left = new FormAttachment( middle, 0 );
    namedClusterWidgetImpl.setLayoutData( fd );

    // script file line
    Label scriptFileLab = new Label( shell, SWT.RIGHT );
    props.setLook( scriptFileLab );
    scriptFileLab.setText( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.PigScript.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( namedClusterWidgetImpl, margin );
    fd.right = new FormAttachment( middle, -margin );
    scriptFileLab.setLayoutData( fd );

    m_pigScriptBrowseBut = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( m_pigScriptBrowseBut );
    m_pigScriptBrowseBut.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( namedClusterWidgetImpl, 0 );
    m_pigScriptBrowseBut.setLayoutData( fd );
    m_pigScriptBrowseBut.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        openDialog();
      }
    } );

    m_pigScriptText = new TextVar( jobMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( m_pigScriptText );
    m_pigScriptText.addModifyListener( lsMod );
    m_pigScriptText.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        m_pigScriptText.setToolTipText( jobMeta.environmentSubstitute( m_pigScriptText.getText() ) );
      }
    } );
    fd = new FormData();
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( namedClusterWidgetImpl, margin );
    fd.right = new FormAttachment( m_pigScriptBrowseBut, -margin );
    m_pigScriptText.setLayoutData( fd );

    // blocking line
    Label enableBlockingLab = new Label( shell, SWT.RIGHT );
    props.setLook( enableBlockingLab );
    enableBlockingLab.setText( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.EnableBlocking.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_pigScriptText, margin );
    fd.right = new FormAttachment( middle, -margin );
    enableBlockingLab.setLayoutData( fd );

    m_enableBlockingBut = new Button( shell, SWT.CHECK );
    props.setLook( m_enableBlockingBut );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_pigScriptText, margin );
    m_enableBlockingBut.setLayoutData( fd );
    m_enableBlockingBut.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        m_jobEntry.setChanged();
      }
    } );

    // local execution line
    Label localExecutionLab = new Label( shell, SWT.RIGHT );
    props.setLook( localExecutionLab );
    localExecutionLab.setText( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.LocalExecution.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( m_enableBlockingBut, margin );
    fd.right = new FormAttachment( middle, -margin );
    localExecutionLab.setLayoutData( fd );

    m_localExecutionBut = new Button( shell, SWT.CHECK );
    props.setLook( m_localExecutionBut );
    fd = new FormData();
    fd.right = new FormAttachment( 100, 0 );
    fd.left = new FormAttachment( middle, 0 );
    fd.top = new FormAttachment( m_enableBlockingBut, margin );
    m_localExecutionBut.setLayoutData( fd );
    m_localExecutionBut.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        m_jobEntry.setChanged();
        setEnabledStatus();
      }
    } );
    if ( m_isMapR ) {
      m_localExecutionBut.setEnabled( false );
      m_localExecutionBut.setSelection( false );
      m_localExecutionBut.setToolTipText( BaseMessages.getString( PKG,
        "JobEntryPigScriptExecutor.Warning.MapRLocalExecution" ) );
      localExecutionLab.setToolTipText( m_localExecutionBut.getToolTipText() );
    }

    // script parameters -----------------
    Group paramsGroup = new Group( shell, SWT.SHADOW_ETCHED_IN );
    paramsGroup.setText( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.ScriptParameters.Label" ) );
    FormLayout paramsLayout = new FormLayout();
    paramsGroup.setLayout( paramsLayout );
    props.setLook( paramsGroup );

    fd = new FormData();
    fd.top = new FormAttachment( m_localExecutionBut, margin );
    fd.right = new FormAttachment( 100, -margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.bottom = new FormAttachment( 100, -margin * 10 );
    paramsGroup.setLayoutData( fd );

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
            BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.ScriptParameters.ParamterName.Label" ),
            ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( BaseMessages
          .getString( PKG, "JobEntryPigScriptExecutor.ScriptParameters.ParamterValue.Label" ),
            ColumnInfo.COLUMN_TYPE_TEXT, false ) };

    m_scriptParams = new TableView( jobMeta, paramsGroup, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, lsMod, props );

    fd = new FormData();
    fd.top = new FormAttachment( 0, margin );
    fd.right = new FormAttachment( 100, -margin );
    fd.left = new FormAttachment( 0, 0 );
    fd.bottom = new FormAttachment( 100, -margin );
    m_scriptParams.setLayoutData( fd );

    // ---- buttons ------------------------
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, paramsGroup );

    // Add listeners
    Listener lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    Listener lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    m_wName.addSelectionListener( lsDef );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        // cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobTransDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !m_display.readAndDispatch() ) {
        m_display.sleep();
      }
    }

    return m_jobEntry;
  }

  private void cancel() {
    m_jobEntry.setChanged( m_backupChanged );

    m_jobEntry = null;
    dispose();
  }

  /**
   * Dispose this dialog
   */
  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  protected void setEnabledStatus() {
    if ( m_isMapR ) {
      m_localExecutionBut.setEnabled( false );
      m_localExecutionBut.setSelection( false );
    }

    boolean local = m_localExecutionBut.getSelection();

    namedClusterWidgetImpl.setEnabled( !local );
  }

  protected void openDialog() {
    FileDialog openDialog = new FileDialog( shell, SWT.OPEN );
    openDialog.setFilterExtensions( new String[] { "*" + PIG_FILE_EXT, "*" } );
    openDialog.setFilterNames( new String[] { "Pig script files", "All files" } );

    // String prevName = jobMeta.environmentSubstitute(m_pigScriptText.getText());
    String parentFolder = null;

    try {
      parentFolder =
        KettleVFS.getFilename( KettleVFS.getFileObject( jobMeta.environmentSubstitute( jobMeta.getFilename() ) ) );

      if ( !Const.isEmpty( parentFolder ) ) {
        openDialog.setFileName( parentFolder );
      }
    } catch ( Exception ex ) {
      // Ignore for now, should log this!
    }

    if ( openDialog.open() != null ) {
      m_pigScriptText.setText( openDialog.getFilterPath() + System.getProperty( "file.separator" )
        + openDialog.getFileName() );
    }
  }

  protected void getData() {
    m_wName.setText( Const.NVL( m_jobEntry.getName(), "" ) );

    // need setSelectItem
    NamedCluster namedCluster = m_jobEntry.getNamedCluster();
    String namedClusterName = null;
    if ( namedCluster != null ) {
      namedClusterName = namedCluster.getName();
    }
    namedClusterWidgetImpl.setSelectedNamedCluster( namedClusterName == null ? "" : namedClusterName );

    m_pigScriptText.setText( Const.NVL( m_jobEntry.getScriptFilename(), "" ) );
    m_enableBlockingBut.setSelection( m_jobEntry.getEnableBlocking() );
    m_localExecutionBut.setSelection( m_jobEntry.getLocalExecution() );

    Map<String, String> params = m_jobEntry.getScriptParameters();
    if ( params.size() > 0 ) {
      for ( String name : params.keySet() ) {
        String value = params.get( name );
        TableItem item = new TableItem( m_scriptParams.table, SWT.NONE );
        item.setText( 1, name );
        item.setText( 2, value );
      }
    }

    m_scriptParams.removeEmptyRows();
    m_scriptParams.setRowNums();
    m_scriptParams.optWidth( true );

    setEnabledStatus();
  }

  protected void ok() {
    if ( Const.isEmpty( m_wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }

    m_jobEntry.setName( m_wName.getText() );

    NamedCluster nc = namedClusterWidgetImpl.getSelectedNamedCluster();
    if ( nc != null ) {
      m_jobEntry.setNamedCluster( nc );
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "Dialog.Error" ) );
      mb.setMessage( BaseMessages.getString( PKG, "JobEntryPigScriptExecutor.NamedClusterMissing.Msg" ) );
      mb.open();
      return;
    }

    m_jobEntry.setScriptFilename( m_pigScriptText.getText() );
    m_jobEntry.setEnableBlocking( m_enableBlockingBut.getSelection() );
    m_jobEntry.setLocalExecution( m_localExecutionBut.getSelection() );

    int numNonEmpty = m_scriptParams.nrNonEmpty();
    HashMap<String, String> params = new HashMap<String, String>();
    if ( numNonEmpty > 0 ) {
      for ( int i = 0; i < numNonEmpty; i++ ) {
        TableItem item = m_scriptParams.getNonEmpty( i );
        String name = item.getText( 1 ).trim();
        String value = item.getText( 2 ).trim();

        params.put( name, value );
      }
    }

    m_jobEntry.setScriptParameters( params );

    m_jobEntry.setChanged();
    dispose();
  }
}
