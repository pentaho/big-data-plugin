/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
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
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.spark.JobEntrySparkSubmit;
import org.pentaho.di.job.entry.JobEntryDialogInterface;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.ComboVar;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.job.dialog.JobDialog;
import org.pentaho.di.ui.job.entry.JobEntryDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog that allows you to enter the settings for a Spark submit job entry.
 *
 * @author jdixon
 * @since Dec-4-2014
 */
public class JobEntrySparkSubmitDialog extends JobEntryDialog implements JobEntryDialogInterface {
  private static Class<?> PKG = JobEntrySparkSubmit.class; // for i18n purposes, needed by Translator2!!
  private static final int SHELL_MINIMUM_WIDTH = 400;
  private static final int MARGIN_LARGE = 15;
  private static final int MARGIN_MEDIUM = 10;
  private static final int MARGIN_SMALL = 5;

  private static final String[] FILEFORMATS = new String[] { BaseMessages.getString( PKG,
      "JobEntrySparkSubmit.Fileformat.All" ) };

  private static final String[] MASTER_URLS = new String[] { "yarn-cluster", "yarn-client" };

  private Shell shell;

  private JobEntrySparkSubmit jobEntry;
  private boolean backupChanged;

  private Text name;
  private TableView configParams;
  private TextVar sparkSubmit;
  private ComboVar masterUrl;
  private TextVar clazz;
  private TextVar jar;
  private TextVar args;
  private TextVar driverMemory;
  private TextVar executorMemory;
  private Button blockExecution;

  public JobEntrySparkSubmitDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (JobEntrySparkSubmit) jobEntryInt;
  }

  public JobEntryInterface open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, props.getJobsDialogStyle() );
    props.setLook( shell );
    JobDialog.setShellImage( shell, jobEntry );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        jobEntry.setChanged();
      }
    };
    backupChanged = jobEntry.hasChanged();

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = MARGIN_LARGE;
    formLayout.marginHeight = MARGIN_LARGE;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Title" ) );

    // Job entry name
    Label nameLabel = new Label( shell, SWT.RIGHT );
    props.setLook( nameLabel );
    nameLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    nameLabel.setLayoutData( fdlName );
    name = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( name );
    name.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( nameLabel, MARGIN_SMALL );
    fdName.left = new FormAttachment( nameLabel, 0, SWT.LEFT );
    fdName.right = new FormAttachment( 70, 0 );
    name.setLayoutData( fdName );

    // Job entry icon
    Label stepIcon = new Label( shell, SWT.NONE );
    props.setLook( stepIcon );
    stepIcon.setImage( GUIResource.getInstance().getImage( "org/pentaho/di/ui/job/entries/spark/img/spark.svg",
        getClass().getClassLoader(), ConstUI.ICON_SIZE, ConstUI.ICON_SIZE ) );

    FormData fdIcon = new FormData();
    fdIcon.right = new FormAttachment( 100 );
    fdIcon.top = new FormAttachment( nameLabel, 0, SWT.TOP );
    stepIcon.setLayoutData( fdIcon );

    Label topSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( topSeparator );
    FormData fdTopSeparator = new FormData();
    fdTopSeparator.top = new FormAttachment( name, MARGIN_LARGE );
    fdTopSeparator.left = new FormAttachment( 0 );
    fdTopSeparator.right = new FormAttachment( 100 );
    topSeparator.setLayoutData( fdTopSeparator );

    // Ok and cancel buttons
    Button wCancel = new Button( shell, SWT.PUSH );
    props.setLook( wCancel );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    FormData fdCancel = new FormData();
    fdCancel.right = new FormAttachment( 100 );
    fdCancel.bottom = new FormAttachment( 100 );
    wCancel.setLayoutData( fdCancel );

    Button wOK = new Button( shell, SWT.PUSH );
    props.setLook( wOK );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fdOk = new FormData();
    fdOk.right = new FormAttachment( wCancel, -MARGIN_SMALL );
    fdOk.bottom = new FormAttachment( wCancel, 0, SWT.BOTTOM );
    wOK.setLayoutData( fdOk );

    Label bottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( bottomSeparator );
    FormData fdBottomSeparator = new FormData();
    fdBottomSeparator.bottom = new FormAttachment( wCancel, -MARGIN_LARGE );
    fdBottomSeparator.left = new FormAttachment( 0 );
    fdBottomSeparator.right = new FormAttachment( 100 );
    bottomSeparator.setLayoutData( fdBottomSeparator );

    // Job config and parameters tabs
    CTabFolder tabs = new CTabFolder( shell, SWT.BORDER );
    props.setLook( tabs, Props.WIDGET_STYLE_TAB );
    FormData fdTabs = new FormData();
    fdTabs.left = new FormAttachment( 0 );
    fdTabs.right = new FormAttachment( 100 );
    fdTabs.top = new FormAttachment( topSeparator, MARGIN_LARGE );
    fdTabs.bottom = new FormAttachment( bottomSeparator, -MARGIN_LARGE, SWT.TOP );
    tabs.setLayoutData( fdTabs );

    CTabItem jobConfigTab = new CTabItem( tabs, SWT.NONE );
    jobConfigTab.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.JobSetupTab.Label" ) );

    Composite jobConfigTabComposite = new Composite( tabs, SWT.NONE );
    props.setLook( jobConfigTabComposite );
    jobConfigTab.setControl( jobConfigTabComposite );
    FormLayout jobConfigCompositeLayout = new FormLayout();
    jobConfigCompositeLayout.marginHeight = MARGIN_LARGE;
    jobConfigCompositeLayout.marginWidth = MARGIN_LARGE;
    jobConfigTabComposite.setLayout( jobConfigCompositeLayout );

    // Spark-submit path
    Label sparkSubmitLabel = new Label( jobConfigTabComposite, SWT.RIGHT );
    sparkSubmitLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.ScriptPath.Label" ) );
    props.setLook( sparkSubmitLabel );
    FormData fdSparkSubmitLabel = new FormData();
    fdSparkSubmitLabel.left = new FormAttachment( 0 );
    fdSparkSubmitLabel.top = new FormAttachment( 0 );
    sparkSubmitLabel.setLayoutData( fdSparkSubmitLabel );

    sparkSubmit = new TextVar( jobMeta, jobConfigTabComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( sparkSubmit );
    sparkSubmit.addModifyListener( lsMod );
    sparkSubmit.addSelectionListener( lsDef );

    Button browseSparkSubmit = new Button( jobConfigTabComposite, SWT.PUSH );
    props.setLook( browseSparkSubmit );
    browseSparkSubmit.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdBrowseSparkSubmit = new FormData();
    fdBrowseSparkSubmit.top = new FormAttachment( sparkSubmitLabel, MARGIN_SMALL );
    fdBrowseSparkSubmit.right = new FormAttachment( 100, 0 );
    browseSparkSubmit.setLayoutData( fdBrowseSparkSubmit );

    FormData fdSparkSubmit = new FormData();
    fdSparkSubmit.left = new FormAttachment( 0 );
    fdSparkSubmit.right = new FormAttachment( browseSparkSubmit, -MARGIN_SMALL );
    fdSparkSubmit.top = new FormAttachment( sparkSubmitLabel, MARGIN_SMALL );
    sparkSubmit.setLayoutData( fdSparkSubmit );

    browseSparkSubmit.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*;*.*" } );
        dialog.setFilterNames( FILEFORMATS );

        if ( sparkSubmit.getText() != null ) {
          dialog.setFileName( sparkSubmit.getText() );
        }

        if ( dialog.open() != null ) {
          sparkSubmit.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    // Spark master
    Label masterUrlLabel = new Label( jobConfigTabComposite, SWT.NONE );
    props.setLook( masterUrlLabel );
    masterUrlLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.SparkMaster.Label" ) );
    FormData fdMasterUrlLabel = new FormData();
    fdMasterUrlLabel.left = new FormAttachment( 0 );
    fdMasterUrlLabel.top = new FormAttachment( sparkSubmit, MARGIN_MEDIUM );
    masterUrlLabel.setLayoutData( fdMasterUrlLabel );

    masterUrl = new ComboVar( jobMeta, jobConfigTabComposite, SWT.BORDER );
    props.setLook( masterUrl );
    masterUrl.addModifyListener( lsMod );

    FormData fdMasterUrl = new FormData();
    fdMasterUrl.left = new FormAttachment( 0 );
    fdMasterUrl.right = new FormAttachment( 100, 0 );
    fdMasterUrl.top = new FormAttachment( masterUrlLabel, MARGIN_SMALL );
    masterUrl.setLayoutData( fdMasterUrl );

    // Jar
    Label jarLabel = new Label( jobConfigTabComposite, SWT.RIGHT );
    jarLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Jar.Label" ) );
    props.setLook( jarLabel );
    FormData fdJarLabel = new FormData();
    fdJarLabel.left = new FormAttachment( 0 );
    fdJarLabel.top = new FormAttachment( masterUrl, MARGIN_MEDIUM );
    jarLabel.setLayoutData( fdJarLabel );

    jar = new TextVar( jobMeta, jobConfigTabComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( jar );
    jar.addModifyListener( lsMod );
    jar.addSelectionListener( lsDef );

    Button browseJar = new Button( jobConfigTabComposite, SWT.PUSH | SWT.CENTER );
    props.setLook( browseJar );
    browseJar.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdBrowseJar = new FormData();
    fdBrowseJar.top = new FormAttachment( jarLabel, MARGIN_SMALL );
    fdBrowseJar.right = new FormAttachment( 100, 0 );
    browseJar.setLayoutData( fdBrowseJar );

    FormData fdJar = new FormData();
    fdJar.left = new FormAttachment( 0 );
    fdJar.right = new FormAttachment( browseJar, -MARGIN_SMALL );
    fdJar.top = new FormAttachment( jarLabel, MARGIN_SMALL );
    jar.setLayoutData( fdJar );

    browseJar.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*;*.*" } );
        dialog.setFilterNames( FILEFORMATS );

        if ( jar.getText() != null ) {
          dialog.setFileName( jar.getText() );
        }

        if ( dialog.open() != null ) {
          jar.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    // Class
    Label classLabel = new Label( jobConfigTabComposite, SWT.RIGHT );
    classLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Class.Label" ) );
    props.setLook( classLabel );
    FormData fdClassLabel = new FormData();
    fdClassLabel.left = new FormAttachment( 0 );
    fdClassLabel.top = new FormAttachment( jar, MARGIN_MEDIUM );
    classLabel.setLayoutData( fdClassLabel );

    clazz = new TextVar( jobMeta, jobConfigTabComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( clazz );
    clazz.addModifyListener( lsMod );
    clazz.addSelectionListener( lsDef );

    FormData fdClass = new FormData();
    fdClass.left = new FormAttachment( 0 );
    fdClass.right = new FormAttachment( 100, 0 );
    fdClass.top = new FormAttachment( classLabel, MARGIN_SMALL );
    clazz.setLayoutData( fdClass );

    // Arguments
    Label argsLabel = new Label( jobConfigTabComposite, SWT.RIGHT );
    argsLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.Args.Label" ) );
    props.setLook( argsLabel );
    FormData fdArgsLabel = new FormData();
    fdArgsLabel.left = new FormAttachment( 0 );
    fdArgsLabel.top = new FormAttachment( clazz, MARGIN_MEDIUM );
    argsLabel.setLayoutData( fdArgsLabel );

    args = new TextVar( jobMeta, jobConfigTabComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( args );
    args.addModifyListener( lsMod );
    args.addSelectionListener( lsDef );

    FormData fdArgs = new FormData();
    fdArgs.left = new FormAttachment( 0 );
    fdArgs.right = new FormAttachment( 100, 0 );
    fdArgs.top = new FormAttachment( argsLabel, MARGIN_SMALL );
    args.setLayoutData( fdArgs );

    // Add group control
    Group group = new Group( jobConfigTabComposite, SWT.NONE );
    props.setLook( group );
    group.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.MemoryAllocation.Label" ) );
    FormLayout groupLayout = new FormLayout();
    groupLayout.marginHeight = MARGIN_LARGE;
    groupLayout.marginWidth = MARGIN_MEDIUM;
    group.setLayout( groupLayout );
    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment( 0 );
    fdGroup.right = new FormAttachment( 100 );
    fdGroup.top = new FormAttachment( args, MARGIN_LARGE );
    group.setLayoutData( fdGroup );

    // Memory allocation settings
    Label executorMemoryLabel = new Label( group, SWT.NONE );
    props.setLook( executorMemoryLabel );
    executorMemoryLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.MemoryAllocation.Executor.Label" ) );
    FormData fdExecutorLabel = new FormData();
    fdExecutorLabel.top = new FormAttachment( 0 );
    fdExecutorLabel.left = new FormAttachment( 0 );
    executorMemoryLabel.setLayoutData( fdExecutorLabel );

    executorMemory = new TextVar( jobEntry, group, SWT.BORDER );
    props.setLook( executorMemory );
    executorMemory.addModifyListener( lsMod );
    executorMemory.addSelectionListener( lsDef );
    FormData fdExecutorMemory = new FormData();
    fdExecutorMemory.top = new FormAttachment( executorMemoryLabel, MARGIN_MEDIUM );
    fdExecutorMemory.left = new FormAttachment( 0 );
    executorMemory.setLayoutData( fdExecutorMemory );

    Label driverMemoryLabel = new Label( group, SWT.NONE );
    props.setLook( driverMemoryLabel );
    driverMemoryLabel.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.MemoryAllocation.Driver.Label" ) );
    FormData fdDriverMemoryLabel = new FormData();
    fdDriverMemoryLabel.top = new FormAttachment( 0 );
    fdDriverMemoryLabel.left = new FormAttachment( executorMemory, 6 * MARGIN_LARGE );
    driverMemoryLabel.setLayoutData( fdDriverMemoryLabel );

    driverMemory = new TextVar( jobEntry, group, SWT.BORDER );
    props.setLook( driverMemory );
    driverMemory.addModifyListener( lsMod );
    driverMemory.addSelectionListener( lsDef );
    FormData fdDriverMemory = new FormData();
    fdDriverMemory.top = new FormAttachment( driverMemoryLabel, MARGIN_MEDIUM );
    fdDriverMemory.left = new FormAttachment( driverMemoryLabel, 0, SWT.LEFT );
    driverMemory.setLayoutData( fdDriverMemory );

    blockExecution = new Button( jobConfigTabComposite, SWT.CHECK );
    props.setLook( blockExecution );
    blockExecution.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.BlockExecution.Label" ) );
    FormData fdBlockExecution = new FormData();
    fdBlockExecution.top = new FormAttachment( group, MARGIN_MEDIUM );
    fdBlockExecution.left = new FormAttachment( 0 );
    blockExecution.setLayoutData( fdBlockExecution );

    // Config parameters tab
    CTabItem parametersTab = new CTabItem( tabs, SWT.NONE );
    parametersTab.setText( BaseMessages.getString( PKG, "JobEntrySparkSubmit.ParametersTab.Label" ) );

    Composite parametersTabComposite = new Composite( tabs, SWT.NONE );
    props.setLook( parametersTabComposite );
    parametersTab.setControl( parametersTabComposite );
    FormLayout parametersTabCompositeLayout = new FormLayout();
    parametersTabCompositeLayout.marginHeight = MARGIN_LARGE;
    parametersTabCompositeLayout.marginWidth = MARGIN_LARGE;
    parametersTabComposite.setLayout( parametersTabCompositeLayout );

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo( BaseMessages.getString( PKG, "JobEntrySparkSubmit.NameColumn.Label" ),
              ColumnInfo.COLUMN_TYPE_TEXT ),
          new ColumnInfo( BaseMessages.getString( PKG, "JobEntrySparkSubmit.ValueColumn.Label" ),
              ColumnInfo.COLUMN_TYPE_TEXT ) };

    configParams =
        new TableView( jobEntry, parametersTabComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE, columns,
            jobEntry.getConfigParams().size(), null, props );
    props.setLook( configParams );
    FormData fdConfigParams = new FormData();
    fdConfigParams.left = new FormAttachment( 0 );
    fdConfigParams.top = new FormAttachment( 0 );
    fdConfigParams.right = new FormAttachment( 100 );
    fdConfigParams.bottom = new FormAttachment( 100 );
    configParams.setLayoutData( fdConfigParams );
    configParams.addModifyListener( lsMod );
    tabs.setSelection( jobConfigTab );

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

    name.addSelectionListener( lsDef );

    // Detect [X] or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setActive();
    BaseStepDialog.setSize( shell );

    shell.pack();
    shell.setMinimumSize( SHELL_MINIMUM_WIDTH, shell.getSize().y );
    shell.open();
    props.setDialogSize( shell, "JobEntrySparkSubmitDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return jobEntry;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void setActive() {
  }

  public void getData() {
    name.setText( Const.nullToEmpty( jobEntry.getName() ) );
    sparkSubmit.setText( Const.nullToEmpty( jobEntry.getScriptPath() ) );
    clazz.setText( Const.nullToEmpty( jobEntry.getClassName() ) );

    for ( String url : MASTER_URLS ) {
      masterUrl.add( url );
    }

    masterUrl.setText( Const.nullToEmpty( jobEntry.getMaster() ) );
    jar.setText( Const.nullToEmpty( jobEntry.getJar() ) );
    args.setText( Const.nullToEmpty( jobEntry.getArgs() ) );
    blockExecution.setSelection( jobEntry.isBlockExecution() );

    List<String> params = jobEntry.getConfigParams();
    for ( int i = 0; i < params.size(); i++ ) {
      TableItem ti = configParams.table.getItem( i );
      String[] nameValue = params.get( i ).split( "=" );
      ti.setText( 1, nameValue[0] );
      ti.setText( 2, nameValue[1] );
    }
    configParams.setRowNums();
    configParams.optWidth( true );

    executorMemory.setText( Const.nullToEmpty( jobEntry.getExecutorMemory() ) );
    driverMemory.setText( Const.nullToEmpty( jobEntry.getDriverMemory() ) );

    name.selectAll();
    name.setFocus();
  }

  private void cancel() {
    jobEntry.setChanged( backupChanged );
    jobEntry = null;
    dispose();
  }

  protected void ok() {
    if ( Const.isEmpty( name.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }
    jobEntry.setName( name.getText() );
    jobEntry.setScriptPath( sparkSubmit.getText() );
    jobEntry.setMaster( masterUrl.getText() );
    jobEntry.setJar( jar.getText() );
    jobEntry.setClassName( clazz.getText() );
    jobEntry.setArgs( args.getText() );
    jobEntry.setBlockExecution( blockExecution.getSelection() );

    ArrayList<String> configParams = new ArrayList<String>( this.configParams.getItemCount() );
    for ( int i = 0; i < this.configParams.getItemCount(); i++ ) {
      String[] item = this.configParams.getItem( i );
      if ( !Const.isEmpty( item[0] ) && !Const.isEmpty( item[1] ) ) {
        configParams.add( item[0].trim() + "=" + item[1].trim() );
      }
    }
    jobEntry.setConfigParams( configParams );
    jobEntry.setDriverMemory( driverMemory.getText() );
    jobEntry.setExecutorMemory( executorMemory.getText() );

    dispose();
  }

  public static void main( String[] args ) {
    Display display = new Display();
    PropsUI.init( display, Props.TYPE_PROPERTIES_SPOON );
    Shell shell = new Shell( display );

    JobEntrySparkSubmitDialog dialog =
        new JobEntrySparkSubmitDialog( shell, new JobEntrySparkSubmit( "Spark submit job entry" ), null, new JobMeta() );

    dialog.open();
  }
}
