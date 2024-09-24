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

package org.pentaho.big.data.kettle.plugins.hdfs.vfs;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class NamedClusterVfsFileChooserDialog extends CustomVfsUiPanel {

  // for message resolution
  private static final Class<?> PKG = NamedClusterVfsFileChooserDialog.class;

  // for logging
  private LogChannel log = new LogChannel( this );

  // Default root file - used to avoid NPE when rootFile was not provided
  // and the browser is resolved
  FileObject defaultInitialFile = null;

  // File objects to keep track of when the user selects the radio buttons
  FileObject hadoopRootFile = null;
  String hadoopOpenFromFolder = null;

  FileObject rootFile = null;
  FileObject initialFile = null;
  VfsFileChooserDialog vfsFileChooserDialog = null;

  String schemeName = Schemes.NAMED_CLUSTER_SCHEME;

  private NamedClusterWidgetImpl namedClusterWidget = null;
  private String namedCluster = null;
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  public NamedClusterVfsFileChooserDialog( String schemeName, String displayName,
                                           VfsFileChooserDialog vfsFileChooserDialog,
                                           FileObject rootFile, FileObject initialFile,
                                           NamedClusterService namedClusterService,
                                           RuntimeTestActionService runtimeTestActionService,
                                           RuntimeTester runtimeTester ) {
    super( schemeName, displayName, vfsFileChooserDialog, SWT.NONE );
    this.schemeName = schemeName;
    this.rootFile = rootFile;
    this.initialFile = initialFile;
    this.vfsFileChooserDialog = vfsFileChooserDialog;
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;

    // Create the Hadoop panel
    GridData gridData = new GridData( SWT.FILL, SWT.CENTER, true, false );
    setLayoutData( gridData );
    setLayout( new GridLayout( 1, false ) );

    createConnectionPanel();
  }

  private void createConnectionPanel() {
    // The Connection group
    Group connectionGroup = new Group( this, SWT.SHADOW_ETCHED_IN );
    connectionGroup
      .setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.ConnectionGroup.Label" ) ); //$NON-NLS-1$ ;
    GridLayout connectionGroupLayout = new GridLayout();
    connectionGroupLayout.marginWidth = 5;
    connectionGroupLayout.marginHeight = 5;
    connectionGroupLayout.verticalSpacing = 5;
    connectionGroupLayout.horizontalSpacing = 5;
    GridData gData = new GridData( SWT.FILL, SWT.FILL, true, false );
    connectionGroup.setLayoutData( gData );
    connectionGroup.setLayout( connectionGroupLayout );

    setNamedClusterWidget(
      new NamedClusterWidgetImpl( connectionGroup, true, namedClusterService, runtimeTestActionService,
        runtimeTester ) );
    getNamedClusterWidget().addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent evt ) {
        try {
          connect();
        } catch ( Exception e ) {
          // To prevent errors from multiple event firings.
          log.logDebug( e.getMessage() );
        }
      }
    } );

    // The composite we need in the group
    Composite textFieldPanel = new Composite( connectionGroup, SWT.NONE );
    GridData gridData = new GridData( SWT.FILL, SWT.FILL, true, false );
    textFieldPanel.setLayoutData( gridData );
    textFieldPanel.setLayout( new GridLayout( 5, false ) );
  }

  public void initializeConnectionPanel( FileObject file ) {
    initialFile = file;
    /*
     * if ( initialFile != null && initialFile.getName().getScheme().equals( HadoopSpoonPlugin.HDFS_SCHEME ) ) { //TODO
     * activate HDFS }
     */
  }

  private void showMessageAndLog( String title, String message, String messageToLog ) {
    MessageBox box = new MessageBox( this.getShell() );
    box.setText( title ); // $NON-NLS-1$
    box.setMessage( message );
    log.logError( messageToLog );
    box.open();
  }

  public VariableSpace getVariableSpace() {
    if ( Spoon.getInstance().getActiveTransformation() != null ) {
      return Spoon.getInstance().getActiveTransformation();
    } else if ( Spoon.getInstance().getActiveJob() != null ) {
      return Spoon.getInstance().getActiveJob();
    } else {
      return new Variables();
    }
  }

  public NamedClusterWidgetImpl getNamedClusterWidget() {
    return namedClusterWidget;
  }

  protected void setNamedClusterWidget( NamedClusterWidgetImpl namedClusterWidget ) {
    this.namedClusterWidget = namedClusterWidget;
  }

  public void setNamedCluster( String namedCluster ) {
    this.namedCluster = namedCluster;
  }

  @Override
  public void activate() {
    vfsFileChooserDialog.setRootFile( null );
    vfsFileChooserDialog.setInitialFile( null );
    vfsFileChooserDialog.openFileCombo.setText(  Schemes.NAMED_CLUSTER_SCHEME + "://" );
    vfsFileChooserDialog.vfsBrowser.fileSystemTree.removeAll();
    getNamedClusterWidget().initiate();
    getNamedClusterWidget().setSelectedNamedCluster( namedCluster );
    super.activate();
  }

  public void connect() {
    NamedCluster nc = getNamedClusterWidget().getSelectedNamedCluster();
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( nc, getVariableSpace() );
    hdfsConnection.setCustomParameters( Props.getInstance() );
    // The Named Cluster may be hdfs, maprfs or wasb.  We need to detect it here since the named
    // cluster was just selected.
    //schemeName = "wasb".equals( nc.getStorageScheme() ) ? "wasb" : "hdfs";
    String connectionString = Schemes.NAMED_CLUSTER_SCHEME + "://" + nc.getName();
    FileSystemOptions fsoptions = new FileSystemOptions();
    FileObject root = rootFile;
    try {
      root = KettleVFS.getFileObject( connectionString, fsoptions );
    } catch ( KettleFileException exc ) {
      showMessageAndLog( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.error" ), BaseMessages.getString( PKG,
        "HadoopVfsFileChooserDialog.Connection.error" ), exc.getMessage() );
    }
    vfsFileChooserDialog.setRootFile( root );
    vfsFileChooserDialog.setSelectedFile( root );
    rootFile = root;
  }

  /**
   * resolve file with <b>new</b> File SystemOptions.
   */
  @Override
  public FileObject resolveFile( String fileUri ) throws FileSystemException {
    try {
      //should we use new instance of FileSystemOptions? should it be depdrecated?
      return KettleVFS.getFileObject( fileUri, getVariableSpace(), getFileSystemOptions() );
    } catch ( KettleFileException e ) {
      throw new FileSystemException( e );
    }
  }

  @Override
  public FileObject resolveFile( String fileUri, FileSystemOptions opts ) throws FileSystemException {
    try {
      return KettleVFS.getFileObject( fileUri, getVariableSpace(), opts );
    } catch ( KettleFileException e ) {
      throw new FileSystemException( e );
    }
  }

  /**
   * @return <b>new</b>FileSystem Options
   * @throws FileSystemException
   */
  protected FileSystemOptions getFileSystemOptions() throws FileSystemException {
    FileSystemOptions opts = new FileSystemOptions();
    return opts;
  }

}
