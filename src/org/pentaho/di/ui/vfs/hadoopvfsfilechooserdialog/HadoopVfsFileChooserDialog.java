/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.apache.commons.vfs.provider.GenericFileName;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class HadoopVfsFileChooserDialog extends CustomVfsUiPanel {

  // for message resolution
  private static final Class<?> PKG = HadoopVfsFileChooserDialog.class;

  // Property in Hadoop Configuration specifying a list of Name Nodes in an HA environment
  private static final String HDFS_HA_CLUSTER_NAMENODES_PROP = "dfs.ha.namenodes.hacluster";

  // Delimiter for Name Node lists in an HA environment
  private static final String NAMENODE_LIST_DELIMITER = ",";

  // Prefix for Hadoop Configuration property for resolving cluster names to host names
  private static final String HDFS_HA_CLUSTER_NAMENODE_RESOLVE_PREFIX = "dfs.namenode.rpc-address.hacluster.";

  // Delimiter for Name Node lists in an HA environment
  private static final String NAMENODE_HOSTNAME_PORT_DELIMITER = ":";

  // for logging
  private LogChannel log = new LogChannel( this );

  // URL label and field
  private Label wlUrl;
  private Text wUrl;
  private GridData fdlUrl, fdUrl;

  // Port label and field
  private Label wlPort;
  private Text wPort;
  private GridData fdlPort, fdPort;

  // UserID label and field
  private Label wlUserID;
  private Text wUserID;
  private GridData fdlUserID, fdUserID;

  // Password label and field
  private Label wlPassword;
  private Text wPassword;
  private GridData fdlPassword, fdPassword;

  // Place holder - for creating a blank widget in a grid layout
  private Label wPlaceHolderLabel;
  private GridData fdlPlaceHolderLabel;

  // Connection button
  private Button wConnectionButton;
  private GridData fdConnectionButton;

  // Default root file - used to avoid NPE when rootFile was not provided
  // and the browser is resolved
  FileObject defaultInitialFile = null;

  // File objects to keep track of when the user selects the radio buttons
  FileObject hadoopRootFile = null;
  String hadoopOpenFromFolder = null;

  FileObject rootFile = null;
  FileObject initialFile = null;
  VfsFileChooserDialog vfsFileChooserDialog = null;

  // Successful connection params (to hand off to VFS)
  String connectedHostname = null;
  String connectedPortString = null;

  // Indicates whether the cluster is a High Availability (HA) cluster. This changes the
  // way hostname and port resolutions (for Connect Test and HDFS) are done.
  boolean isHighAvailabilityCluster = false;

  String schemeName = "hdfs";

  public HadoopVfsFileChooserDialog( String schemeName, String displayName, VfsFileChooserDialog vfsFileChooserDialog,
      FileObject rootFile, FileObject initialFile ) {
    super( schemeName, displayName, vfsFileChooserDialog, SWT.NONE );
    this.schemeName = schemeName;
    this.rootFile = rootFile;
    this.initialFile = initialFile;
    this.vfsFileChooserDialog = vfsFileChooserDialog;
    // Create the Hadoop panel
    GridData gridData = new GridData( SWT.FILL, SWT.CENTER, true, false );
    setLayoutData( gridData );
    setLayout( new GridLayout( 1, false ) );

    createConnectionPanel();
    initializeConnectionPanel();
  }

  private void createConnectionPanel() {
    // The Connection group
    Group connectionGroup = new Group( this, SWT.SHADOW_ETCHED_IN );
    connectionGroup.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.ConnectionGroup.Label" ) ); //$NON-NLS-1$;
    GridLayout connectionGroupLayout = new GridLayout();
    connectionGroupLayout.marginWidth = 5;
    connectionGroupLayout.marginHeight = 5;
    connectionGroupLayout.verticalSpacing = 5;
    connectionGroupLayout.horizontalSpacing = 5;
    GridData gData = new GridData( SWT.FILL, SWT.FILL, true, false );
    connectionGroup.setLayoutData( gData );
    connectionGroup.setLayout( connectionGroupLayout );

    // The composite we need in the group
    Composite textFieldPanel = new Composite( connectionGroup, SWT.NONE );
    GridData gridData = new GridData( SWT.FILL, SWT.FILL, true, false );
    textFieldPanel.setLayoutData( gridData );
    textFieldPanel.setLayout( new GridLayout( 5, false ) );

    // URL label and text field
    wlUrl = new Label( textFieldPanel, SWT.RIGHT );
    wlUrl.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.URL.Label" ) ); //$NON-NLS-1$
    fdlUrl = new GridData();
    fdlUrl.widthHint = 75;
    wlUrl.setLayoutData( fdlUrl );
    wUrl = new Text( textFieldPanel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    fdUrl = new GridData();
    fdUrl.widthHint = 150;
    wUrl.setLayoutData( fdUrl );
    wUrl.setText( Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.host", "localhost" ) );
    wUrl.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        handleConnectionButton();
      }
    } );

    // UserID label and field
    wlUserID = new Label( textFieldPanel, SWT.RIGHT );
    wlUserID.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.UserID.Label" ) ); //$NON-NLS-1$
    fdlUserID = new GridData();
    fdlUserID.widthHint = 75;
    wlUserID.setLayoutData( fdlUserID );

    wUserID = new Text( textFieldPanel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    fdUserID = new GridData();
    fdUserID.widthHint = 150;
    wUserID.setLayoutData( fdUserID );
    wUserID.setText( Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.user", "" ) );

    // Place holder
    wPlaceHolderLabel = new Label( textFieldPanel, SWT.RIGHT );
    wPlaceHolderLabel.setText( "" );
    fdlPlaceHolderLabel = new GridData();
    fdlPlaceHolderLabel.widthHint = 75;
    wlUserID.setLayoutData( fdlPlaceHolderLabel );

    // Port label and text field
    wlPort = new Label( textFieldPanel, SWT.RIGHT );
    wlPort.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.Port.Label" ) ); //$NON-NLS-1$
    fdlPort = new GridData();
    fdlPort.widthHint = 75;
    wlPort.setLayoutData( fdlPort );

    wPort = new Text( textFieldPanel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    fdPort = new GridData();
    fdPort.widthHint = 150;
    wPort.setLayoutData( fdPort );
    wPort.setText( Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.port", "9000" ) );
    wPort.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        handleConnectionButton();
      }
    } );

    // password label and field
    wlPassword = new Label( textFieldPanel, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.Password.Label" ) ); //$NON-NLS-1$
    fdlPassword = new GridData();
    fdlPassword.widthHint = 75;
    wlPassword.setLayoutData( fdlPassword );

    wPassword = new Text( textFieldPanel, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wPassword.setEchoChar( '*' );
    fdPassword = new GridData();
    fdPassword.widthHint = 150;
    wPassword.setLayoutData( fdPassword );
    wPassword.setText( Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.password", "" ) );

    // Connection button
    wConnectionButton = new Button( textFieldPanel, SWT.CENTER );
    fdConnectionButton = new GridData();
    fdConnectionButton.widthHint = 75;
    wConnectionButton.setLayoutData( fdConnectionButton );

    wConnectionButton.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.ConnectionButton.Label" ) );
    wConnectionButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        // Store the successful connection info to hand off to VFS
        connectedHostname = wUrl.getText();
        connectedPortString = wPort.getText();

        try {

          // Create list of addresses to try. In non-HA environments, this will likely only have
          // one entry.
          ArrayList<InetSocketAddress> addressList = new ArrayList<InetSocketAddress>();

          // Before creating a socket, see if there is some name resolution we need to do
          // For example, in High Availability clusters, we might need to resolve the cluster name
          // (with no port) to a list of host:port pairs to try in sequence.
          // NOTE: If we could set the HDFS retry limit for the Test capability, we wouldn't need this
          // code. It's been fixed in later versions of Hadoop, but we can't be sure which version we're
          // using, or if a particular distribution has incorporated the fix.
          HadoopConfiguration hadoopConfig = getHadoopConfig();
          if ( hadoopConfig != null ) {
            HadoopShim shim = hadoopConfig.getHadoopShim();
            Configuration conf = shim.createConfiguration();
            String haNameNodes = conf.get( HDFS_HA_CLUSTER_NAMENODES_PROP );
            if ( !Const.isEmpty( haNameNodes ) ) {

              String[] haNameNode = haNameNodes.split( NAMENODE_LIST_DELIMITER );
              if ( !Const.isEmpty( haNameNode ) ) {
                for ( String nameNode : haNameNode ) {
                  String nameNodeResolveProperty = HDFS_HA_CLUSTER_NAMENODE_RESOLVE_PREFIX + nameNode;
                  String nameNodeHostAndPort = conf.get( nameNodeResolveProperty );
                  if ( !Const.isEmpty( nameNodeHostAndPort ) ) {
                    String[] nameNodeParams = nameNodeHostAndPort.split( NAMENODE_HOSTNAME_PORT_DELIMITER );
                    String hostname = nameNodeParams[0];
                    int port = 0;
                    if ( nameNodeParams.length > 1 ) {
                      try {
                        port = Integer.parseInt( nameNodeParams[1] );
                      } catch ( NumberFormatException nfe ) {
                        // ignore, use default
                      }
                    }
                    addressList.add( new InetSocketAddress( hostname, port ) );
                    isHighAvailabilityCluster = true;
                  }
                }
              }
            } else {
              String hostname = wUrl.getText();
              int port = 0;
              try {
                port = Integer.parseInt( wPort.getText() );
              } catch ( NumberFormatException nfe ) {
                // ignore, use default
              }
              addressList.add( new InetSocketAddress( hostname, port ) );
              isHighAvailabilityCluster = false;
            }

            boolean success = false;
            StringBuffer connectMessage = new StringBuffer();
            for ( int i = 0; !success && i < addressList.size(); i++ ) {
              InetSocketAddress address = addressList.get( i );
              connectMessage.append( "Connect " );
              connectMessage.append( address.getHostName() );
              connectMessage.append( NAMENODE_HOSTNAME_PORT_DELIMITER );
              connectMessage.append( address.getPort() );
              Socket testHdfsSocket = new Socket( address.getHostName(), address.getPort() );
              try {
                testHdfsSocket.getOutputStream();
                testHdfsSocket.close();
                success = true;
                connectedHostname = address.getHostName();
                connectedPortString = Integer.toString( address.getPort() );
                connectMessage.append( "=success!" );
              } catch ( IOException ioe ) {
                // Add errors to message string, but otherwise ignore, we'll check for success later
                connectMessage.append( "=failed, " );
                connectMessage.append( ioe.getMessage() );
                connectMessage.append( System.getProperty( "line.separator" ) );
              }
            }
            if ( !success ) {
              throw new IOException( connectMessage.toString() );
            }
          } else {
            throw new Exception( "No active Hadoop Configuration specified!" );
          }

        } catch ( Throwable t ) {
          showMessageAndLog( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.error" ), BaseMessages.getString(
              PKG, "HadoopVfsFileChooserDialog.Connection.error" ), t.getMessage() );
          return;
        }

        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.host", wUrl.getText() );
        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.port", connectedPortString );
        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.user", wUserID.getText() );
        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.password", wPassword.getText() );

        FileObject root = rootFile;
        try {
          root = KettleVFS.getFileObject( buildHadoopFileSystemUrlString() );
        } catch ( KettleFileException e1 ) {
          // Search for "unsupported scheme" message. The actual string has parameters that we won't be able to match,
          // so build a string with
          // known (dummy) params, then split to get the beginning string, then compare against the current exception's
          // message.
          final String unsupportedSchemeMessage =
              BaseMessages.getString( HadoopConfiguration.class, "Error.UnsupportedSchemeForConfiguration", "@!@",
                  "!@!" );
          final String unsupportedSchemeMessagePrefix = unsupportedSchemeMessage.split( "@!@" )[0];
          final String message = e1.getMessage();
          if ( message.contains( unsupportedSchemeMessagePrefix ) ) {
            try {
              HadoopConfiguration hadoopConfig = getHadoopConfig();
              String hadoopConfigName = ( hadoopConfig == null ) ? "Unknown" : hadoopConfig.getName();
              showMessageAndLog( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.error" ), BaseMessages
                  .getString( PKG, "HadoopVfsFileChooserDialog.Connection.schemeError", hadoopConfigName ), message );
            } catch ( ConfigurationException ce ) {
              showMessageAndLog( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.error" ), BaseMessages
                  .getString( PKG, "HadoopVfsFileChooserDialog.Connection.error" ), ce.getMessage() );
            }
          } else {
            showMessageAndLog( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.error" ), BaseMessages
                .getString( PKG, "HadoopVfsFileChooserDialog.Connection.error" ), e1.getMessage() );
          }
          return;
        }
        vfsFileChooserDialog.setSelectedFile( root );
        vfsFileChooserDialog.setRootFile( root );
        rootFile = root;
      }
    } );

    // set the tab order
    textFieldPanel.setTabList( new Control[] { wUrl, wPort, wUserID, wPassword, wConnectionButton } );
  }

  /**
   * Build an HDFS URL given a URL and Port provided by the user.
   * 
   * @return a String containing the HDFS URL
   * @TODO: relocate to a Hadoop helper class or similar
   */
  public String buildHadoopFileSystemUrlString() {
    StringBuffer urlString = new StringBuffer( schemeName );
    urlString.append( "://" );
    if ( wUserID.getText() != null && !"".equals( wUserID.getText() ) ) {

      urlString.append( wUserID.getText() );
      urlString.append( ":" );
      urlString.append( wPassword.getText() );
      urlString.append( "@" );
    }

    urlString.append( wUrl.getText() );
    if ( !Const.isEmpty( wPort.getText() ) ) {
      urlString.append( ":" );
      urlString.append( wPort.getText() );
    }
    return urlString.toString();
  }

  private void initializeConnectionPanel() {
    if ( initialFile != null && initialFile.getName().getScheme().equals( HadoopSpoonPlugin.HDFS_SCHEME ) ) {
      // populate the server and port fields
      try {
        GenericFileName genericFileName = (GenericFileName) initialFile.getFileSystem().getRoot().getName();
        wUrl.setText( genericFileName.getHostName() );
        wPort.setText( String.valueOf( genericFileName.getPort() ) );
        wUserID.setText( genericFileName.getUserName() == null ? "" : genericFileName.getUserName() ); //$NON-NLS-1$
        wPassword.setText( genericFileName.getPassword() == null ? "" : genericFileName.getPassword() ); //$NON-NLS-1$
      } catch ( FileSystemException fse ) {
        showMessageAndLog( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.error" ), BaseMessages.getString(
            PKG, "HadoopVfsFileChooserDialog.FileSystem.error" ), fse.getMessage() );
      }
    }

    handleConnectionButton();
  }

  private void showMessageAndLog( String title, String message, String messageToLog ) {
    MessageBox box = new MessageBox( this.getShell() );
    box.setText( title ); //$NON-NLS-1$
    box.setMessage( message );
    log.logError( messageToLog );
    box.open();
  }

  private void handleConnectionButton() {
    if ( !Const.isEmpty( wUrl.getText() ) ) {
      wConnectionButton.setEnabled( true );
    } else {
      wConnectionButton.setEnabled( false );
    }
  }

  private HadoopConfiguration getHadoopConfig() throws ConfigurationException {
    HadoopConfiguration hadoopConfig = null;
    HadoopConfigurationProvider provider = HadoopConfigurationBootstrap.getHadoopConfigurationProvider();
    if ( provider != null ) {
      hadoopConfig = provider.getActiveConfiguration();
    }
    return hadoopConfig;
  }
}
