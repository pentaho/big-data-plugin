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
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.namedconfig.model.NamedConfiguration;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.namedconfig.NamedConfigurationWidget;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopConfigurationProvider;
import org.pentaho.hadoop.shim.spi.HadoopShim;
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
  
  private String ncHostname = null;
  private String ncPort = null;
  private String ncUsername = null;
  private String ncPassword = null;
  private NamedConfigurationWidget namedConfigurationWidget = null;

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
    
    namedConfigurationWidget = new NamedConfigurationWidget( connectionGroup );
    namedConfigurationWidget.addSelectionListener( new SelectionListener() {
      public void widgetSelected( SelectionEvent evt ) {
        handleConnectionButton();
      }
      
      public void widgetDefaultSelected( SelectionEvent evt ) {
      }
    } );

    // The composite we need in the group
    Composite textFieldPanel = new Composite( connectionGroup, SWT.NONE );
    GridData gridData = new GridData( SWT.FILL, SWT.FILL, true, false );
    textFieldPanel.setLayoutData( gridData );
    textFieldPanel.setLayout( new GridLayout( 5, false ) );
    
    // Connection button
    wConnectionButton = new Button( textFieldPanel, SWT.CENTER );
    fdConnectionButton = new GridData();
    fdConnectionButton.widthHint = 75;
    wConnectionButton.setLayoutData( fdConnectionButton );

    wConnectionButton.setText( BaseMessages.getString( PKG, "HadoopVfsFileChooserDialog.ConnectionButton.Label" ) );
    wConnectionButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        
        loadNamedConfiguration();

        // Store the successful connection info to hand off to VFS
        connectedHostname = ncHostname;
        connectedPortString = ncPort;

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
              String hostname = ncHostname;
              int port = 0;
              try {
                port = Integer.parseInt( ncPort );
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

        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.host", ncHostname );
        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.port", connectedPortString );
        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.user", ncUsername );
        Props.getInstance().setCustomParameter( "HadoopVfsFileChooserDialog.password", ncPassword );

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
    if ( ncUsername != null && !ncUsername.trim().equals( "" )) {

      urlString.append( ncUsername );
      urlString.append( ":" );
      urlString.append( ncPassword );
      urlString.append( "@" );
    }

    urlString.append( ncHostname );
    if ( !Const.isEmpty( ncPort ) ) {
      urlString.append( ":" );
      urlString.append( ncPort );
    }
    return urlString.toString();
  }

  public void initializeConnectionPanel( FileObject defaultInitialFile ) {
    this.initialFile = defaultInitialFile;
    if ( initialFile != null && initialFile.getName().getScheme().equals( HadoopSpoonPlugin.HDFS_SCHEME ) ) {
      try {
        //TODO EC Resolve Named Config based on hostname, port, userId and password?
        GenericFileName genericFileName = (GenericFileName) initialFile.getFileSystem().getRoot().getName();
        String hostName =  genericFileName.getHostName();
        String port =  String.valueOf( genericFileName.getPort() ) ;
        String userId = genericFileName.getUserName() == null ? "" : genericFileName.getUserName();
        String password =  genericFileName.getPassword() == null ? "" : genericFileName.getPassword(); 
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
    loadNamedConfiguration();
    if ( !Const.isEmpty( ncHostname ) ) {
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
  
  private void loadNamedConfiguration() {
    NamedConfiguration namedConfiguration = namedConfigurationWidget.getSelectedNamedConfiguration();
    if ( namedConfiguration != null ) {
      ncHostname = namedConfiguration.getPropertyValue( "HDFS", "hostname" );
      ncPort = namedConfiguration.getPropertyValue( "HDFS", "port" );
      ncUsername = namedConfiguration.getPropertyValue( "HDFS", "username" );
      ncPassword = namedConfiguration.getPropertyValue( "HDFS", "password" );
    }
  }
  
  public NamedConfigurationWidget getNamedConfigurationWidget() {
    return namedConfigurationWidget;
  }
}
