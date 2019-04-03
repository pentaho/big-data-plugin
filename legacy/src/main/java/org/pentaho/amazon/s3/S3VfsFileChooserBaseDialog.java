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

package org.pentaho.amazon.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridLayout;
import org.pentaho.amazon.AmazonSpoonPlugin;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.s3.vfs.S3FileObject;
import org.pentaho.s3n.vfs.S3NFileObject;
import org.pentaho.s3n.vfs.S3NFileProvider;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.widget.TextVar;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.apache.commons.vfs2.provider.GenericFileName;

/**
 * The UI for S3 VFS
 */
public abstract class S3VfsFileChooserBaseDialog extends CustomVfsUiPanel {

  private static Class<?> PKG = AmazonSpoonPlugin.class;

  private LogChannel log = new LogChannel( this );

  protected FileObject rootFile;
  protected FileObject initialFile;
  protected VfsFileChooserDialog vfsFileChooserDialog;

  // URL label and field
  private Label wlAccessKey;
  private TextVar wAccessKey;
  private GridData fdlAccessKey, fdAccessKey;

  // UserID label and field
  private Label wlSecretKey;
  private TextVar wSecretKey;
  private GridData fdSecretKey;

  // Place holder - for creating a blank widget in a grid layout
  private Label wPlaceHolderLabel;
  private GridData fdlPlaceHolderLabel;

  // Connection button
  private Button wConnectionButton;
  private GridData fdConnectionButton;

  private String accessKey;
  private String secretKey;

  public S3VfsFileChooserBaseDialog( VfsFileChooserDialog vfsFileChooserDialog, FileObject rootFile,
                                  FileObject initialFile, String schema, String fileSystemDisplayText ) {
    super( schema, fileSystemDisplayText, vfsFileChooserDialog, SWT.NONE );

    this.vfsFileChooserDialog = vfsFileChooserDialog;
    this.rootFile = rootFile;
    this.initialFile = initialFile;

    setLayout( new GridLayout() );

    // Create the s3 panel
    createConnectionPanel();
    initializeConnectionPanel();
  }

  private void createConnectionPanel() {

    // The Connection group
    Group connectionGroup = new Group( this, SWT.SHADOW_ETCHED_IN );
    connectionGroup
      .setText( BaseMessages.getString( PKG, "S3VfsFileChooserDialog.ConnectionGroup.Label" ) ); //$NON-NLS-1$;
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
    textFieldPanel.setLayout( new GridLayout( 3, false ) );

    // URL label and text field
    wlAccessKey = new Label( textFieldPanel, SWT.RIGHT );
    wlAccessKey.setText( BaseMessages.getString( PKG, "S3VfsFileChooserDialog.AccessKey.Label" ) ); //$NON-NLS-1$
    fdlAccessKey = new GridData();
    fdlAccessKey.widthHint = 75;
    wlAccessKey.setLayoutData( fdlAccessKey );
    wAccessKey = new TextVar( getVariableSpace(), textFieldPanel, SWT.PASSWORD | SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    fdAccessKey = new GridData();
    fdAccessKey.widthHint = 150;
    wAccessKey.setLayoutData( fdAccessKey );
    wAccessKey.setText( Encr.decryptPasswordOptionallyEncrypted(
      Props.getInstance().getCustomParameter( "S3VfsFileChooserDialog.AccessKey", "" ) ) );

    wAccessKey.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        handleConnectionButton();
      }
    } );

    // // Place holder
    wPlaceHolderLabel = new Label( textFieldPanel, SWT.RIGHT );
    wPlaceHolderLabel.setText( "" );
    fdlPlaceHolderLabel = new GridData();
    fdlPlaceHolderLabel.widthHint = 75;
    wPlaceHolderLabel.setLayoutData( fdlPlaceHolderLabel );

    // UserID label and field
    wlSecretKey = new Label( textFieldPanel, SWT.RIGHT );
    wlSecretKey.setText( BaseMessages.getString( PKG, "S3VfsFileChooserDialog.SecretKey.Label" ) );
    wSecretKey = new TextVar( getVariableSpace(), textFieldPanel, SWT.PASSWORD | SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    fdSecretKey = new GridData();
    fdSecretKey.widthHint = 300;
    wSecretKey.setLayoutData( fdSecretKey );
    wSecretKey.setText( Encr.decryptPasswordOptionallyEncrypted(
      Props.getInstance().getCustomParameter( "S3VfsFileChooserDialog.SecretKey", "" ) ) );

    wSecretKey.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        handleConnectionButton();
      }
    } );

    // Connection button
    wConnectionButton = new Button( textFieldPanel, SWT.CENTER );
    fdConnectionButton = new GridData();
    fdConnectionButton.widthHint = 75;
    wConnectionButton.setLayoutData( fdConnectionButton );
    wConnectionButton.setEnabled( true );
    wConnectionButton.setText( BaseMessages.getString( PKG, "S3VfsFileChooserDialog.ConnectionButton.Label" ) );
    wConnectionButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        try {
          if ( !ValueMetaBase.convertStringToBoolean( Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_USE_AWS_DEFAULT_CREDENTIALS ), "N" ) ) ) {
            System.setProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY, environmentSubstitute( wAccessKey.getText() ) );
            System.setProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY, environmentSubstitute( wSecretKey.getText() ) );
          }
          String file = vfsFileChooserDialog.openFileCombo.getText() != null ? vfsFileChooserDialog.openFileCombo.getText() : buildS3FileSystemUrlString();
          FileObject root = resolveFile( file );
          vfsFileChooserDialog.vfsBrowser.clearFileChildren( root.getPublicURIString() );
          vfsFileChooserDialog.setRootFile( root );
          vfsFileChooserDialog.vfsBrowser.resetVfsRoot( root );
          vfsFileChooserDialog.setSelectedFile( root );
          rootFile = root;
        } catch ( AmazonS3Exception | FileSystemException ex ) {
          // if anything went wrong, we have to show an error dialog
          MessageBox box = new MessageBox( getShell() );
          box.setText( BaseMessages.getString( PKG, "S3VfsFileChooserDialog.error" ) ); //$NON-NLS-1$
          box.setMessage( ex.getMessage() );
          log.logError( ex.getMessage(), ex );
          box.open();
          return;
        }
      }
    } );

    // set the tab order
    textFieldPanel.setTabList( new Control[] { wAccessKey, wSecretKey, wConnectionButton } );
  }

  private String environmentSubstitute( String str ) {
    return getVariableSpace().environmentSubstitute( str );
  }

  /**
   * Build a URL given Url and Port provided by the user.
   *
   * @return
   * @TODO: relocate to a s3 helper class or similar
   */
  public String buildS3FileSystemUrlString() {
    return S3NFileProvider.SCHEME + "://s3n/";
  }

  @Override
  public void activate() {
    wAccessKey.setVariables( getVariableSpace() );
    wSecretKey.setVariables( getVariableSpace() );
    wAccessKey.setText( Encr.decryptPasswordOptionallyEncrypted(
      Props.getInstance().getCustomParameter( "S3VfsFileChooserDialog.AccessKey", "" ) ) );
    wSecretKey.setText( Encr.decryptPasswordOptionallyEncrypted(
      Props.getInstance().getCustomParameter( "S3VfsFileChooserDialog.SecretKey", "" ) ) );
    vfsFileChooserDialog.setRootFile( rootFile );
    vfsFileChooserDialog.setInitialFile( initialFile );
    vfsFileChooserDialog.vfsBrowser.fileSystemTree.removeAll();
    super.activate();

    try {
      String filename = Props.getInstance().getCustomParameter( "S3VfsFileChooserDialog.Filename", buildS3FileSystemUrlString() );
      FileObject newRoot = resolveFile( filename );
      vfsFileChooserDialog.vfsBrowser.resetVfsRoot( newRoot );
    } catch ( FileSystemException ignored ) {
      //ignored
    }
  }

  private void initializeConnectionPanel() {
    if ( initialFile != null && ( initialFile instanceof S3NFileObject || initialFile instanceof S3FileObject ) ) {
      // populate the server and port fields
      try {
        GenericFileName genericFileName = (GenericFileName) initialFile.getFileSystem().getRoot().getName();
        wAccessKey.setText( genericFileName.getUserName() == null ? "" : genericFileName.getUserName() );
        wSecretKey.setText( genericFileName.getPassword() ); //$NON-NLS-1$
      } catch ( FileSystemException fse ) {
        showMessageAndLog( "S3VfsFileChooserDialog.error", "S3VfsFileChooserDialog.FileSystem.error",
          fse.getMessage() );
      }
    }
    handleConnectionButton();
  }

  private void showMessageAndLog( String title, String message, String messageToLog ) {
    MessageBox box = new MessageBox( getShell() );
    box.setText( BaseMessages.getString( PKG, title ) ); //$NON-NLS-1$
    box.setMessage( BaseMessages.getString( PKG, message ) );
    log.logError( messageToLog );
    box.open();
  }

  private void handleConnectionButton() {
    if ( !StringUtils.isEmpty( wAccessKey.getText() ) && !StringUtils.isEmpty( wSecretKey.getText() ) ) {
      accessKey = Encr.decryptPasswordOptionallyEncrypted( getVariableSpace().environmentSubstitute( wAccessKey.getText() ) );
      secretKey = Encr.decryptPasswordOptionallyEncrypted( getVariableSpace().environmentSubstitute( wSecretKey.getText() ) );
    } else {
      accessKey = null;
      secretKey = null;
    }
  }


  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public FileObject resolveFile( String fileUri ) throws FileSystemException {
    try {
      return KettleVFS.getFileObject( fileUri, getVariableSpace(), getFileSystemOptions() );
    } catch ( KettleFileException e ) {
      throw new FileSystemException( e );
    }
  }

  public FileObject resolveFile( String fileUri, FileSystemOptions opts ) throws FileSystemException {
    try {
      return KettleVFS.getFileObject( fileUri, getVariableSpace(), opts );
    } catch ( KettleFileException e ) {
      throw new FileSystemException( e );
    }
  }

  private FileSystemOptions getFileSystemOptions() throws FileSystemException {
    FileSystemOptions opts = new FileSystemOptions();
    try {
      String accessKey = "";
      String secretKey = "";
      /* For legacy transformations containing AWS S3 access credentials, {@link Const#KETTLE_USE_AWS_DEFAULT_CREDENTIALS} can force Spoon to use
       * the Amazon Default Credentials Provider Chain instead of using the credentials embedded in the transformation metadata. */
      if ( !ValueMetaBase.convertStringToBoolean( Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_USE_AWS_DEFAULT_CREDENTIALS ), "N" ) ) ) {
        accessKey = System.getProperty( S3Util.ACCESS_KEY_SYSTEM_PROPERTY );
        secretKey = System.getProperty( S3Util.SECRET_KEY_SYSTEM_PROPERTY );
      } else {
        AWSCredentials credentials = S3CredentialsProvider.getAWSCredentials();
        if ( credentials != null ) {
          accessKey = credentials.getAWSAccessKeyId();
          secretKey = credentials.getAWSSecretKey();
        }
      }
      StaticUserAuthenticator userAuthenticator = new StaticUserAuthenticator( null, secretKey, accessKey );
      DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts, userAuthenticator );

    } catch ( SdkClientException e ) {
      throw new FileSystemException( e );
    }
    return opts;
  }

  private VariableSpace getVariableSpace() {
    if ( Spoon.getInstance().getActiveTransformation() != null ) {
      return Spoon.getInstance().getActiveTransformation();
    } else if ( Spoon.getInstance().getActiveJob() != null ) {
      return Spoon.getInstance().getActiveJob();
    } else {
      return new Variables();
    }
  }
}
