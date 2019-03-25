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

import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridLayout;
import org.pentaho.amazon.AmazonS3FileSystemBootstrap;
import org.pentaho.amazon.AmazonSpoonPlugin;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * The UI for S3 VFS
 */
public class S3VfsFileChooserDialog extends CustomVfsUiPanel {

  private static Class<?> PKG = AmazonSpoonPlugin.class;

  private LogChannel log = new LogChannel( this );

  private FileObject rootFile;
  private FileObject initialFile;
  private VfsFileChooserDialog vfsFileChooserDialog;

  public S3VfsFileChooserDialog( VfsFileChooserDialog vfsFileChooserDialog, FileObject rootFile,
                                 FileObject initialFile ) {
    super( S3FileProvider.SCHEME, AmazonS3FileSystemBootstrap.getS3FileSystemDisplayText(), vfsFileChooserDialog,
      SWT.NONE );

    this.vfsFileChooserDialog = vfsFileChooserDialog;
    this.rootFile = rootFile;
    this.initialFile = initialFile;

    setLayout( new GridLayout() );
  }

  @Override
  public void activate() {
    vfsFileChooserDialog.setRootFile( rootFile );
    vfsFileChooserDialog.setInitialFile( initialFile );
    vfsFileChooserDialog.openFileCombo.setText( "s3://" );
    vfsFileChooserDialog.vfsBrowser.fileSystemTree.removeAll();
    super.activate();

    try {
      FileObject newRoot = resolveFile( vfsFileChooserDialog.openFileCombo.getText() );
      vfsFileChooserDialog.vfsBrowser.resetVfsRoot( newRoot );
    } catch ( FileSystemException ignored ) {
      //ignored
    }
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
    AWSCredentials credentials = S3CredentialsProvider.getAWSCredentials();
    if ( credentials != null ) {
      StaticUserAuthenticator userAuthenticator =
        new StaticUserAuthenticator( null, credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey() );
      DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts, userAuthenticator );
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
