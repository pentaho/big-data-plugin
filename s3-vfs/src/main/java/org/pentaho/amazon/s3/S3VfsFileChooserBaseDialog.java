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
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridLayout;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.s3n.vfs.S3NFileProvider;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;
import org.pentaho.di.core.Props;

/**
 * The UI for S3 VFS
 */
public abstract class S3VfsFileChooserBaseDialog extends CustomVfsUiPanel {

  protected FileObject rootFile;
  protected FileObject initialFile;
  protected VfsFileChooserDialog vfsFileChooserDialog;

  public S3VfsFileChooserBaseDialog( VfsFileChooserDialog vfsFileChooserDialog, FileObject rootFile,
                                  FileObject initialFile, String schema, String fileSystemDisplayText ) {
    super( schema, fileSystemDisplayText, vfsFileChooserDialog, SWT.NONE );

    this.vfsFileChooserDialog = vfsFileChooserDialog;
    this.rootFile = rootFile;
    this.initialFile = initialFile;

    setLayout( new GridLayout() );
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
