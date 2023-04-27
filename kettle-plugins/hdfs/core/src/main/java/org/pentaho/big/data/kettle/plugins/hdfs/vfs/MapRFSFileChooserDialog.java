/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class MapRFSFileChooserDialog extends CustomVfsUiPanel {

  private VfsFileChooserDialog vfsFileChooserDialog;

  public MapRFSFileChooserDialog( String schemeName, String displayName, VfsFileChooserDialog vfsFileChooserDialog ) {
    super( schemeName, displayName, vfsFileChooserDialog, SWT.NONE );
    this.vfsFileChooserDialog = vfsFileChooserDialog;
  }

  public void activate() {
    vfsFileChooserDialog.setRootFile( null );
    vfsFileChooserDialog.setInitialFile( null );
    vfsFileChooserDialog.openFileCombo.setText( "maprfs://" );
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

  protected FileSystemOptions getFileSystemOptions() throws FileSystemException {
    FileSystemOptions opts = new FileSystemOptions();
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
