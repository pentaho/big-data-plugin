/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog;

import org.apache.commons.vfs.FileObject;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Combo;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsBrowser;
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
  }
}
