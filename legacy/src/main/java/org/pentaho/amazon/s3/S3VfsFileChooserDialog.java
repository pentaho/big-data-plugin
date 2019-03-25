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

import org.apache.commons.vfs2.FileObject;
import org.pentaho.amazon.AmazonS3FileSystemBootstrap;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * The UI for S3 VFS
 */
public class S3VfsFileChooserDialog extends S3VfsFileChooserBaseDialog {

  public S3VfsFileChooserDialog( VfsFileChooserDialog vfsFileChooserDialog, FileObject rootFile,
                                 FileObject initialFile ) {
    super( vfsFileChooserDialog, rootFile, initialFile, S3FileProvider.SCHEME, AmazonS3FileSystemBootstrap.getS3FileSystemDisplayText() );
  }

  @Override
  public void activate() {
    vfsFileChooserDialog.openFileCombo.setText( "s3://s3/" );
    super.activate();
  }
  /**
   * Build a URL given Url and Port provided by the user.
   *
   * @return
   * @TODO: relocate to a s3 helper class or similar
   */
  public String buildS3FileSystemUrlString() {
    return S3FileProvider.SCHEME + "://s3/";
  }

}
