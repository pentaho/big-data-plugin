/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.vfs2.FileSystemOptions;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.vfs.VfsFileChooserHelper;
import org.pentaho.s3n.vfs.S3NFileProvider;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * created by: rfellows date: 5/24/12
 */
public class S3NVfsFileChooserHelper extends VfsFileChooserHelper {

  public S3NVfsFileChooserHelper( Shell shell, VfsFileChooserDialog fileChooserDialog, VariableSpace variableSpace ) {
    super( shell, fileChooserDialog, variableSpace );
    setDefaultScheme( S3NFileProvider.SCHEME );
    setSchemeRestriction( S3NFileProvider.SCHEME );
  }

  public S3NVfsFileChooserHelper( Shell shell, VfsFileChooserDialog fileChooserDialog, VariableSpace variableSpace,
      FileSystemOptions fileSystemOptions ) {
    super( shell, fileChooserDialog, variableSpace, fileSystemOptions );
    setDefaultScheme( S3NFileProvider.SCHEME );
    setSchemeRestriction( S3NFileProvider.SCHEME );
  }

  @Override
  protected boolean returnsUserAuthenticatedFileObjects() {
    return true;
  }
}
