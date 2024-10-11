/*! ******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.sqoop.ui;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.Schemes;
import org.pentaho.big.data.kettle.plugins.sqoop.AbstractSqoopJobEntry;
import org.pentaho.big.data.kettle.plugins.sqoop.SqoopImportConfig;
import org.pentaho.big.data.kettle.plugins.sqoop.SqoopImportJobEntry;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.util.Collection;

/**
 * Controller for the Sqoop Import Dialog.
 */
public class SqoopImportJobEntryController extends
    AbstractSqoopJobEntryController<SqoopImportConfig, SqoopImportJobEntry> {

  public static final String SQOOP_IMPORT_STEP_NAME = "sqoop-import";

  public SqoopImportJobEntryController( JobMeta jobMeta, XulDomContainer container, SqoopImportJobEntry sqoopJobEntry,
      BindingFactory bindingFactory ) {
    super( jobMeta, container, sqoopJobEntry, bindingFactory );
  }

  @Override
  public String getDialogElementId() {
    return "sqoop-import";
  }

  @Override
  protected void createBindings( SqoopImportConfig config, XulDomContainer container, BindingFactory bindingFactory,
      Collection<Binding> bindings ) {
    super.createBindings( config, container, bindingFactory, bindings );

    bindings.add( bindingFactory.createBinding( config, SqoopImportConfig.TARGET_DIR, SqoopImportConfig.TARGET_DIR, "value" ) );
  }

  public void browseForTargetDirectory() {
    try {
      String[] schemeRestrictions = new String[1];
      if ( selectedNamedCluster != null && !"false".equals( selectedNamedCluster.getVariable( "valid" ) ) ) {
        schemeRestrictions[0] = selectedNamedCluster.isMapr() ? Schemes.MAPRFS_SCHEME : Schemes.HDFS_SCHEME;
      } else {
        // must select cluster
        return;
      }

      String path = getConfig().getTargetDir();
      FileObject initialFile = getInitialFile( path );

      if ( initialFile == null ) {
        showErrorDialog( BaseMessages.getString( PKG, "Sqoop.JobEntry.Connection.Error.title" ),
            BaseMessages.getString( PKG, "Sqoop.JobEntry.Connection.error" ) );
        return;
      }

      FileObject targetDir =
          browseVfs( null, initialFile, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY, schemeRestrictions,
              false, schemeRestrictions[0], selectedNamedCluster, false, false );
      VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
      boolean okPressed = dialog.okPressed;
      if ( okPressed ) {
        getConfig().setTargetDir( targetDir != null ? targetDir.getName().getPath() : null );
        extractNamedClusterFromVfsFileChooser();
      }
    } catch ( KettleFileException | FileSystemException e ) {
      getJobEntry().logError( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorBrowsingDirectory" ), e );
    }
  }

  public void editNamedCluster() {
    editNamedCluster( SQOOP_IMPORT_STEP_NAME );
  }

  public void newNamedCluster() {
    newNamedCluster( SQOOP_IMPORT_STEP_NAME );
  }
}
