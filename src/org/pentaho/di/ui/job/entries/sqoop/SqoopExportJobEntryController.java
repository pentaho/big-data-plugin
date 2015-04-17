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

package org.pentaho.di.ui.job.entries.sqoop;

import static org.pentaho.di.job.entries.sqoop.SqoopExportConfig.EXPORT_DIR;

import java.util.Collection;

import org.apache.commons.vfs.FileObject;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.sqoop.AbstractSqoopJobEntry;
import org.pentaho.di.job.entries.sqoop.SqoopExportConfig;
import org.pentaho.di.job.entries.sqoop.SqoopExportJobEntry;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

/**
 * Controller for the Sqoop Export Dialog.
 */
public class SqoopExportJobEntryController extends
    AbstractSqoopJobEntryController<SqoopExportConfig, SqoopExportJobEntry> {

  public SqoopExportJobEntryController( JobMeta jobMeta, XulDomContainer container, SqoopExportJobEntry sqoopJobEntry,
      BindingFactory bindingFactory ) {
    super( jobMeta, container, sqoopJobEntry, bindingFactory );
  }

  @Override
  public String getDialogElementId() {
    return "sqoop-export";
  }

  @Override
  protected void createBindings( SqoopExportConfig config, XulDomContainer container, BindingFactory bindingFactory,
      Collection<Binding> bindings ) {
    super.createBindings( config, container, bindingFactory, bindings );

    bindings.add( bindingFactory.createBinding( config, EXPORT_DIR, EXPORT_DIR, "value" ) );
  }

  public void browseForExportDirectory() {
    FileObject path = null;
    // TODO Build proper URL for path
    // path = resolveFile(getConfig().getExportDir());
    try {
      String[] schemeRestrictions = new String[2];
      schemeRestrictions[0] = HadoopSpoonPlugin.HDFS_SCHEME;
      schemeRestrictions[1] = HadoopSpoonPlugin.MAPRFS_SCHEME;
      
      FileObject exportDir =
          browseVfs( null, path, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY, schemeRestrictions,
              false, HadoopSpoonPlugin.HDFS_SCHEME, selectedNamedCluster, false );
      VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
      boolean okPressed = dialog.okPressed;
      if ( okPressed ) {
        getConfig().setExportDir( exportDir != null ? exportDir.getName().getPath() : null);
        extractNamedClusterFromVfsFileChooser();
      }
    } catch ( KettleFileException e ) {
      getJobEntry().logError( BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorBrowsingDirectory" ), e );
    }
  }

  @Override
  public void accept() {
    // Set the database meta based on the current database
    jobEntry.setDatabaseMeta( jobMeta.findDatabase( config.getDatabase() ) );
    super.accept();
  }
  
  public void editNamedCluster() {
    if ( isSelectedNamedCluster() ) {
      XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "sqoop-export" );
      Shell shell = (Shell) xulDialog.getRootObject();
      ncDelegate.editNamedCluster( null, selectedNamedCluster, shell );
      populateNamedClusters();
    }
  }

  public void newNamedCluster() {
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( "sqoop-export" );
    Shell shell = (Shell) xulDialog.getRootObject();
    ncDelegate.newNamedCluster( jobMeta, null, shell );
    populateNamedClusters();
  }  
}
