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

package org.pentaho.di.job.entries.hadoopcopyfiles;

import java.util.List;

import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.di.ui.core.namedconfig.NamedConfigurationWidget;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog.HadoopVfsFileChooserDialog;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

@JobEntry( id = "HadoopCopyFilesPlugin", image = "HDM.png", name = "HadoopCopyFilesPlugin.Name",
    description = "HadoopCopyFilesPlugin.Description",
    categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
    i18nPackageName = "org.pentaho.di.job.entries.hadoopcopyfiles" )
public class JobEntryHadoopCopyFiles extends JobEntryCopyFiles {

  public JobEntryHadoopCopyFiles() {
    this( "" ); //$NON-NLS-1$
  }

  public JobEntryHadoopCopyFiles( String name ) {
    super( name );
    
    VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
    List<CustomVfsUiPanel> customPanels = dialog.getCustomVfsUiPanels();
    for( CustomVfsUiPanel panel : customPanels ) {
      if( panel instanceof HadoopVfsFileChooserDialog ) {
        NamedConfigurationWidget namedConfigurationWidget = ( ( HadoopVfsFileChooserDialog ) panel ).getNamedConfigurationWidget();
        namedConfigurationWidget.initiate();
      }
    }
  }
}
