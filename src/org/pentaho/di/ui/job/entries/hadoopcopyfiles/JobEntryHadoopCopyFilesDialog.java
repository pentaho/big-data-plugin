/*! ******************************************************************************
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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.ui.job.entries.hadoopcopyfiles;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.namedcluster.NamedClusterManager;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.di.job.entries.hadoopcopyfiles.JobEntryHadoopCopyFiles;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.namedcluster.NamedClusterWidget;
import org.pentaho.di.ui.job.entries.copyfiles.JobEntryCopyFilesDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog.HadoopVfsFileChooserDialog;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class JobEntryHadoopCopyFilesDialog extends JobEntryCopyFilesDialog {
  private static Class<?> BASE_PKG = JobEntryCopyFiles.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  private static Class<?> PKG = JobEntryHadoopCopyFiles.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private LogChannel log = new LogChannel( this );

  private Map<String, String> transientMappings = null;
  private NamedClusterManager namedClusterManager = NamedClusterManager.getInstance();

  public JobEntryHadoopCopyFilesDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (JobEntryCopyFiles) jobEntryInt;
    transientMappings = ( ( JobEntryHadoopCopyFiles ) jobEntry ).getNamedClusterURLMapping();
    if ( this.jobEntry.getName() == null ) {
      this.jobEntry.setName( BaseMessages.getString( BASE_PKG, "JobCopyFiles.Name.Default" ) );
    }
  }

  protected void initUI() {
    super.initUI();
    shell.setText( BaseMessages.getString( PKG, "JobHadoopCopyFiles.Title" ) );
  }

  protected SelectionAdapter getFileSelectionAdapter() {
    return new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        String path = wFields.getActiveTableItem().getText( wFields.getActiveTableColumn() );
        setSelectedFile( path );
      }
    };
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( jobEntry.getName() != null ) {
      wName.setText( jobEntry.getName() );
    }
    wName.selectAll();
    wCopyEmptyFolders.setSelection( jobEntry.copy_empty_folders );
    
    if ( jobEntry.source_filefolder != null ) {
      for ( int i = 0; i < jobEntry.source_filefolder.length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( jobEntry.source_filefolder[i] != null ) {
          String sourceUrl = jobEntry.source_filefolder[i];
          String clusterName = transientMappings.get( sourceUrl );
          sourceUrl = 
              namedClusterManager.processURLsubstitution(
                  clusterName, sourceUrl, HadoopSpoonPlugin.HDFS_SCHEME, getMetaStore(), jobEntry );
          ti.setText( 1, sourceUrl );
        }
        if ( jobEntry.destination_filefolder[i] != null ) {
          String destinationURL = jobEntry.destination_filefolder[i];
          String clusterName = transientMappings.get( destinationURL );
          destinationURL =
              namedClusterManager.processURLsubstitution(
                  clusterName, destinationURL, HadoopSpoonPlugin.HDFS_SCHEME, getMetaStore(), jobEntry );
          ti.setText( 2, destinationURL );
        }
        if ( jobEntry.wildcard[i] != null ) {
          ti.setText( 3, jobEntry.wildcard[i] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
    wPrevious.setSelection( jobEntry.arg_from_previous );
    wOverwriteFiles.setSelection( jobEntry.overwrite_files );
    wIncludeSubfolders.setSelection( jobEntry.include_subfolders );
    wRemoveSourceFiles.setSelection( jobEntry.remove_source_files );
    wDestinationIsAFile.setSelection( jobEntry.destination_is_a_file );
    wCreateDestinationFolder.setSelection( jobEntry.create_destination_folder );

    wAddFileToResult.setSelection( jobEntry.add_result_filesname );

  }

  protected void ok() {
    if ( Const.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( BASE_PKG, "System.StepJobEntryNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( BASE_PKG, "System.JobEntryNameMissing.Msg" ) );
      mb.open();
      return;
    }

    jobEntry.setName( wName.getText() );
    jobEntry.setCopyEmptyFolders( wCopyEmptyFolders.getSelection() );
    jobEntry.setoverwrite_files( wOverwriteFiles.getSelection() );
    jobEntry.setIncludeSubfolders( wIncludeSubfolders.getSelection() );
    jobEntry.setArgFromPrevious( wPrevious.getSelection() );
    jobEntry.setRemoveSourceFiles( wRemoveSourceFiles.getSelection() );
    jobEntry.setAddresultfilesname( wAddFileToResult.getSelection() );
    jobEntry.setDestinationIsAFile( wDestinationIsAFile.getSelection() );
    jobEntry.setCreateDestinationFolder( wCreateDestinationFolder.getSelection() );

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    Map<String, String> namedClusterURLMappings = new HashMap<String, String>();
    jobEntry.source_filefolder = new String[nr];
    jobEntry.destination_filefolder = new String[nr];
    jobEntry.wildcard = new String[nr];
    nr = 0;
    for ( int i = 0; i < nritems; i++ ) {
      String source = wFields.getNonEmpty( i ).getText( 1 );
      String dest = wFields.getNonEmpty( i ).getText( 2 );
      String wild = wFields.getNonEmpty( i ).getText( 3 );
      if ( source != null && source.length() != 0 ) {
        jobEntry.source_filefolder[nr] = source;
        processNamedClusterURLMapping( source, namedClusterURLMappings );
        jobEntry.destination_filefolder[nr] = dest;
        processNamedClusterURLMapping( dest, namedClusterURLMappings );
        jobEntry.wildcard[nr] = wild;
        nr++;
      }
    }
    ( (JobEntryHadoopCopyFiles) jobEntry ).setNamedClusterURLMapping( namedClusterURLMappings );
    dispose();
  }

  private void processNamedClusterURLMapping( String locationURL, Map<String, String> namedClusterURLMappings ) {
    // The locationURL has to correspond to a NamedCluster otherwise it was modified by the user
    // thus breaking the URL/NamedCluster link.
    String cluster = transientMappings.get( locationURL );
    if ( cluster != null ) {
      namedClusterURLMappings.put( locationURL, cluster );
    } else {
      // The locationURL was modified thus the link to the NamedCluster is lost.
      namedClusterURLMappings.put( locationURL, "" );
    }
  }

  private FileObject setSelectedFile( String path ) {

    FileObject selectedFile = null;

    try {
      // Get current file
      FileObject rootFile = null;
      FileObject initialFile = null;
      FileObject defaultInitialFile = null;

      if ( path != null ) {

        String fileName = jobMeta.environmentSubstitute( path );

        if ( fileName != null && !fileName.equals( "" ) ) {
          try {
            initialFile = KettleVFS.getFileObject( fileName );
          } catch ( KettleException e ) {
            initialFile = KettleVFS.getFileObject( "" );
          }
          defaultInitialFile = KettleVFS.getFileObject( "file:///c:/" );
          rootFile = initialFile.getFileSystem().getRoot();
        } else {
          defaultInitialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
        }
      }

      if ( rootFile == null ) {
        rootFile = defaultInitialFile.getFileSystem().getRoot();
        initialFile = defaultInitialFile;
      }
      VfsFileChooserDialog fileChooserDialog = Spoon.getInstance().getVfsFileChooserDialog( rootFile, initialFile );
      fileChooserDialog.defaultInitialFile = defaultInitialFile;
      
      NamedClusterWidget namedClusterWidget = null;
      List<CustomVfsUiPanel> customPanels = fileChooserDialog.getCustomVfsUiPanels();
      String ncName = null;
      HadoopVfsFileChooserDialog hadoopDialog = null;
      for( CustomVfsUiPanel panel : customPanels ) {
        if( panel instanceof HadoopVfsFileChooserDialog ) {
          hadoopDialog = ( ( HadoopVfsFileChooserDialog ) panel );
          namedClusterWidget = hadoopDialog.getNamedClusterWidget();
          namedClusterWidget.initiate();
          ncName = null;
          if ( initialFile != null ) {
            ncName = transientMappings.get( initialFile.getURL().toString() );
          } 
          hadoopDialog.setNamedCluster( ncName );
          hadoopDialog.initializeConnectionPanel( initialFile );
        }
      }
      
      selectedFile =
          fileChooserDialog.open( shell, null, HadoopSpoonPlugin.HDFS_SCHEME, true, null, new String[] { "*.*" },
              FILETYPES, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY );
      
      CustomVfsUiPanel currentPanel = fileChooserDialog.getCurrentPanel();
      if( currentPanel instanceof HadoopVfsFileChooserDialog ) {
        namedClusterWidget = ( ( HadoopVfsFileChooserDialog ) currentPanel ).getNamedClusterWidget();
      }

      if ( selectedFile != null ) {
        String url = selectedFile.getURL().toString();
        NamedCluster nc = namedClusterWidget.getSelectedNamedCluster();
        if ( nc != null ) {
          url = 
              namedClusterManager.processURLsubstitution(
                  nc.getName(), url, HadoopSpoonPlugin.HDFS_SCHEME, getMetaStore(), jobEntry );
          transientMappings.put( url, nc.getName() );
        }
        
        wFields.getActiveTableItem().setText( wFields.getActiveTableColumn(), url );
      }
      
      return selectedFile;

    } catch ( KettleFileException ex ) {
      log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.KettleFileException" ) );
      return selectedFile;
    } catch ( FileSystemException ex ) {
      log.logError( BaseMessages.getString( PKG, "HadoopFileInputDialog.FileBrowser.FileSystemException" ) );
      return selectedFile;
    }
  }

  protected Image getImage() {
    return GUIResource.getInstance().getImage( "HDM.svg", getClass().getClassLoader(), ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );    
  }  
  
  public boolean showFileButtons() {
    return false;
  }  
  
}