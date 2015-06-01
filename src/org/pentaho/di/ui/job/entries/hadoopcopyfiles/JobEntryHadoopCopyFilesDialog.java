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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.pentaho.di.core.Const;
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
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.job.entries.copyfiles.JobEntryCopyFilesDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.vfs.hadoopvfsfilechooserdialog.HadoopVfsFileChooserDialog;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.s3.vfs.S3FileProvider;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class JobEntryHadoopCopyFilesDialog extends JobEntryCopyFilesDialog {
  private static Class<?> BASE_PKG = JobEntryCopyFiles.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  private static Class<?> PKG = JobEntryHadoopCopyFiles.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  private LogChannel log = new LogChannel( this );
  private JobEntryHadoopCopyFiles jobEntryHadoopCopyFiles;

  public static final String S3_ENVIRONMENT = "S3";

  public JobEntryHadoopCopyFilesDialog( Shell parent, JobEntryInterface jobEntryInt, Repository rep, JobMeta jobMeta ) {
    super( parent, jobEntryInt, rep, jobMeta );
    jobEntry = (JobEntryCopyFiles) jobEntryInt;
    jobEntryHadoopCopyFiles = (JobEntryHadoopCopyFiles) jobEntry;
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
        String clusterName = wFields.getActiveTableItem().getText( wFields.getActiveTableColumn() - 1 );
        setSelectedFile( path, clusterName );
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
          String clusterName = jobEntry.getConfigurationBy( sourceUrl );
          ti.setText( 1, STATIC_ENVIRONMENT );
          if ( clusterName != null ) {
            clusterName =
                clusterName.startsWith( JobEntryCopyFiles.LOCAL_SOURCE_FILE ) ? LOCAL_ENVIRONMENT : clusterName;
            clusterName =
                clusterName.startsWith( JobEntryCopyFiles.STATIC_SOURCE_FILE ) ? STATIC_ENVIRONMENT : clusterName;
            clusterName =
                clusterName.startsWith( JobEntryHadoopCopyFiles.S3_SOURCE_FILE ) ? S3_ENVIRONMENT : clusterName;

            ti.setText( 1, clusterName );
            sourceUrl =
                clusterName.equals( LOCAL_ENVIRONMENT ) || clusterName.equals( STATIC_ENVIRONMENT ) ||
                clusterName.equals( S3_ENVIRONMENT ) ? sourceUrl
                    : jobEntry.getUrlPath( sourceUrl );
          }
          ti.setText( 2, sourceUrl );
        }
        if ( jobEntry.wildcard[i] != null ) {
          ti.setText( 3, jobEntry.wildcard[i] );
        }
        if ( jobEntry.destination_filefolder[i] != null && !Const.isEmpty( jobEntry.destination_filefolder[i] ) ) {
          String destinationURL = jobEntry.destination_filefolder[i];
          String clusterName = jobEntry.getConfigurationBy( destinationURL );
          ti.setText( 4, STATIC_ENVIRONMENT );
          if ( clusterName != null ) {
            clusterName =
                clusterName.startsWith( JobEntryCopyFiles.LOCAL_DEST_FILE ) ? LOCAL_ENVIRONMENT : clusterName;
            clusterName =
                clusterName.startsWith( JobEntryCopyFiles.STATIC_DEST_FILE ) ? STATIC_ENVIRONMENT : clusterName;
            clusterName =
                clusterName.startsWith( JobEntryHadoopCopyFiles.S3_DEST_FILE ) ? S3_ENVIRONMENT : clusterName;
            ti.setText( 4, clusterName );
            destinationURL =
                clusterName.equals( LOCAL_ENVIRONMENT ) || clusterName.equals( STATIC_ENVIRONMENT ) ||
                clusterName.equals( S3_ENVIRONMENT ) ? destinationURL
                    : jobEntry.getUrlPath( destinationURL );
          }
          ti.setText( 5, destinationURL );
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

      String sourceNc = wFields.getNonEmpty( i ).getText( 1 );
      sourceNc = sourceNc.equals( LOCAL_ENVIRONMENT ) ? JobEntryCopyFiles.LOCAL_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( STATIC_ENVIRONMENT ) ? JobEntryCopyFiles.STATIC_SOURCE_FILE + i : sourceNc;
      sourceNc = sourceNc.equals( S3_ENVIRONMENT ) ? JobEntryHadoopCopyFiles.S3_SOURCE_FILE + i : sourceNc;
      String source = wFields.getNonEmpty( i ).getText( 2 );
      String wild = wFields.getNonEmpty( i ).getText( 3 );
      String destNc = wFields.getNonEmpty( i ).getText( 4 );
      destNc = destNc.equals( LOCAL_ENVIRONMENT ) ? JobEntryCopyFiles.LOCAL_DEST_FILE + i : destNc;
      destNc = destNc.equals( STATIC_ENVIRONMENT ) ? JobEntryCopyFiles.STATIC_DEST_FILE + i : destNc;
      destNc = destNc.equals( S3_ENVIRONMENT ) ? JobEntryHadoopCopyFiles.S3_DEST_FILE + i : destNc;
      String dest = wFields.getNonEmpty( i ).getText( 5 );

      if ( !Const.isEmpty( source ) && jobEntry.source_filefolder.length > 0 ) {
        jobEntry.source_filefolder[nr] =
            jobEntryHadoopCopyFiles.loadURL( source, sourceNc, getMetaStore(), namedClusterURLMappings );
        jobEntry.destination_filefolder[nr] =
            jobEntryHadoopCopyFiles.loadURL( dest, destNc, getMetaStore(), namedClusterURLMappings );
        jobEntry.wildcard[nr] = wild;
        nr++;
      }
    }
    jobEntry.setConfigurationMappings( namedClusterURLMappings );
    dispose();
  }

  private FileObject setSelectedFile( String path, String clusterName ) {

    FileObject selectedFile = null;

    try {
      // Get current file
      FileObject rootFile = null;
      FileObject initialFile = null;
      FileObject defaultInitialFile = null;

      if ( !clusterName.equals( LOCAL_ENVIRONMENT ) && !clusterName.equals( S3_ENVIRONMENT ) ) {
        NamedCluster namedCluster =
            NamedClusterManager.getInstance().getNamedClusterByName( clusterName, getMetaStore() );
        if ( Const.isEmpty( path ) ) {
          path = "/";
        }
        if ( namedCluster == null ) {
          return null;
        }
        if ( namedCluster.isMapr() ) {
          path = HadoopSpoonPlugin.MAPRFS_SCHEME + "://" + path;
        } else {
          path = NamedClusterManager.getInstance().processURLsubstitution(
              clusterName, path, HadoopSpoonPlugin.HDFS_SCHEME, getMetaStore(), jobMeta );
        }
      }

      boolean resolvedInitialFile = false;
      
      if ( path != null ) {

        String fileName = jobMeta.environmentSubstitute( path );

        if ( fileName != null && !fileName.equals( "" ) ) {
          try {
            initialFile = KettleVFS.getFileObject( fileName );
            resolvedInitialFile = true;
          } catch ( Exception e ) {
            showMessageAndLog( BaseMessages.getString( PKG, "JobHadoopCopyFiles.Connection.Error.title" ), BaseMessages.getString(
                PKG, "JobHadoopCopyFiles.Connection.error" ), e.getMessage() );
            return null;
          }
          File startFile = new File( System.getProperty( "user.home" ) );
          defaultInitialFile = KettleVFS.getFileObject( startFile.getAbsolutePath() );
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

      if ( clusterName.equals( LOCAL_ENVIRONMENT ) ) {
        selectedFile =
            fileChooserDialog.open( shell, new String[] { "file" }, "file", true, path, new String[] { "*.*" },
                FILETYPES, false, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );
      } else if ( clusterName.equals( S3_ENVIRONMENT ) ) {
        selectedFile =
            fileChooserDialog.open( shell, new String[] { S3FileProvider.SCHEME }, S3FileProvider.SCHEME, true, 
                path, new String[] { "*.*" }, FILETYPES, false, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY,
                false, true );
      } else {
        NamedCluster namedCluster =
            NamedClusterManager.getInstance().getNamedClusterByName( clusterName, getMetaStore() );
        if ( namedCluster != null ) {
          if ( namedCluster.isMapr() ) {
            selectedFile =
                fileChooserDialog.open( shell, new String[] { HadoopSpoonPlugin.MAPRFS_SCHEME },
                    HadoopSpoonPlugin.MAPRFS_SCHEME, false, path, new String[] { "*.*" }, FILETYPES, true,
                    VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );
          } else {
            List<CustomVfsUiPanel> customPanels = fileChooserDialog.getCustomVfsUiPanels();
            for ( CustomVfsUiPanel panel : customPanels ) {
              if ( panel instanceof HadoopVfsFileChooserDialog ) {
                HadoopVfsFileChooserDialog hadoopDialog = ( (HadoopVfsFileChooserDialog) panel );
                namedClusterWidget = hadoopDialog.getNamedClusterWidget();
                namedClusterWidget.initiate();
                hadoopDialog.setNamedCluster( clusterName );
                hadoopDialog.initializeConnectionPanel( initialFile );
              }
            }
            if ( resolvedInitialFile ) {
              fileChooserDialog.initialFile = initialFile;
            }
            selectedFile =
                fileChooserDialog.open( shell, new String[] { HadoopSpoonPlugin.HDFS_SCHEME },
                    HadoopSpoonPlugin.HDFS_SCHEME, false, path, new String[] { "*.*" }, FILETYPES, true,
                    VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY, false, false );
          }
        }
      }

      CustomVfsUiPanel currentPanel = fileChooserDialog.getCurrentPanel();
      if ( currentPanel instanceof HadoopVfsFileChooserDialog ) {
        namedClusterWidget = ( (HadoopVfsFileChooserDialog) currentPanel ).getNamedClusterWidget();
      }

      if ( selectedFile != null ) {
        String url = selectedFile.getURL().toString();
        if ( currentPanel.getVfsSchemeDisplayText().equals( LOCAL_ENVIRONMENT ) ) {
          wFields.getActiveTableItem().setText( wFields.getActiveTableColumn() - 1, LOCAL_ENVIRONMENT );
        } else if ( currentPanel.getVfsSchemeDisplayText().equals( S3_ENVIRONMENT ) ) {
          wFields.getActiveTableItem().setText( wFields.getActiveTableColumn() - 1, S3_ENVIRONMENT );
        } else if ( namedClusterWidget != null && namedClusterWidget.getSelectedNamedCluster() != null ) {
          url = jobEntry.getUrlPath( url );
          wFields.getActiveTableItem().setText( wFields.getActiveTableColumn() - 1, 
              namedClusterWidget.getSelectedNamedCluster().getName() );
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

  private void showMessageAndLog( String title, String message, String messageToLog ) {
    MessageBox box = new MessageBox( shell );
    box.setText( title ); //$NON-NLS-1$
    box.setMessage( message );
    log.logError( messageToLog );
    box.open();
  }
  
  protected Image getImage() {
    return GUIResource.getInstance().getImage( "HDM.svg", getClass().getClassLoader(), ConstUI.ICON_SIZE,
        ConstUI.ICON_SIZE );
  }

  public boolean showFileButtons() {
    return false;
  }

  protected void setComboValues( ColumnInfo colInfo ) {
    try {
      super.setComboValues( colInfo );
      String[] superValues = colInfo.getComboValues();
      
      String[] s3value = { S3_ENVIRONMENT };
      String[] comboValues = (String[]) ArrayUtils.addAll( superValues, s3value );
      
      String[] namedClusters =
          NamedClusterManager.getInstance().listNames( getMetaStore() ).toArray( new String[0] );
      String[] values = (String[]) ArrayUtils.addAll( comboValues, namedClusters );
      colInfo.setComboValues( values );
    } catch ( MetaStoreException e ) {
      log.logError( e.getMessage() );
    }
  }
}
