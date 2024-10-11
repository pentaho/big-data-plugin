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

package org.pentaho.di.ui.vfs;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * User: RFellows Date: 6/8/12
 */
public class VfsFileChooserHelper {
  private static final Logger logger = LoggerFactory.getLogger( VfsFileChooserHelper.class );
  private VfsFileChooserDialog fileChooserDialog = null;
  private Shell shell = null;
  private VariableSpace variableSpace = null;
  private FileSystemOptions fileSystemOptions = null;
  private String defaultScheme = "file";
  private String[] schemeRestrictions = null;
  private boolean showFileScheme = true;

  public VfsFileChooserHelper( Shell shell, VfsFileChooserDialog fileChooserDialog, VariableSpace variableSpace ) {
    this( shell, fileChooserDialog, variableSpace, new FileSystemOptions() );
  }

  public VfsFileChooserHelper( Shell shell, VfsFileChooserDialog fileChooserDialog, VariableSpace variableSpace,
      FileSystemOptions fileSystemOptions ) {
    this.fileChooserDialog = fileChooserDialog;
    this.shell = shell;
    this.variableSpace = variableSpace;
    this.fileSystemOptions = fileSystemOptions;
    this.schemeRestrictions = new String[0];
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri ) throws KettleException,
    FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, int fileDialogMode )
    throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, fileSystemOptions, fileDialogMode );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, int fileDialogMode,
      boolean showLocation ) throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, fileSystemOptions, fileDialogMode, showLocation, true );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, int fileDialogMode,
      boolean showLocation, boolean showCustomUI ) throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, fileSystemOptions, fileDialogMode, showLocation, showCustomUI );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, FileSystemOptions opts )
    throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, opts, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, FileSystemOptions opts,
      int fileDialogMode ) throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, opts, fileDialogMode, true, true );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, FileSystemOptions opts,
      int fileDialogMode, boolean showLocation, boolean showCustomUI ) throws KettleException, FileSystemException {
    // Get current file
    FileObject rootFile = null;
    FileObject initialFile = null;

    if ( fileUri != null ) {
      initialFile = KettleVFS.getFileObject( fileUri, variableSpace, opts );
    } else {
      initialFile = KettleVFS.getFileObject( Spoon.getInstance().getLastFileOpened() );
    }
    rootFile = initialFile.getFileSystem().getRoot();
    fileChooserDialog.setRootFile( rootFile );
    fileChooserDialog.setInitialFile( initialFile );
    fileChooserDialog.defaultInitialFile = rootFile;

    FileObject selectedFile = null;
    selectedFile = fileChooserDialog.open(
        shell, this.schemeRestrictions, getDefaultScheme(), showFileScheme(), initialFile.getName().getPath(),
        fileFilters, fileFilterNames, returnsUserAuthenticatedFileObjects(), fileDialogMode, showLocation, showCustomUI );

    return selectedFile;
  }

  public VariableSpace getVariableSpace() {
    return variableSpace;
  }

  public void setVariableSpace( VariableSpace variableSpace ) {
    this.variableSpace = variableSpace;
  }

  public FileSystemOptions getFileSystemOptions() {
    return fileSystemOptions;
  }

  public void setFileSystemOptions( FileSystemOptions fileSystemOptions ) {
    this.fileSystemOptions = fileSystemOptions;
  }

  public String getDefaultScheme() {
    return defaultScheme;
  }

  public void setDefaultScheme( String defaultScheme ) {
    this.defaultScheme = defaultScheme;
  }

  public String getSchemeRestriction() {
    String schemaRestriction = null;
    if ( this.schemeRestrictions != null && this.schemeRestrictions.length > 0 ) {
      schemaRestriction = this.schemeRestrictions[0];
    }
    return schemaRestriction;
  }

  public void setSchemeRestriction( String schemeRestriction ) {
    this.schemeRestrictions = new String[1];
    this.schemeRestrictions[0] = schemeRestriction;
  }

  public void setSchemeRestrictions( String[] schemeRestrictions ) {
    this.schemeRestrictions = schemeRestrictions;
  }

  public boolean showFileScheme() {
    return this.showFileScheme;
  }

  public void setShowFileScheme( boolean showFileScheme ) {
    this.showFileScheme = showFileScheme;
  }

  protected boolean returnsUserAuthenticatedFileObjects() {
    return false;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
    for ( CustomVfsUiPanel currentPanel : dialog.getCustomVfsUiPanels() ) {
      if ( currentPanel != null ) {
        try {
          Method setNamedCluster = currentPanel.getClass().getMethod( "setNamedCluster", new Class[] { String.class } );
          setNamedCluster.invoke( currentPanel, namedCluster.getName() );
        } catch ( NoSuchMethodException e ) {
          if ( logger.isDebugEnabled() ) {
            logger.debug( "Couldn't set named cluster " + namedCluster.getName() + " on " + currentPanel + " because it doesn't have setNamedCluster method.", e );
          }
        } catch ( InvocationTargetException e ) {
          if ( logger.isDebugEnabled() ) {
            logger.debug( "Couldn't set named cluster " + namedCluster.getName() + " on " + currentPanel + " because of exception.", e.getCause() );
          }
        } catch ( IllegalAccessException e ) {
          if ( logger.isDebugEnabled() ) {
            logger.debug( "Couldn't set named cluster " + namedCluster.getName() + " on " + currentPanel + " because setNamedCluster method isn't accessible.", e );
          }
        }
      }
    }
  }

  @VisibleForTesting
    VfsFileChooserDialog getFileChooserDialog() {
    return fileChooserDialog;
  }

  @VisibleForTesting
    Shell getShell() {
    return shell;
  }

  @VisibleForTesting
    String[] getSchemeRestrictions() {
    return schemeRestrictions;
  }

}
