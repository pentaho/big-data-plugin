/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.impl.browse;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.pentaho.big.data.impl.browse.model.NamedClusterDirectory;
import org.pentaho.big.data.impl.browse.model.NamedClusterFile;
import org.pentaho.big.data.impl.browse.model.NamedClusterTree;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.plugins.fileopensave.api.providers.BaseFileProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.File;
import org.pentaho.di.plugins.fileopensave.api.providers.Tree;
import org.pentaho.di.plugins.fileopensave.api.providers.Utils;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileException;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileNotFoundException;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class NamedClusterProvider extends BaseFileProvider<NamedClusterFile> {

  public static final String NAME = "Hadoop Clusters";
  public static final String TYPE = "clusters";
  public static final String SCHEME = "hc";

  private NamedClusterService namedClusterManager;
  private MetastoreLocator metastoreLocator;

  public NamedClusterProvider( NamedClusterService namedClusterManager, MetastoreLocator metastoreLocator ) {
    this.namedClusterManager = namedClusterManager;
    this.metastoreLocator = metastoreLocator;
  }

  @Override public String getName() {
    return NAME;
  }

  @Override public String getType() {
    return TYPE;
  }

  @Override public Class<NamedClusterFile> getFileClass() {
    return NamedClusterFile.class;
  }

  @Override public boolean isAvailable() {
    return true;
  }

  @Override public Tree getTree() {
    NamedClusterTree namedClusterTree = new NamedClusterTree( NAME );

    try {
      List<String> names = namedClusterManager.listNames( metastoreLocator.getMetastore() );
      names.forEach( name -> {
        NamedClusterDirectory namedClusterDirectory = new NamedClusterDirectory();
        namedClusterDirectory.setName( name );
        namedClusterDirectory.setPath( SCHEME + "://" + name );
        namedClusterDirectory.setRoot( NAME );
        namedClusterDirectory.setHasChildren( true );
        namedClusterDirectory.setCanDelete( false );
        namedClusterTree.addChild( namedClusterDirectory );
      } );
    } catch ( MetaStoreException me ) {
      // ignored
    }

    return namedClusterTree;
  }

  @Override public List<NamedClusterFile> getFiles( NamedClusterFile file, String filters, VariableSpace space ) throws FileException {
    FileObject fileObject;
    try {
      fileObject = KettleVFS.getFileObject( file.getPath() );
      if ( !fileObject.exists() ) {
        throw new FileNotFoundException( file.getPath(), TYPE );
      }
    } catch ( KettleFileException | FileSystemException e ) {
      throw new FileNotFoundException( file.getPath(), TYPE );
    }
    return populateChildren( file, fileObject, filters );
  }

  /**
   * Check if a file object has children
   *
   * @param fileObject
   * @return
   */
  private boolean hasChildren( FileObject fileObject ) {
    try {
      return fileObject != null && fileObject.getType().hasChildren();
    } catch ( FileSystemException e ) {
      return false;
    }
  }

  /**
   * Get the children if they are available, if an error return an empty list
   *
   * @param fileObject
   * @return
   */
  private FileObject[] getChildren( FileObject fileObject ) {
    try {
      return fileObject != null ? fileObject.getChildren() : new FileObject[] {};
    } catch ( FileSystemException e ) {
      return new FileObject[] {};
    }
  }

  /**
   * Populate Named Cluster file objects from named cluster FileObject types
   *
   * @param parent
   * @param fileObject
   * @param filters
   * @return
   */
  private List<NamedClusterFile> populateChildren( NamedClusterFile parent, FileObject fileObject, String filters ) {
    List<NamedClusterFile> files = new ArrayList<>();
    if ( fileObject != null && hasChildren( fileObject ) ) {
      FileObject[] children = getChildren( fileObject );
      for ( FileObject child : children ) {
        if ( hasChildren( child ) ) {
          files.add( NamedClusterDirectory.create( parent.getPath(), child ) );
        } else {
          if ( child != null && Utils.matches( child.getName().getBaseName(), filters ) ) {
            files.add( NamedClusterFile.create( parent.getPath(), child ) );
          }
        }
      }
    }
    return files;
  }

  @Override public List<NamedClusterFile> delete( List<NamedClusterFile> files, VariableSpace space ) throws FileException {
    List<NamedClusterFile> deletedFiles = new ArrayList<>();
    for ( NamedClusterFile file : files ) {
      try {
        FileObject fileObject = KettleVFS.getFileObject( file.getPath() );
        if ( fileObject.delete() ) {
          deletedFiles.add( file );
        }
      } catch ( KettleFileException | FileSystemException kfe ) {
        // Ignore don't add
      }
    }
    return deletedFiles;
  }

  @Override public NamedClusterFile add( NamedClusterFile folder, VariableSpace space ) throws FileException {
    try {
      FileObject fileObject = KettleVFS.getFileObject( folder.getPath() );
      fileObject.createFolder();
      String parent = folder.getPath().substring( 0, folder.getPath().length() - 1 );
      return NamedClusterFile.create( parent, fileObject );
    } catch ( KettleFileException | FileSystemException ignored ) {
      // Ignored
    }
    return null;
  }

  @Override public NamedClusterFile getFile( NamedClusterFile file, VariableSpace space ) {
    try {
      FileObject fileObject = KettleVFS.getFileObject( file.getPath() );
      if ( fileObject.getType().equals( FileType.FOLDER ) ) {
        return NamedClusterDirectory.create( null, fileObject );
      } else {
        return NamedClusterFile.create( null, fileObject );
      }
    } catch ( KettleFileException | FileSystemException e ) {
      // File does not exist
    }
    return null;
  }

  @Override public boolean fileExists( NamedClusterFile dir, String path, VariableSpace space ) throws FileException {
    path = sanitizeName( dir, path );
    try {
      FileObject fileObject = KettleVFS.getFileObject( path );
      return fileObject.exists();
    } catch ( KettleFileException | FileSystemException e ) {
      throw new FileException();
    }
  }

  @Override public String getNewName( NamedClusterFile destDir, String newPath, VariableSpace space ) throws FileException {
    String extension = Utils.getExtension( newPath );
    String parent = Utils.getParent( newPath );
    String name = Utils.getName( newPath ).replace( "." + extension, "" );
    int i = 1;
    String testName = sanitizeName( destDir, newPath );
    try {
      while ( KettleVFS.getFileObject( testName ).exists() ) {
        if ( Utils.isValidExtension( extension ) ) {
          testName = sanitizeName( destDir, parent + name + " " + i + "." + extension );
        } else {
          testName = sanitizeName( destDir, newPath + " " + i );
        }
        i++;
      }
    } catch ( KettleFileException | FileSystemException e ) {
      return testName;
    }
    return testName;
  }

  @Override public boolean isSame( File file1, File file2 ) {
    return file1 instanceof NamedClusterFile && file2 instanceof NamedClusterFile;
  }

  @Override public NamedClusterFile rename( NamedClusterFile file, String newPath, boolean overwrite, VariableSpace space )
    throws FileException {
    return doMove( file, newPath, overwrite );
  }

  @Override public NamedClusterFile copy( NamedClusterFile file, String toPath, boolean b, VariableSpace space )
    throws FileException {
    try {
      FileObject fileObject = KettleVFS.getFileObject( file.getPath() );
      FileObject copyObject = KettleVFS.getFileObject( toPath );
      copyObject.copyFrom( fileObject, Selectors.SELECT_SELF );
      if ( file instanceof NamedClusterDirectory ) {
        return NamedClusterDirectory.create( copyObject.getParent().getPublicURIString(), fileObject );
      } else {
        return NamedClusterFile.create( copyObject.getParent().getPublicURIString(), fileObject );
      }
    } catch ( KettleFileException | FileSystemException e ) {
      throw new FileException();
    }
  }

  @Override public NamedClusterFile move( NamedClusterFile namedClusterFile, String s, boolean b, VariableSpace space )
    throws FileException {
    return null;
  }

  private NamedClusterFile doMove( NamedClusterFile file, String newPath, boolean overwrite ) {
    try {
      FileObject fileObject = KettleVFS.getFileObject( file.getPath() );
      FileObject renameObject = KettleVFS.getFileObject( newPath );
      if ( overwrite && renameObject.exists() ) {
        renameObject.delete();
      }
      fileObject.moveTo( renameObject );
      if ( file instanceof NamedClusterDirectory ) {
        return NamedClusterDirectory.create( renameObject.getParent().getPublicURIString(), renameObject );
      } else {
        return NamedClusterFile.create( renameObject.getParent().getPublicURIString(), renameObject );
      }
    } catch ( KettleFileException | FileSystemException e ) {
      return null;
    }
  }

  @Override public InputStream readFile( NamedClusterFile file, VariableSpace space ) throws FileException {
    try {
      FileObject fileObject = KettleVFS.getFileObject( file.getPath() );
      return fileObject.getContent().getInputStream();
    } catch ( KettleFileException | FileSystemException e ) {
      return null;
    }
  }

  @Override
  public NamedClusterFile writeFile( InputStream inputStream, NamedClusterFile destDir, String path, boolean overwrite,
                                     VariableSpace space ) throws FileException {
    FileObject fileObject = null;
    try {
      fileObject = KettleVFS.getFileObject( path );
    } catch ( KettleFileException ke ) {
      throw new FileException();
    }
    if ( fileObject != null ) {
      try ( OutputStream outputStream = fileObject.getContent().getOutputStream(); ) {
        IOUtils.copy( inputStream, outputStream );
        outputStream.flush();
        return NamedClusterFile.create( destDir.getPath(), fileObject );
      } catch ( IOException e ) {
        return null;
      }
    }
    return null;
  }

  @Override public NamedClusterFile getParent( NamedClusterFile file ) {
    NamedClusterFile vfsFile = new NamedClusterFile();
    vfsFile.setPath( file.getParent() );
    return vfsFile;
  }

  @Override public void clearProviderCache() {
    // Nothing to clear
  }
}
