/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
import org.pentaho.di.core.bowl.Bowl;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.plugins.fileopensave.api.overwrite.OverwriteStatus;
import org.pentaho.di.plugins.fileopensave.api.providers.BaseFileProvider;
import org.pentaho.di.plugins.fileopensave.api.providers.File;
import org.pentaho.di.plugins.fileopensave.api.providers.Tree;
import org.pentaho.di.plugins.fileopensave.api.providers.Utils;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileException;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.FileNotFoundException;
import org.pentaho.di.plugins.fileopensave.api.providers.exception.ProviderServiceInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class NamedClusterProvider extends BaseFileProvider<NamedClusterFile> {

  public static final String NAME = "Hadoop Clusters";
  public static final String TYPE = "clusters";
  public static final String SCHEME = "hc";

  private NamedClusterService namedClusterManager;
  private MetastoreLocator metastoreLocator;
  private Logger logger = LoggerFactory.getLogger( NamedClusterProvider.class );

  public NamedClusterProvider( NamedClusterService namedClusterManager ) {
    this.namedClusterManager = namedClusterManager;
    try {
      Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
      this.metastoreLocator = metastoreLocators.stream().findFirst().get();
    } catch ( Exception e ) {
      logger.error( "Error getting MetastoreLocator", e );
    }
    try {
      Collection<ProviderServiceInterface> providerServiceInterfaces = PluginServiceLoader.loadServices( ProviderServiceInterface.class );
      providerServiceInterfaces.stream().findFirst().get().addProviderService( this );
    } catch ( Exception e ) {
      logger.error( "Error registering Hadoop Clusters file provider", e );
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public Class<NamedClusterFile> getFileClass() {
    return NamedClusterFile.class;
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public Tree getTree( Bowl bowl ) {
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

  @Override
  public List<NamedClusterFile> getFiles( Bowl bowl, NamedClusterFile file, String filters,
                                          VariableSpace space ) throws FileException {
    FileObject fileObject;
    try {
      fileObject = KettleVFS.getInstance( bowl ).getFileObject( file.getPath() );
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

  @Override
  public List<NamedClusterFile> delete( Bowl bowl, List<NamedClusterFile> files, VariableSpace space )
    throws FileException {
    List<NamedClusterFile> deletedFiles = new ArrayList<>();
    for ( NamedClusterFile file : files ) {
      try {
        FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( file.getPath() );
        if ( fileObject.delete() ) {
          deletedFiles.add( file );
        }
      } catch ( KettleFileException | FileSystemException kfe ) {
        // Ignore don't add
      }
    }
    return deletedFiles;
  }

  @Override
  public NamedClusterFile add( Bowl bowl, NamedClusterFile folder, VariableSpace space ) throws FileException {
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( folder.getPath() );
      fileObject.createFolder();
      String parent = folder.getPath().substring( 0, folder.getPath().length() - 1 );
      return NamedClusterFile.create( parent, fileObject );
    } catch ( KettleFileException | FileSystemException ignored ) {
      // Ignored
    }
    return null;
  }

  @Override
  public NamedClusterFile getFile( Bowl bowl, NamedClusterFile file, VariableSpace space ) {
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( file.getPath() );
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

  @Override
  public boolean fileExists( Bowl bowl, NamedClusterFile dir, String path, VariableSpace space ) throws FileException {
    path = sanitizeName( bowl, dir, path );
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( path );
      return fileObject.exists();
    } catch ( KettleFileException | FileSystemException e ) {
      throw new FileException();
    }
  }

  @Override
  public String getNewName( Bowl bowl, NamedClusterFile destDir, String newPath, VariableSpace space )
    throws FileException {
    String extension = Utils.getExtension( newPath );
    String parent = Utils.getParent( newPath, "/" );
    String name = Utils.getName( newPath, "/" ).replace( "." + extension, "" );
    int i = 1;
    String testName = sanitizeName( bowl, destDir, newPath );
    try {
      while ( KettleVFS.getInstance( bowl ).getFileObject( testName ).exists() ) {
        if ( Utils.isValidExtension( extension ) ) {
          testName = sanitizeName( bowl, destDir, parent + name + " " + i + "." + extension );
        } else {
          testName = sanitizeName( bowl, destDir, newPath + " " + i );
        }
        i++;
      }
    } catch ( KettleFileException | FileSystemException e ) {
      return testName;
    }
    return testName;
  }

  @Override
  public boolean isSame( Bowl bowl, File file1, File file2 ) {
    return file1 instanceof NamedClusterFile && file2 instanceof NamedClusterFile;
  }

  @Override
  public NamedClusterFile rename( Bowl bowl, NamedClusterFile file, String newPath, OverwriteStatus overwriteStatus,
    VariableSpace space ) throws FileException {
    return doMove( bowl, file, newPath, overwriteStatus );
  }

  @Override
  public NamedClusterFile copy( Bowl bowl, NamedClusterFile file, String toPath, OverwriteStatus overwriteStatus,
    VariableSpace space ) throws FileException {
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( file.getPath() );
      FileObject copyObject = KettleVFS.getInstance( bowl ).getFileObject( toPath );
      copyObject.copyFrom( fileObject, Selectors.SELECT_ALL );
      if ( file instanceof NamedClusterDirectory ) {
        return NamedClusterDirectory.create( copyObject.getParent().getPublicURIString(), fileObject );
      } else {
        return NamedClusterFile.create( copyObject.getParent().getPublicURIString(), fileObject );
      }
    } catch ( KettleFileException | FileSystemException e ) {
      throw new FileException();
    }
  }

  @Override
  public NamedClusterFile move( Bowl bowl, NamedClusterFile namedClusterFile, String s, OverwriteStatus overwriteStatus,
    VariableSpace space ) throws FileException {
    return null;
  }

  private NamedClusterFile doMove( Bowl bowl, NamedClusterFile file, String newPath, OverwriteStatus overwriteStatus ) {
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( file.getPath() );
      FileObject renameObject = KettleVFS.getInstance( bowl ).getFileObject( newPath );
      if ( renameObject.exists() ) {
        overwriteStatus.promptOverwriteIfNecessary( file.getPath(), file.getType() );
        if ( overwriteStatus.isOverwrite() ) {
          renameObject.delete();
        } else if ( overwriteStatus.isCancel() || overwriteStatus.isSkip() ) {
          return null;
        } else if ( overwriteStatus.isRename() ) {
          NamedClusterDirectory namedClusterDir =
            NamedClusterDirectory.create( renameObject.getParent().getPath().toString(), renameObject );
          newPath = getNewName( bowl, namedClusterDir, newPath, new Variables() );
          renameObject = KettleVFS.getInstance( bowl ).getFileObject( newPath );
        }
      }

      fileObject.moveTo( renameObject );
      if ( file instanceof NamedClusterDirectory ) {
        return NamedClusterDirectory.create( renameObject.getParent().getPublicURIString(), renameObject );
      } else {
        return NamedClusterFile.create( renameObject.getParent().getPublicURIString(), renameObject );
      }
    } catch ( KettleFileException | FileSystemException | FileException e ) {
      return null;
    }
  }

  @Override
  public InputStream readFile( Bowl bowl, NamedClusterFile file, VariableSpace space ) throws FileException {
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( file.getPath() );
      return fileObject.getContent().getInputStream();
    } catch ( KettleFileException | FileSystemException e ) {
      return null;
    }
  }

  @Override
  public NamedClusterFile writeFile( Bowl bowl, InputStream inputStream, NamedClusterFile destDir, String path,
                                     OverwriteStatus overwriteStatus, VariableSpace space ) throws FileException {
    FileObject fileObject = null;
    try {
      fileObject = KettleVFS.getInstance( bowl ).getFileObject( path );
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

  @Override
  public NamedClusterFile getParent( Bowl bowl, NamedClusterFile file ) {
    NamedClusterFile vfsFile = new NamedClusterFile();
    vfsFile.setPath( file.getParent() );
    return vfsFile;
  }

  @Override
  public void clearProviderCache() {
    // Nothing to clear
  }

  @Override
  public File getFile( Bowl bowl, String path, boolean isDirectory ) {
    FileObject fileObject = null;
    try {
      fileObject = KettleVFS.getInstance( bowl ).getFileObject( path );
      if ( isDirectory ) {
        if ( fileObject.exists() && !fileObject.getType().equals( FileType.FOLDER ) ) {
          throwIllegalArgumentException( path, "is not a directory" );
        }
        return NamedClusterDirectory.create( null, fileObject );
      } else {
        if ( fileObject.exists() && !fileObject.getType().equals( FileType.FILE ) ) {
          throwIllegalArgumentException( path, "is a directory" );
        }
        return NamedClusterFile.create( null, fileObject );
      }
    } catch ( KettleFileException | FileSystemException e ) {
      throwIllegalArgumentException( path, "could not create a VFSFile object" );
    }
    return null; //Will never be executed but compiler complained
  }

  private void throwIllegalArgumentException( String path, String message ) {
    throw new IllegalArgumentException( "\"" + path + "\" " + message );
  }

  @Override
  public NamedClusterFile createDirectory( Bowl bowl, String parentPath, NamedClusterFile file,
    String newDirectoryName ) {
    NamedClusterDirectory namedClusterDir = null;
    try {
      FileObject fileObject = KettleVFS.getInstance( bowl ).getFileObject( parentPath + "/" + newDirectoryName );
      namedClusterDir = NamedClusterDirectory.create( null, fileObject );
      add( bowl, namedClusterDir,null );
    } catch ( KettleFileException | FileException e ) {
      e.printStackTrace();
      return null;
    }
    return namedClusterDir;
  }
}
