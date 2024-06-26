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

package org.pentaho.big.data.impl.vfs.hdfs.nc;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileProvider;
import org.pentaho.di.core.osgi.api.VfsEmbeddedFileSystemCloser;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by dstepanov on 11/05/17.
 */
public class NamedClusterProvider extends HDFSFileProvider implements VfsEmbeddedFileSystemCloser {

  private Map<String, Set<FileSystem>> cacheEntries =
    Collections.synchronizedMap( new HashMap<>() );

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               FileNameParser fileNameParser,
                               String[] schemes,
                               MetastoreLocator metaStore ) throws FileSystemException {
    this(
        hadoopFileSystemLocator,
        namedClusterService,
        (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
        fileNameParser,
        schemes,
        metaStore );
  }

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               FileNameParser fileNameParser,
                               String schema ) throws FileSystemException {
    this(
        hadoopFileSystemLocator,
        namedClusterService,
        (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
        fileNameParser,
        new String[] { schema },
        null );
  }


  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               DefaultFileSystemManager fileSystemManager,
                               FileNameParser fileNameParser,
                               String[] schemes,
                               MetastoreLocator metaStore ) throws FileSystemException {
    super( hadoopFileSystemLocator, namedClusterService, fileSystemManager, fileNameParser, schemes, metaStore );
  }


  @Override
  protected FileSystem doCreateFileSystem( FileName name, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    String clusterName = genericFileName.getHostName();
    String path = genericFileName.getPath();
    NamedCluster namedCluster = getNamedClusterByName( clusterName, fileSystemOptions );
    try {
      if ( namedCluster == null ) {
        namedCluster = namedClusterService.getClusterTemplate();
      }
      String generatedUrl = namedCluster
        .processURLsubstitution( path == null ? "" : path,
          getMetastore( clusterName, fileSystemOptions ), new Variables() );
      URI uri = URI.create( generatedUrl );

      return new NamedClusterFileSystem( name, uri, fileSystemOptions,
        hadoopFileSystemLocator.getHadoopFilesystem( namedCluster, uri ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return NamedClusterConfigBuilder.getInstance( getMetastoreLocator(), namedClusterService );
  }

  /**
   * package visibility for test purpose only
   * @param clusterNameToResolve - name of namedcluster for resolve namedcluster
   * @param filesSystemOptions - The fileSystemOptions for the file system in play
   * @return named cluster from metastore or null
   * @throws FileSystemException
   */
  NamedCluster getNamedClusterByName( String clusterNameToResolve, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    IMetaStore metaStore = getMetastore( clusterNameToResolve, fileSystemOptions );
    NamedCluster namedCluster = null;
    try {
      namedCluster = namedClusterService.read( clusterNameToResolve, metaStore );
    } catch ( MetaStoreException e ) {
      throw new FileSystemException( e );
    }
    return namedCluster;
  }

  protected synchronized FileSystem getFileSystem( final FileName rootName, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    FileSystem fs = findFileSystem( rootName, fileSystemOptions );
    if ( fs == null ) {
      //  Need to create the file system, and cache it
      fs = doCreateFileSystem( rootName, fileSystemOptions );
      addCacheEntry( rootName, fs );
    }
    return fs;
  }

  private String getFileSystemKey( String rootName, FileSystemOptions fileSystemOptions ) {
    return getEmbeddedMetastoreKey( fileSystemOptions ) == null ? rootName
      : rootName + getEmbeddedMetastoreKey( fileSystemOptions );
  }

  private String getEmbeddedMetastoreKey( FileSystemOptions fileSystemOptions ) {
    return ( (NamedClusterConfigBuilder) getConfigBuilder() ).getEmbeddedMetastoreKey( fileSystemOptions );
  }

  private IMetaStore getMetastore( String clusterNameToResolve, FileSystemOptions fileSystemOptions ) {
    String embeddedMetastoreKey = getEmbeddedMetastoreKey( fileSystemOptions );
    IMetaStore metaStore = ( embeddedMetastoreKey != null ) ? getMetastoreLocator().getMetastore( embeddedMetastoreKey )
      : getMetastoreLocator().getMetastore();
    if ( metaStore != null ) {
      try {
        if ( namedClusterService.read( clusterNameToResolve, metaStore ) != null ) {
          return metaStore; // The namedCluster agnostic metaStore has this namedCluster, return it.
        }
      } catch ( MetaStoreException e ) {
        // fall through and return the embedded metastore
      }
      if ( getMetastoreLocator().getExplicitMetastore( embeddedMetastoreKey ) != null ) {
        metaStore = getMetastoreLocator().getExplicitMetastore( embeddedMetastoreKey );
      }
    }
    return metaStore;
  }

  private void addCacheEntry( FileName rootName, FileSystem fs ) throws FileSystemException {
    addFileSystem( getFileSystemKey( rootName.toString(), fs.getFileSystemOptions() ), fs );
    String embeddedMetastoreKey = getEmbeddedMetastoreKey( fs.getFileSystemOptions() );
    Set<FileSystem> fsSet = cacheEntries.get( embeddedMetastoreKey );
    if ( fsSet == null ) {
      fsSet = Collections.synchronizedSet( new HashSet<FileSystem>() );
      cacheEntries.put( embeddedMetastoreKey, fsSet );
    }
    fsSet.add( fs );
  }

  public void closeFileSystem( String embeddedMetastoreKey ) {
    IMetaStore defaultMetastore = getMetastoreLocator().getMetastore();
    IMetaStore embeddedMetastore = getMetastoreLocator().getExplicitMetastore( embeddedMetastoreKey );
    if ( cacheEntries.get( embeddedMetastoreKey ) != null ) {
      for ( FileSystem fs : cacheEntries.get( embeddedMetastoreKey ) ) {
        closeFileSystem( fs );
      }
    }
    cacheEntries.remove( embeddedMetastoreKey );
    namedClusterService.close( defaultMetastore );
    if ( defaultMetastore != embeddedMetastore ) {
      namedClusterService.close( embeddedMetastore );
    }
  }

  protected synchronized FileSystem findFileSystem( final Comparable<?> key, final FileSystemOptions fileSystemProps ) {
    String editedKey = getFileSystemKey( key.toString(), fileSystemProps );
    return super.findFileSystem( editedKey, fileSystemProps );
  }

}
