package org.pentaho.big.data.impl.vfs.hdfs.namedcluster;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.MetaStoreService;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileProvider;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.net.URI;

/**
 * Created by dstepanov on 11/05/17.
 */
public class NamedClusterProvider extends HDFSFileProvider {

  private MetaStoreService metaStoreService;

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               FileNameParser fileNameParser, String[] schemes, MetaStoreService metaStore )
    throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(), fileNameParser, schemes, metaStore );
  }

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               FileNameParser fileNameParser, String schema, MetaStoreService metaStore )
    throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(), fileNameParser,
      new String[] { schema }, metaStore );
  }


  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               DefaultFileSystemManager fileSystemManager,
                               FileNameParser fileNameParser, String[] schemes, MetaStoreService metaStore )
    throws FileSystemException {
    super( hadoopFileSystemLocator, namedClusterService, fileSystemManager, fileNameParser, schemes );
    this.metaStoreService = metaStore;
  }


  @SuppressWarnings( "finally" ) @Override
  protected FileSystem doCreateFileSystem( FileName name, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    String clusterName = genericFileName.getHostName();
    String path = genericFileName.getPath();
    NamedCluster namedCluster = getNamedClusterByName( clusterName );
    try {
      if ( namedCluster == null ) {
        namedCluster = namedClusterService.getClusterTemplate();
      }
      String generatedUrl = namedCluster
        .processURLsubstitution( path == null ? "" : path, metaStoreService.getMetaStore(), new Variables() );
      URI uri = URI.create( generatedUrl );

      return new HDFSFileSystem( name, fileSystemOptions,
        hadoopFileSystemLocator.getHadoopFilesystem( namedCluster, uri ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }


  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return NamedClusterConfigBuilder.getInstance( metaStoreService, namedClusterService );
  }


  private NamedCluster getNamedClusterByName( String clusterNameToResolve ) throws FileSystemException {
    IMetaStore metaStore = metaStoreService.getMetaStore();
    NamedCluster namedCluster = null;
    try {
      if ( metaStore != null ) {
        namedCluster = namedClusterService.read( clusterNameToResolve, metaStore );
      }
    } catch ( MetaStoreException e ) {
      throw new FileSystemException( e );
    }
    return namedCluster;
  }



}
