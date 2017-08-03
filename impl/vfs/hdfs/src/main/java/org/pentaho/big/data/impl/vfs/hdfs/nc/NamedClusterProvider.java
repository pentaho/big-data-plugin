/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.impl.vfs.hdfs.nc;

import java.net.URI;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
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
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

/**
 * Created by dstepanov on 11/05/17.
 */
public class NamedClusterProvider extends HDFSFileProvider {

  private MetastoreLocator metaStoreService;

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
                               String schema,
                               MetastoreLocator metaStore ) throws FileSystemException {
    this(
        hadoopFileSystemLocator,
        namedClusterService,
        (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
        fileNameParser,
        new String[] { schema },
        metaStore );
  }

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               DefaultFileSystemManager fileSystemManager,
                               FileNameParser fileNameParser,
                               String[] schemes,
                               MetastoreLocator metaStore ) throws FileSystemException {
    super( hadoopFileSystemLocator, namedClusterService, fileSystemManager, fileNameParser, schemes );
    this.metaStoreService = metaStore;
  }

  @Override
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
      String generatedUrl = namedCluster.processURLsubstitution( path == null ? "" : path, metaStoreService.getMetastore(), new Variables() );
      URI uri = URI.create( generatedUrl );
      return new HDFSFileSystem( name, fileSystemOptions, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster, uri ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }

  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return NamedClusterConfigBuilder.getInstance( metaStoreService, namedClusterService );
  }

  /**
   * package visibility for test purpose only
   * @param clusterNameToResolve - name of namedcluster for resolve namedcluster
   * @return named cluster from metastore or null
   * @throws FileSystemException
   */
  NamedCluster getNamedClusterByName( String clusterNameToResolve ) throws FileSystemException {
    IMetaStore metaStore = metaStoreService.getMetastore();
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
