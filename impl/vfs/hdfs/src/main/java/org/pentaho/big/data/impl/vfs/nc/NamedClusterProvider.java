/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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
package org.pentaho.big.data.impl.vfs.nc;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

public class NamedClusterProvider extends AbstractOriginatingFileProvider {

  protected static final Collection<Capability> capabilities =
      Collections.unmodifiableCollection(
          Arrays.asList(
              new Capability[] {
                Capability.CREATE,
                Capability.DELETE,
                Capability.RENAME,
                Capability.GET_TYPE,
                Capability.LIST_CHILDREN,
                Capability.READ_CONTENT,
                Capability.URI,
                Capability.WRITE_CONTENT,
                Capability.GET_LAST_MODIFIED,
                Capability.SET_LAST_MODIFIED_FILE,
                Capability.RANDOM_ACCESS_READ } ) );

  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final NamedClusterService namedClusterService;
  private final MetastoreLocator metastoreLocator;

  public NamedClusterProvider(
      MetastoreLocator locator,
      HadoopFileSystemLocator hadoopFileSystemLocator,
      NamedClusterService namedClusterService,
      FileNameParser fileNameParser,
      String schema ) throws FileSystemException {
     this(
         locator,
         hadoopFileSystemLocator,
         namedClusterService,
         (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
         fileNameParser,
         new String[] { schema } );
  }

  public NamedClusterProvider(
      MetastoreLocator locator,
      HadoopFileSystemLocator hadoopFileSystemLocator,
      NamedClusterService namedClusterService,
      DefaultFileSystemManager fileSystemManager,
      FileNameParser fileNameParser,
      String[] schemes )  throws FileSystemException {
    super();
    this.metastoreLocator = locator;
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.namedClusterService = namedClusterService;
    setFileNameParser( fileNameParser );
    fileSystemManager.addProvider( schemes, this );
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
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
      String generatedUrl = namedCluster.processURLsubstitution( path == null ? "" : path, metastoreLocator.getMetastore(), new Variables() );
      URI uri = URI.create( generatedUrl );

      return new HDFSFileSystem( name, fileSystemOptions, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster, uri ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }

  private NamedCluster getNamedClusterByName( String clusterNameToResolve ) throws FileSystemException {
    IMetaStore metaStore = metastoreLocator.getMetastore();
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
