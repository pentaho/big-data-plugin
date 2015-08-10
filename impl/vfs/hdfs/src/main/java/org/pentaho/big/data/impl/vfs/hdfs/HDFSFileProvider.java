/*******************************************************************************
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
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.impl.vfs.hdfs;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.vfs.KettleVFS;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class HDFSFileProvider extends AbstractOriginatingFileProvider {
  /**
   * The scheme this provider was designed to support
   */
  public static final String SCHEME = "hdfs";
  public static final String MAPRFS = "maprfs";
  /**
   * User Information.
   */
  public static final String ATTR_USER_INFO = "UI";
  /**
   * Authentication types.
   */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
    new UserAuthenticationData.Type[] { UserAuthenticationData.USERNAME,
      UserAuthenticationData.PASSWORD };
  /**
   * The provider's capabilities.
   */
  protected static final Collection<Capability> capabilities =
    Collections.unmodifiableCollection( Arrays.asList( new Capability[] { Capability.CREATE, Capability.DELETE,
      Capability.RENAME, Capability.GET_TYPE, Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI,
      Capability.WRITE_CONTENT, Capability.APPEND_CONTENT,
      Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE, Capability.RANDOM_ACCESS_READ } ) );
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final NamedClusterService namedClusterService;

  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                           NamedClusterService namedClusterService ) throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager() );
  }

  public HDFSFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator, NamedClusterService namedClusterService,
                           DefaultFileSystemManager fileSystemManager ) throws FileSystemException {
    super();
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.namedClusterService = namedClusterService;
    setFileNameParser( HDFSFileNameParser.getInstance() );
    fileSystemManager.addProvider( new String[] { SCHEME, MAPRFS }, this );
  }

  @Override protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    String hostName = genericFileName.getHostName();
    int port = genericFileName.getPort();
    // TODO: load from metastore
    NamedCluster namedCluster = namedClusterService.getClusterTemplate();
    namedCluster.setHdfsHost( hostName );
    if ( port > 0 ) {
      namedCluster.setHdfsPort( String.valueOf( port ) );
    } else {
      namedCluster.setHdfsPort( "" );
    }
    namedCluster.setMapr( MAPRFS.equals( name.getScheme() ) );
    try {
      return new HDFSFileSystem( name, fileSystemOptions, hadoopFileSystemLocator.getHadoopFilesystem( namedCluster ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }

  @Override public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
