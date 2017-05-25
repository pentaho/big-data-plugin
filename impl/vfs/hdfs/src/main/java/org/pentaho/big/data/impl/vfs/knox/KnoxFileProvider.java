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

package org.pentaho.big.data.impl.vfs.knox;

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.vfs.KettleVFS;

public class KnoxFileProvider extends AbstractOriginatingFileProvider {
  /**
   * The scheme this provider was designed to support
   */
  public static final String KNOX = "knox";
  public static final String KNOX_ROOT_MARKER = "knoxRoot";

  /**
   * User Information.
   */
  public static final String ATTR_USER_INFO = "UI";
  /**
   * Authentication types.
   */
  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
    UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD };
  /**
   * The provider's capabilities.
   */
  protected static final Collection<Capability> capabilities = Collections.unmodifiableCollection( Arrays
      .asList( new Capability[] { Capability.CREATE, Capability.DELETE, Capability.RENAME, Capability.GET_TYPE,
        Capability.LIST_CHILDREN, Capability.READ_CONTENT, Capability.URI, Capability.WRITE_CONTENT,
        Capability.GET_LAST_MODIFIED, Capability.SET_LAST_MODIFIED_FILE, Capability.RANDOM_ACCESS_READ } ) );
  private final HadoopFileSystemLocator hadoopFileSystemLocator;
  private final NamedClusterService namedClusterService;

  public KnoxFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator, NamedClusterService namedClusterService,
      FileNameParser fileNameParser, String schema ) throws FileSystemException {
    this( hadoopFileSystemLocator, namedClusterService, (DefaultFileSystemManager) KettleVFS.getInstance()
        .getFileSystemManager(), fileNameParser, new String[] { schema } );
  }

  public KnoxFileProvider( HadoopFileSystemLocator hadoopFileSystemLocator, NamedClusterService namedClusterService,
      DefaultFileSystemManager fileSystemManager, FileNameParser fileNameParser, String[] schemes )
    throws FileSystemException {
    super();
    this.hadoopFileSystemLocator = hadoopFileSystemLocator;
    this.namedClusterService = namedClusterService;
    setFileNameParser( fileNameParser );
    fileSystemManager.addProvider( schemes, this );
  }

  @Override
  protected FileSystem doCreateFileSystem( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    String hostName = genericFileName.getHostName();
    int port = genericFileName.getPort();
    // TODO: load from metastore
    NamedCluster namedCluster = namedClusterService.getClusterTemplate();
    namedCluster.setHdfsPort( "" );
    namedCluster.setUseGateway( true );
    try {
      URI uri = URI.create( name.getURI() == null ? "" : name.getURI() );
      HadoopFileSystem thdfs =
          hadoopFileSystemLocator.getHadoopFilesystem( namedCluster, uri );
      FileSystem knoxFileSystem = new KnoxFileSystem( name, fileSystemOptions, thdfs );
      return knoxFileSystem;
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  /**
   * Locates a file from its parsed URI.
   *
   * @param name
   *          The file name.
   * @param fileSystemOptions
   *          FileSystem options.
   * @return A FileObject associated with the file.
   * @throws FileSystemException
   *           if an error occurs.
   */
  @Override
  protected FileObject findFile( final FileName name, final FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    final FileName rootName;
    if ( KNOX.equals( genericFileName.getScheme() ) ) {
      String baseUrl = getContext().getFileSystemManager().resolveName( name, FileName.ROOT_PATH ).getURI();
      String normalizedPath = name.getPathDecoded();
      int markerPosition = normalizedPath.indexOf( KNOX_ROOT_MARKER );
      String gatewayPath;
      if ( markerPosition > -1 ) {
        gatewayPath = normalizedPath.substring( 0, markerPosition );
      } else {
        gatewayPath = ""; // TODO: Not sure what to return here - maybe check fileSystemOptions
      }
      rootName = getContext().getFileSystemManager().resolveName( name, gatewayPath );
    } else {
      // Check in the cache for the file system
      rootName = getContext().getFileSystemManager().resolveName( name, FileName.ROOT_PATH );
    }

    // final FileName rootName = new FileName(calcName);
    final FileSystem fs = getFileSystem( rootName, fileSystemOptions );

    // Locate the file
    // return fs.resolveFile(name.getPath());
    return fs.resolveFile( name );
  }
}
