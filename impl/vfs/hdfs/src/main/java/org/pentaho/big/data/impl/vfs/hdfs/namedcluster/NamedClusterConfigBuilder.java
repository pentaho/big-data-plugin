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

package org.pentaho.big.data.impl.vfs.hdfs.namedcluster;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.pentaho.big.data.api.cluster.MetaStoreService;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.di.core.vfs.configuration.KettleGenericFileSystemConfigBuilder;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

public class NamedClusterConfigBuilder extends KettleGenericFileSystemConfigBuilder {

  private static final NamedClusterConfigBuilder BUILDER = new NamedClusterConfigBuilder();
  private final MetaStoreService metaStoreService;
  private final NamedClusterService namedClusterService;

  public NamedClusterConfigBuilder() {
    this( null, null );
  }

  public NamedClusterConfigBuilder(
    MetaStoreService metaStoreService, NamedClusterService namedClusterService ) {
    this.metaStoreService = metaStoreService;
    this.namedClusterService = namedClusterService;
  }

  /**
   * @return NamedClusterConfigBuilder instance
   */
  public static NamedClusterConfigBuilder getInstance() {
    return BUILDER;
  }

  public static FileSystemConfigBuilder getInstance( MetaStoreService metaStoreService,
                                                     NamedClusterService namedClusterService ) {
    return new NamedClusterConfigBuilder( metaStoreService, namedClusterService );
  }

  /**
   * @return HDFSFileSystem
   */
  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return HDFSFileSystem.class;
  }

  public void snapshotNamedClusterToMetaStore( IMetaStore snapshotMetaStore ) throws MetaStoreException {
    IMetaStore metaStore = metaStoreService.getMetaStore();
    List<NamedCluster> ncList = namedClusterService.list( metaStore );
    if ( ncList != null ) {
      for ( NamedCluster nc : ncList ) {
        namedClusterService.create( nc, snapshotMetaStore );
      }
    }
  }
}
