/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.vfs.configuration.KettleGenericFileSystemConfigBuilder;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.List;

public class NamedClusterConfigBuilder extends KettleGenericFileSystemConfigBuilder {

  private static final NamedClusterConfigBuilder BUILDER = new NamedClusterConfigBuilder();
  private static final String EMBEDDED_METASTORE_KEY_PROPERTY = "embeddedMetaStoreKey";
  private final MetastoreLocatorOsgi metastoreLocator;
  private final NamedClusterService namedClusterService;

  public NamedClusterConfigBuilder() {
    this( null, null );
  }

  public NamedClusterConfigBuilder( MetastoreLocatorOsgi metastoreLocator, NamedClusterService namedClusterService ) {
    this.metastoreLocator = metastoreLocator;
    this.namedClusterService = namedClusterService;
  }

  /**
   * @return NamedClusterConfigBuilder instance
   */
  public static NamedClusterConfigBuilder getInstance() {
    return BUILDER;
  }

  public static FileSystemConfigBuilder getInstance( MetastoreLocatorOsgi metastoreLocator,  NamedClusterService namedClusterService ) {
    return new NamedClusterConfigBuilder( metastoreLocator, namedClusterService );
  }

  /**
   * @return HDFSFileSystem
   */
  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return HDFSFileSystem.class;
  }

  public void snapshotNamedClusterToMetaStore( IMetaStore snapshotMetaStore ) throws MetaStoreException {
    IMetaStore metaStore = metastoreLocator.getMetastore();
    List<NamedCluster> ncList = namedClusterService.list( metaStore );
    if ( ncList != null ) {
      for ( NamedCluster nc : ncList ) {
        namedClusterService.create( nc, snapshotMetaStore );
      }
    }
  }

  public void setEmbeddedMetastoreKey( final FileSystemOptions opts, final String embeddedMetaStoreKey ) {
    setParam( opts, EMBEDDED_METASTORE_KEY_PROPERTY, embeddedMetaStoreKey );
  }

  public String getEmbeddedMetastoreKey( final FileSystemOptions opts ) {
    return (String) getParam( opts, EMBEDDED_METASTORE_KEY_PROPERTY );
  }
}
