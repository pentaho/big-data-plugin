/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.impl.vfs.hdfs.nc;

import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.di.core.vfs.configuration.KettleGenericFileSystemConfigBuilder;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.util.List;

public class NamedClusterConfigBuilder extends KettleGenericFileSystemConfigBuilder {

  private static final NamedClusterConfigBuilder BUILDER = new NamedClusterConfigBuilder();
  private static final String EMBEDDED_METASTORE_KEY_PROPERTY = "embeddedMetaStoreKey";
  private final MetastoreLocator metastoreLocator;
  private final NamedClusterService namedClusterService;

  public NamedClusterConfigBuilder() {
    this( null, null );
  }

  public NamedClusterConfigBuilder( MetastoreLocator metastoreLocator, NamedClusterService namedClusterService ) {
    this.metastoreLocator = metastoreLocator;
    this.namedClusterService = namedClusterService;
  }

  /**
   * @return NamedClusterConfigBuilder instance
   */
  public static NamedClusterConfigBuilder getInstance() {
    return BUILDER;
  }

  public static FileSystemConfigBuilder getInstance( MetastoreLocator metastoreLocator, NamedClusterService namedClusterService ) {
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
