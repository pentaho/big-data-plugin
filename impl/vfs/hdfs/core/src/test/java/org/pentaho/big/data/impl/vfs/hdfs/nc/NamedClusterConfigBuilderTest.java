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

import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NamedClusterConfigBuilderTest {

  private NamedClusterService namedClusterService = mock( NamedClusterService.class );

  private MetastoreLocator metastoreLocator = mock( MetastoreLocator.class );

  private IMetaStore metastore = mock( IMetaStore.class );

  private NamedCluster namedCluster = mock( NamedCluster.class );

  @Before
  public void setUp() throws MetaStoreException {
    when( metastoreLocator.getMetastore() ).thenReturn( metastore );
  }

  @Test
  public void testSnapshotNamedClusterToMetaStore() throws MetaStoreException {
    when( namedClusterService.list( eq( metastore ) ) ).thenReturn( Arrays.asList( namedCluster ) );

    NamedClusterConfigBuilder builder = new NamedClusterConfigBuilder( metastoreLocator, namedClusterService );
    builder.snapshotNamedClusterToMetaStore( metastore );

    verify( namedClusterService ).create( eq( namedCluster ), eq( metastore ) );
  }

  @Test
  public void testSnapshotNamedClusterToMetaStore_staticInit() throws MetaStoreException {
    when( namedClusterService.list( eq( metastore ) ) ).thenReturn( Arrays.asList( namedCluster ) );

    FileSystemConfigBuilder builder = NamedClusterConfigBuilder.getInstance( metastoreLocator, namedClusterService );
    assertTrue( builder instanceof NamedClusterConfigBuilder );
    NamedClusterConfigBuilder ncbuilder = (NamedClusterConfigBuilder) builder;
    ncbuilder.snapshotNamedClusterToMetaStore( metastore );

    verify( namedClusterService ).create( eq( namedCluster ), eq( metastore ) );
  }

  @Test
  public void testSnapshotNamedClusterToMetaStore_MetastoreDoesNotHaveNC() throws MetaStoreException {
    when( namedClusterService.list( eq( metastore ) ) ).thenReturn( null );

    NamedClusterConfigBuilder builder = new NamedClusterConfigBuilder( metastoreLocator, namedClusterService );
    builder.snapshotNamedClusterToMetaStore( metastore );

    verify( namedClusterService, never() ).create( eq( namedCluster ), eq( metastore ) );
  }

  @Test
  public void testSnapshotNamedClusterToMetaStore_staticInit_MetastoreDoesNotHaveNC() throws MetaStoreException {
    when( namedClusterService.list( eq( metastore ) ) ).thenReturn( null );

    FileSystemConfigBuilder builder = NamedClusterConfigBuilder.getInstance( metastoreLocator, namedClusterService );
    assertTrue( builder instanceof NamedClusterConfigBuilder );
    NamedClusterConfigBuilder ncbuilder = (NamedClusterConfigBuilder) builder;
    ncbuilder.snapshotNamedClusterToMetaStore( metastore );

    verify( namedClusterService, never() ).create( eq( namedCluster ), eq( metastore ) );
  }

}
