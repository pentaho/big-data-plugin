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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;

import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

public class NamedClusterConfigBuilderTest {

  private NamedClusterService namedClusterService = mock( NamedClusterService.class );

  private MetastoreLocatorOsgi metastoreLocator = mock( MetastoreLocatorOsgi.class );

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
