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

package org.pentaho.big.data.kettle.plugins.formats.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith( MockitoJUnitRunner.class )
public class NamedClusterResolverTest {
  @Mock private MetastoreLocator metaStoreService;
  @Mock private NamedClusterService namedClusterService;
  @Mock private KettleLoggingEventListener kettleLoggingEventListener;
  @Mock private NamedCluster namedCluster;
  @Mock private NamedClusterServiceLocator namedClusterServiceLocator;

  private NamedClusterResolver namedClusterResolver;

  @Before
  public void before() {
    KettleLogStore.init();
    KettleLogStore.getAppender().addLoggingEventListener( kettleLoggingEventListener );
    when( namedClusterService.getNamedClusterByName( "testhc", null ) )
      .thenReturn( namedCluster );
    when( namedClusterService.getNamedClusterByHost( "somehost", null ) )
      .thenReturn( namedCluster );
    namedClusterResolver = new NamedClusterResolver( namedClusterServiceLocator, namedClusterService );
  }

  @Test
  public void windowsFilePathsAreHandled() {
    assertNull(
      namedClusterResolver.resolveNamedCluster( "C:/path/to some/file" ) );
    verify( kettleLoggingEventListener, times( 0 ) ).eventAdded( ArgumentMatchers.any() );
  }

  @Test
  public void testNamedClusterByName() {
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( metaStoreService );
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
        .thenReturn( metastoreLocatorCollection );
      NamedCluster cluster = namedClusterResolver.resolveNamedCluster( "hc://testhc/path" );
      assertEquals( namedCluster, cluster );

      cluster = namedClusterResolver.resolveNamedCluster( "hc://nosuchhc/path" );
      assertNull( cluster );
    }
  }

  @Test
  public void testNamedClusterByHost() {
    Collection<MetastoreLocator> metastoreLocatorCollection = new ArrayList<>();
    metastoreLocatorCollection.add( metaStoreService );
    try ( MockedStatic<PluginServiceLoader> pluginServiceLoaderMockedStatic = Mockito.mockStatic( PluginServiceLoader.class ) ) {
      pluginServiceLoaderMockedStatic.when( () -> PluginServiceLoader.loadServices( MetastoreLocator.class ) )
        .thenReturn( metastoreLocatorCollection );
      NamedCluster cluster = namedClusterResolver.resolveNamedCluster( "hdfs://somehost/path" );
      assertEquals( namedCluster, cluster );
    }
  }

}
