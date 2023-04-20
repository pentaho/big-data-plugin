/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2023 by Hitachi Vantara : http://www.pentaho.com
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
