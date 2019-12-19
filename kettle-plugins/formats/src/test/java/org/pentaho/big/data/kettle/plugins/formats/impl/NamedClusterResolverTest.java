/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@RunWith( MockitoJUnitRunner.class )
public class NamedClusterResolverTest {
  @Mock private MetastoreLocatorOsgi metaStoreService;
  @Mock private NamedClusterService namedClusterService;
  @Mock private KettleLoggingEventListener kettleLoggingEventListener;
  @Mock private NamedCluster namedCluster;

  @Before
  public void before() {
    KettleLogStore.init();
    KettleLogStore.getAppender().addLoggingEventListener( kettleLoggingEventListener );
    when( namedClusterService.getNamedClusterByName( "testhc", null ) )
      .thenReturn( namedCluster );
    when( namedClusterService.getNamedClusterByHost( "somehost", null ) )
      .thenReturn( namedCluster );
  }

  @Test
  public void windowsFilePathsAreHandled() {
    assertNull(
      NamedClusterResolver.resolveNamedCluster(
        namedClusterService, metaStoreService, "C:/path/to some/file" ) );
    verify( kettleLoggingEventListener, times( 0 ) ).eventAdded( any() );
  }

  @Test
  public void testNamedClusterByName() {
    NamedCluster cluster = NamedClusterResolver.resolveNamedCluster(
      namedClusterService, metaStoreService, "hc://testhc/path" );
    assertEquals( namedCluster, cluster );

    cluster = NamedClusterResolver.resolveNamedCluster(
      namedClusterService, metaStoreService, "hc://nosuchhc/path" );
    assertNull( cluster );

  }

  @Test
  public void testNamedClusterByHost() {
    NamedCluster cluster = NamedClusterResolver.resolveNamedCluster(
      namedClusterService, metaStoreService, "hdfs://somehost/path" );
    assertEquals( namedCluster, cluster );
  }

}
