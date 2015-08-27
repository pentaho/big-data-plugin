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

package org.pentaho.big.data.api.initializer.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializerProvider;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/27/15.
 */
public class ClusterInitializerImplTest {
  private ClusterInitializerProvider clusterInitializerProvider1;
  private ClusterInitializerProvider clusterInitializerProvider2;
  private ClusterInitializerProvider clusterInitializerProvider3;
  private ClusterInitializerImpl clusterInitializer;
  private NamedCluster namedCluster;

  @Before
  public void setup() {
    clusterInitializerProvider1 = mock( ClusterInitializerProvider.class );
    clusterInitializerProvider2 = mock( ClusterInitializerProvider.class );
    clusterInitializerProvider3 = mock( ClusterInitializerProvider.class );
    clusterInitializer = new ClusterInitializerImpl( Arrays.asList( clusterInitializerProvider1,
      clusterInitializerProvider2, clusterInitializerProvider3 ) );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testInitialize() throws ClusterInitializationException {
    when( clusterInitializerProvider1.canHandle( namedCluster ) ).thenReturn( false );
    when( clusterInitializerProvider2.canHandle( namedCluster ) ).thenReturn( true );
    clusterInitializer.initialize( namedCluster );
    verify( clusterInitializerProvider1 ).canHandle( namedCluster );
    verify( clusterInitializerProvider2 ).canHandle( namedCluster );
    verify( clusterInitializerProvider2 ).initialize( namedCluster );
    verifyNoMoreInteractions( clusterInitializerProvider1, clusterInitializerProvider2, clusterInitializerProvider3 );
  }

  @Test
  public void testInitializeNoneCanHandle() throws ClusterInitializationException {
    clusterInitializer.initialize( namedCluster );
    verify( clusterInitializerProvider1 ).canHandle( namedCluster );
    verify( clusterInitializerProvider2 ).canHandle( namedCluster );
    verify( clusterInitializerProvider3 ).canHandle( namedCluster );
    verifyNoMoreInteractions( clusterInitializerProvider1, clusterInitializerProvider2, clusterInitializerProvider3 );
  }
}
