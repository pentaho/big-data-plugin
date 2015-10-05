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

package org.pentaho.big.data.impl.shim.initializer;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/2/15.
 */
public class ClusterInitializerProviderImplTest {
  private HadoopConfigurationBootstrap hadoopConfigurationBootstrap;
  private ClusterInitializerProviderImpl clusterInitializerProvider;
  private NamedCluster namedCluster;

  @Before
  public void setup() {
    hadoopConfigurationBootstrap = mock( HadoopConfigurationBootstrap.class );
    clusterInitializerProvider = new ClusterInitializerProviderImpl( hadoopConfigurationBootstrap );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testNoArgConstructor() {
    assertNotNull( new ClusterInitializerProviderImpl() );
  }

  @Test
  public void testCanHandle() {
    assertTrue( clusterInitializerProvider.canHandle( null ) );
    assertTrue( clusterInitializerProvider.canHandle( namedCluster ) );
  }

  @Test
  public void testInitializeSuccess() throws ClusterInitializationException, ConfigurationException {
    clusterInitializerProvider.initialize( namedCluster );
    verify( hadoopConfigurationBootstrap ).getProvider();
  }

  @Test( expected = ClusterInitializationException.class )
  public void testInitializationFalure() throws ClusterInitializationException, ConfigurationException {
    when( hadoopConfigurationBootstrap.getProvider() ).thenThrow( new ConfigurationException( null ) );
    clusterInitializerProvider.initialize( namedCluster );
  }
}
