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

package org.pentaho.bigdata.api.pig.impl;

import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.PigServiceFactory;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 7/14/15.
 */
public class PigServiceLocatorImplTest {
  @Test
  public void testGetPigService() throws ClusterInitializationException {
    NamedCluster namedCluster = mock( NamedCluster.class );
    NamedCluster namedCluster2 = mock( NamedCluster.class );

    PigServiceFactory pigServiceFactory = mock( PigServiceFactory.class );
    when( pigServiceFactory.canHandle( namedCluster ) ).thenReturn( true );
    when( pigServiceFactory.canHandle( namedCluster2 ) ).thenReturn( false );

    PigService pigService = mock( PigService.class );
    when( pigServiceFactory.create( namedCluster ) ).thenReturn( pigService );

    ClusterInitializer clusterInitializer = mock( ClusterInitializer.class );
    PigServiceLocatorImpl pigServiceLocator =
      new PigServiceLocatorImpl( new ArrayList<>( Arrays.asList( pigServiceFactory ) ),
        clusterInitializer );
    assertEquals( pigService, pigServiceLocator.getPigService( namedCluster ) );
    assertNull( pigServiceLocator.getPigService( namedCluster2 ) );
    verify( pigServiceFactory, never() ).create( namedCluster2 );
  }
}
