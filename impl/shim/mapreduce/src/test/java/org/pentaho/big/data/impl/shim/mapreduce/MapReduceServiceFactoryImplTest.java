/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.mapreduce;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;
import org.pentaho.bigdata.api.mapreduce.TransformationVisitorService;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 12/8/15.
 */
public class MapReduceServiceFactoryImplTest {
  private boolean isActiveConfiguration;
  private HadoopConfiguration hadoopConfiguration;
  private ExecutorService executorService;
  private MapReduceServiceFactoryImpl mapReduceServiceFactory;
  private NamedCluster namedCluster;
  private List<TransformationVisitorService> visitorServices = new ArrayList<>();

  @Before
  public void setup() {
    isActiveConfiguration = true;
    hadoopConfiguration = mock( HadoopConfiguration.class );
    executorService = mock( ExecutorService.class );
    mapReduceServiceFactory =
      new MapReduceServiceFactoryImpl( isActiveConfiguration, hadoopConfiguration, executorService, visitorServices );
    namedCluster = mock( NamedCluster.class );
  }

  @Test
  public void testGetServiceClass() {
    assertEquals( MapReduceService.class, mapReduceServiceFactory.getServiceClass() );
  }

  @Test
  public void testCanHandleActive() {
    assertTrue( mapReduceServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCanHandleInactive() {
    isActiveConfiguration = false;
    mapReduceServiceFactory =
      new MapReduceServiceFactoryImpl( isActiveConfiguration, hadoopConfiguration, executorService, visitorServices );
    assertFalse( mapReduceServiceFactory.canHandle( namedCluster ) );
  }

  @Test
  public void testCreate() {
    assertTrue( mapReduceServiceFactory.create( namedCluster ) instanceof MapReduceServiceImpl );
  }
}
