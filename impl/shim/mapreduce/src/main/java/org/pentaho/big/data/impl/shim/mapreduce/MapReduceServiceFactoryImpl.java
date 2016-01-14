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

package org.pentaho.big.data.impl.shim.mapreduce;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;
import org.pentaho.hadoop.shim.HadoopConfiguration;

import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 7/6/15.
 */
public class MapReduceServiceFactoryImpl implements NamedClusterServiceFactory<MapReduceService> {
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;
  private final ExecutorService executorService;

  public MapReduceServiceFactoryImpl( boolean isActiveConfiguration,
                                      HadoopConfiguration hadoopConfiguration, ExecutorService executorService ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
    this.executorService = executorService;
  }

  @Override public Class<MapReduceService> getServiceClass() {
    return MapReduceService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    // String shimIdentifier = null; TODO: Specify shim
    return isActiveConfiguration;
  }

  @Override public MapReduceService create( NamedCluster namedCluster ) {
    return new MapReduceServiceImpl( namedCluster, hadoopConfiguration, executorService );
  }
}
