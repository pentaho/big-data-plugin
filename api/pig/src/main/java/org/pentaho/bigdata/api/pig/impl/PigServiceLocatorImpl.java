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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.bigdata.api.pig.PigService;
import org.pentaho.bigdata.api.pig.PigServiceFactory;
import org.pentaho.bigdata.api.pig.PigServiceLocator;

import java.util.List;

/**
 * Created by bryan on 7/6/15.
 */
public class PigServiceLocatorImpl implements PigServiceLocator {
  private final List<PigServiceFactory> pigServiceFactories;
  private final ClusterInitializer clusterInitializer;

  public PigServiceLocatorImpl( List<PigServiceFactory> pigServiceFactories, ClusterInitializer clusterInitializer ) {
    this.pigServiceFactories = pigServiceFactories;
    this.clusterInitializer = clusterInitializer;
  }

  @Override public PigService getPigService( NamedCluster namedCluster ) throws ClusterInitializationException {
    clusterInitializer.initialize( namedCluster );
    for ( PigServiceFactory pigServiceFactory : pigServiceFactories ) {
      if ( pigServiceFactory.canHandle( namedCluster ) ) {
        return pigServiceFactory.create( namedCluster );
      }
    }
    return null;
  }
}
