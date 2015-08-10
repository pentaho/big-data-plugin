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

package org.pentaho.bigdata.api.hdfs.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemFactory;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by bryan on 6/4/15.
 */
public class HadoopFileSystemLocatorImpl implements HadoopFileSystemLocator {
  private static final Logger LOGGER = LoggerFactory.getLogger( HadoopFileSystemLocatorImpl.class );
  private final List<HadoopFileSystemFactory> hadoopFileSystemFactories;
  private final ClusterInitializer clusterInitializer;

  public HadoopFileSystemLocatorImpl( List<HadoopFileSystemFactory> hadoopFileSystemFactories,
                                      ClusterInitializer clusterInitializer ) {
    this.hadoopFileSystemFactories = hadoopFileSystemFactories;
    this.clusterInitializer = clusterInitializer;
  }

  @Override public HadoopFileSystem getHadoopFilesystem( NamedCluster namedCluster )
    throws ClusterInitializationException {
    clusterInitializer.initialize( namedCluster );
    for ( HadoopFileSystemFactory hadoopFileSystemFactory : hadoopFileSystemFactories ) {
      if ( hadoopFileSystemFactory.canHandle( namedCluster ) ) {
        try {
          return hadoopFileSystemFactory.create( namedCluster );
        } catch ( IOException e ) {
          LOGGER.warn( "Unable to create hdfs service with " + hadoopFileSystemFactory + " for " + namedCluster, e );
        }
      }
    }
    return null;
  }
}
