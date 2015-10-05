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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializerProvider;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.hadoop.shim.ConfigurationException;

/**
 * Created by bryan on 8/7/15.
 */
public class ClusterInitializerProviderImpl implements ClusterInitializerProvider {
  private final HadoopConfigurationBootstrap hadoopConfigurationBootstrap;

  public ClusterInitializerProviderImpl() {
    this( HadoopConfigurationBootstrap.getInstance() );
  }

  public ClusterInitializerProviderImpl( HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    this.hadoopConfigurationBootstrap = hadoopConfigurationBootstrap;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override public void initialize( NamedCluster namedCluster ) throws ClusterInitializationException {
    try {
      hadoopConfigurationBootstrap.getProvider();
    } catch ( ConfigurationException e ) {
      throw new ClusterInitializationException( e );
    }
  }
}
