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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;

/**
 * Created by bryan on 1/27/16.
 */
public class HBaseServiceFactory implements NamedClusterServiceFactory<HBaseService> {
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public HBaseServiceFactory( boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override public Class<HBaseService> getServiceClass() {
    return HBaseService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    String shimIdentifier = null; // TODO: Specify shim
    return ( shimIdentifier == null && isActiveConfiguration ) || hadoopConfiguration.getIdentifier()
      .equals( shimIdentifier );
  }

  @Override public HBaseService create( NamedCluster namedCluster ) {
    try {
      return new HBaseServiceImpl( namedCluster, hadoopConfiguration );
    } catch ( ConfigurationException e ) {
      return null;
    }
  }
}
