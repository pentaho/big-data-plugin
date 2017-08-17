/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.sqoop;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.sqoop.SqoopService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqoopServiceFactoryImpl implements NamedClusterServiceFactory<SqoopService> {
  private static final Logger LOGGER = LoggerFactory.getLogger( SqoopServiceFactoryImpl.class );
  private final boolean isActiveConfiguration;
  private final HadoopConfiguration hadoopConfiguration;

  public SqoopServiceFactoryImpl( boolean isActiveConfiguration,
                                  HadoopConfiguration hadoopConfiguration ) {
    this.isActiveConfiguration = isActiveConfiguration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  @Override
  public Class<SqoopService> getServiceClass() {
    return SqoopService.class;
  }

  @Override
  public boolean canHandle( NamedCluster namedCluster ) {
    boolean ncState = namedCluster == null ? true : !namedCluster.isUseGateway();
    return isActiveConfiguration && ncState;
  }

  @Override
  public SqoopService create( NamedCluster namedCluster ) {
    try {
      HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim();
      SqoopShim sqoopShim = hadoopConfiguration.getSqoopShim();
      return new SqoopServiceImpl( hadoopShim, sqoopShim, namedCluster );
    } catch ( ConfigurationException e ) {
      LOGGER.error( "Unable to load SqoopService for " + namedCluster );
      return null;
    }
  }

}
