/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.impl.shim.jaas;

import org.pentaho.hadoop.shim.api.jaas.JaasConfigService;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;

public class JaasConfigServiceFactory implements NamedClusterServiceFactory<JaasConfigService> {
  private final HadoopConfiguration hadoopConfiguration;

  public JaasConfigServiceFactory(
    @SuppressWarnings( "unused" ) boolean isActiveConfiguration, HadoopConfiguration hadoopConfiguration ) {
    this.hadoopConfiguration = hadoopConfiguration;
  }
  @Override public Class<JaasConfigService> getServiceClass() {
    return JaasConfigService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override public JaasConfigService create( NamedCluster namedCluster ) {
    return new JaasConfigServiceImpl( hadoopConfiguration.getConfigProperties() );
  }
}
