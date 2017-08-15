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
package org.pentaho.big.data.impl.shim.format;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceFactory;
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;

public class FormatServiceFactory implements NamedClusterServiceFactory<FormatService> {
  private final HadoopConfiguration hadoopConfiguration;

  public FormatServiceFactory( HadoopConfiguration hadoopConfiguration ) {
    this.hadoopConfiguration = hadoopConfiguration;
  }
  @Override public Class<FormatService> getServiceClass() {
    return FormatService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override public FormatService create( NamedCluster namedCluster ) {
    try {
      return new FormatServiceImpl( namedCluster, hadoopConfiguration );
    } catch ( ConfigurationException e ) {
      throw new RuntimeException( "Error getting format shim ", e );
    }
  }
}
