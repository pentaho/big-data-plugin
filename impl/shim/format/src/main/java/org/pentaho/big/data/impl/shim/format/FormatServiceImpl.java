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
import org.pentaho.bigdata.api.format.FormatService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat;
import org.pentaho.hadoop.shim.spi.FormatShim;

public class FormatServiceImpl implements FormatService {

  private final FormatShim formatShim;
  private final NamedCluster namedCluster;
  private final HadoopConfiguration hadoopConfiguration;

  public FormatServiceImpl( NamedCluster namedCluster, HadoopConfiguration hadoopConfiguration )
    throws ConfigurationException {
    this.namedCluster = namedCluster;
    this.hadoopConfiguration = hadoopConfiguration;
    this.formatShim = hadoopConfiguration.getFormatShim();
  }

  @Override
  public <T extends IPentahoInputFormat> T createInputFormat( Class<T> type ) throws Exception {
    return formatShim.createInputFormat( type );
  }

  @Override
  public <T extends IPentahoOutputFormat> T createOutputFormat( Class<T> type ) throws Exception {
    return formatShim.createOutputFormat( type );
  }
}
