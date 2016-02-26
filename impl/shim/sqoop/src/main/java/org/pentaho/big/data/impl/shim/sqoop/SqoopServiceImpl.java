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

package org.pentaho.big.data.impl.shim.sqoop;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.sqoop.SqoopService;
import org.pentaho.hadoop.shim.api.Configuration;
import org.pentaho.hadoop.shim.spi.HadoopShim;
import org.pentaho.hadoop.shim.spi.SqoopShim;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SqoopServiceImpl implements SqoopService {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger( SqoopServiceImpl.class );
  public static final String SQOOP_THROW_ON_ERROR = "sqoop.throwOnError";

  private final HadoopShim hadoopShim;
  private final SqoopShim sqoopShim;
  private final NamedCluster namedCluster;

  public SqoopServiceImpl( HadoopShim hadoopShim, SqoopShim sqoopShim, NamedCluster namedCluster ) {
    this.hadoopShim = hadoopShim;
    this.sqoopShim = sqoopShim;
    this.namedCluster = namedCluster;
  }

  @Override public int runTool( List<String> args, Properties properties ) {
    Configuration configuration = hadoopShim.createConfiguration();
    for ( Map.Entry<String, String> entry : Maps.fromProperties( properties ).entrySet() ) {
      configuration.set( entry.getKey(), entry.getValue() );
    }

    try {
      // Make sure Sqoop throws exceptions instead of returning a status of 1
      System.setProperty( SQOOP_THROW_ON_ERROR, Boolean.toString( true ) );

      configureShim( configuration );
      return sqoopShim.runTool( args.toArray( new String[args.size()] ), configuration );
    } catch ( Exception e ) {
      LOGGER.error( "Error executing sqoop", e );
      return 1;
    }
  }

  public void configureShim( Configuration conf ) throws Exception {
    List<String> messages = Lists.newArrayList();

    if ( namedCluster.isMapr() ) {
      hadoopShim.configureConnectionInformation( "", "", "", "", conf, messages );
    } else {
      hadoopShim.configureConnectionInformation(
        namedCluster.environmentSubstitute( namedCluster.getHdfsHost() ),
        namedCluster.environmentSubstitute( namedCluster.getHdfsPort() ),
        namedCluster.environmentSubstitute( namedCluster.getJobTrackerHost() ),
        namedCluster.environmentSubstitute( namedCluster.getJobTrackerPort() ), conf, messages );
    }

    for ( String m : messages ) {
      LOGGER.info( m );
    }
  }

}
