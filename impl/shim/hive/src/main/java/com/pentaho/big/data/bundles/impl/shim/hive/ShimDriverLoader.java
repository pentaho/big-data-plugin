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

package com.pentaho.big.data.bundles.impl.shim.hive;

import org.pentaho.big.data.api.jdbc.DriverRegistry;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;
import org.pentaho.di.core.database.DelegatingDriver;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.hadoop.HadoopConfigurationListener;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Created by bryan on 3/29/16.
 */
public class ShimDriverLoader implements HadoopConfigurationListener {
  public static final String HIVE = "hive";
  public static final String HIVE_2 = "hive2";
  public static final String HIVE_2_SIMBA = "hive2Simba";
  public static final String IMPALA = "Impala";
  public static final String IMPALA_SIMBA = "ImpalaSimba";

  private final Logger LOGGER = LoggerFactory.getLogger( ShimDriverLoader.class );

  private final JdbcUrlParser jdbcUrlParser;
  private final ConcurrentMap<HadoopConfiguration, ConcurrentMap<String, Driver>> configMap;
  private final Map<String, DriverFactory> hiveDriverFactoryMap;
  private final DriverRegistry driverRegistry;

  public ShimDriverLoader( JdbcUrlParser jdbcUrlParser, DriverRegistry driverRegistry ) {
    this( jdbcUrlParser, driverRegistry, HadoopConfigurationBootstrap.getInstance(), initHiveDriverFactoryMap() );
  }

  public ShimDriverLoader( JdbcUrlParser jdbcUrlParser, DriverRegistry driverRegistry,
                           HadoopConfigurationBootstrap hadoopConfigurationBootstrap ) {
    this( jdbcUrlParser, driverRegistry, hadoopConfigurationBootstrap, initHiveDriverFactoryMap() );
  }

  public ShimDriverLoader( JdbcUrlParser jdbcUrlParser,
                           DriverRegistry driverRegistry, HadoopConfigurationBootstrap hadoopConfigurationBootstrap,
                           Map<String, DriverFactory> hiveDriverFactoryMap ) {
    this.jdbcUrlParser = jdbcUrlParser;
    this.driverRegistry = driverRegistry;
    this.configMap = new ConcurrentHashMap<>();
    this.hiveDriverFactoryMap = new ConcurrentHashMap<>( hiveDriverFactoryMap );
    try {
      hadoopConfigurationBootstrap.registerHadoopConfigurationListener( this );
    } catch ( ConfigurationException e ) {
      LOGGER.error( "Unable to register " + this, e );
    }
  }

  private static Map<String, DriverFactory> initHiveDriverFactoryMap() {
    Map<String, DriverFactory> hiveDriverFactoryMap = new HashMap<>();
    hiveDriverFactoryMap.put( HIVE, HiveDriver::new );
    hiveDriverFactoryMap.put( HIVE_2, HiveDriver::new );
    hiveDriverFactoryMap.put( HIVE_2_SIMBA, HiveSimbaDriver::new );
    hiveDriverFactoryMap.put( IMPALA, ImpalaDriver::new );
    hiveDriverFactoryMap.put( IMPALA_SIMBA, ImpalaSimbaDriver::new );
    return hiveDriverFactoryMap;
  }

  @Override public void onClassLoaderAvailable( ClassLoader classLoader ) {
    // Noop
  }

  @Override public void onConfigurationOpen( HadoopConfiguration hadoopConfiguration, boolean defaultConfiguration ) {
    if ( hadoopConfiguration == null ) {
      return;
    }
    configMap.put( hadoopConfiguration, hiveDriverFactoryMap.entrySet().stream()
      .map( pair -> {
        Driver jdbcDriver;
        try {
          jdbcDriver = hadoopConfiguration.getHadoopShim().getJdbcDriver( pair.getKey() );
        } catch ( Throwable e ) {
          return null;
        }
        if ( jdbcDriver == null ) {
          return null;
        }
        Driver hiveDriver = pair.getValue()
          .create( jdbcDriver, hadoopConfiguration.getIdentifier(), defaultConfiguration, jdbcUrlParser );
        if ( hiveDriver == null ) {
          return null;
        }
        DelegatingDriver driver = new DelegatingDriver( hiveDriver );
        try {
          driverRegistry.registerDriver( driver );
        } catch ( SQLException e ) {
          LOGGER.error( "Unable to register " + hiveDriver, e );
          return null;
        }
        return new Map.Entry<String, Driver>() {

          @Override
          public String getKey() {
            return pair.getKey();
          }

          @Override
          public Driver getValue() {
            return driver;
          }

          @Override
          public Driver setValue( Driver value ) {
            throw new UnsupportedOperationException();
          }
        };
      } )
      .filter( Objects::nonNull )
      .collect( Collectors.toMap( entry -> entry.getKey(), entry -> entry.getValue(), ( u, v ) -> {
        throw new IllegalStateException( String.format( "Duplicate key %s", u ) );
      }, ConcurrentHashMap::new ) ) );
  }

  @Override public void onConfigurationClose( HadoopConfiguration hadoopConfiguration ) {
    if ( hadoopConfiguration == null ) {
      return;
    }
    Map<String, Driver> hiveDrivers = configMap.remove( hadoopConfiguration );
    if ( hiveDrivers == null ) {
      return;
    }
    hiveDrivers.values().forEach( hiveDriver -> {
      try {
        driverRegistry.deregisterDriver( hiveDriver );
      } catch ( SQLException e ) {
        LOGGER.error( "Unable to deregister " + hiveDriver );
      }
    } );
  }

  protected interface DriverFactory {
    Driver create( Driver delegate, String hadoopConfigurationId, boolean defaultConfiguration,
                   JdbcUrlParser jdbcUrlParser );
  }
}
