/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hive;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;

import java.util.HashMap;
import java.util.Map;

@DatabaseMetaPlugin( type = "IMPALASIMBA", typeDescription = "Cloudera Impala" )
public class ImpalaSimbaDatabaseMeta extends BaseSimbaDatabaseMeta {

  protected static final String JAR_FILE = "ImpalaJDBC41.jar";
  protected static final String JDBC_URL_PREFIX = "jdbc:impala://";
  protected static final String DRIVER_CLASS_NAME = "com.cloudera.impala.jdbc41.Driver";
  protected static final int DEFAULT_PORT = 21050;
  protected static final String SOCKET_TIMEOUT_OPTION = "SocketTimeout";

  public ImpalaSimbaDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService ) {
    super( driverLocator, namedClusterService );
  }

  @Override protected String getJdbcPrefix() {
    return JDBC_URL_PREFIX;
  }

  @Override
  public String getDriverClass() {
    return DRIVER_CLASS_NAME;
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { JAR_FILE };
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }

  @Override
  public Map<String, String> getDefaultOptions() {
    HashMap<String, String> options = new HashMap<>();
    options.put( String.format( "%s.%s", getPluginId(), SOCKET_TIMEOUT_OPTION ), "10" );

    return options;
  }
}
