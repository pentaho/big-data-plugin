/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
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
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC ) {
      return ODBC_DRIVER_CLASS_NAME;
    } else {
      return DRIVER_CLASS_NAME;
    }
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
