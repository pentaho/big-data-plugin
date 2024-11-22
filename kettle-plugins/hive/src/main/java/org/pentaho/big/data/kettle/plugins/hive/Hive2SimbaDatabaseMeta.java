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

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;

// Intenionally disabled.  The Simba Hive driver is currently unsupported.
//@DatabaseMetaPlugin( type = "HIVE2SIMBA", typeDescription = "Hadoop Hive 2 with Simba Driver" )
public class Hive2SimbaDatabaseMeta extends BaseSimbaDatabaseMeta {

  @VisibleForTesting static final String JAR_FILE = "HiveJDBC41.jar";
  @VisibleForTesting static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveSimbaDriver";
  @VisibleForTesting static final String JDBC_URL_PREFIX = "jdbc:hive2://";
  @VisibleForTesting static final int DEFAULT_PORT = 10000;


  public Hive2SimbaDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService ) {
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
}
