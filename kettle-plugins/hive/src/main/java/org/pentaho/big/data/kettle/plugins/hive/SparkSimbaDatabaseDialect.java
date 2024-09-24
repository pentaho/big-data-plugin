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
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.DatabaseType;
import org.pentaho.database.model.IDatabaseType;

public class SparkSimbaDatabaseDialect extends Hive2SimbaDatabaseDialect {
  public static final String DB_TYPE_NAME_SHORT = "SPARKSIMBA";

  public SparkSimbaDatabaseDialect() {
    super();
  }

  private static final long serialVersionUID = 5665821298486490578L;

  @VisibleForTesting static final IDatabaseType DBTYPE =
    new DatabaseType( "SparkSQL", DB_TYPE_NAME_SHORT,
      DatabaseAccessType.getList( DatabaseAccessType.NATIVE,
        DatabaseAccessType.JNDI ), SparkSimbaDatabaseMeta.DEFAULT_PORT,
      "http://www.simba.com/drivers/spark-jdbc-odbc/",
        "",
        ImmutableMap.<String, String>builder().put( Joiner.on( "." ).join( DB_TYPE_NAME_SHORT, SOCKET_TIMEOUT_OPTION ),
            DEFAULT_SOCKET_TIMEOUT ).build()
    );

  public IDatabaseType getDatabaseType() {
    return DBTYPE;
  }


  @Override
  public String getNativeDriver() {
    return SparkSimbaDatabaseMeta.DRIVER_CLASS_NAME;
  }

  @Override
  public String getNativeJdbcPre() {
    return SparkSimbaDatabaseMeta.JDBC_URL_PREFIX;
  }

  @Override
  public int getDefaultDatabasePort() {
    return DBTYPE.getDefaultDatabasePort();
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { SparkSimbaDatabaseMeta.JAR_FILE };
  }
}
