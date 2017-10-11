/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
