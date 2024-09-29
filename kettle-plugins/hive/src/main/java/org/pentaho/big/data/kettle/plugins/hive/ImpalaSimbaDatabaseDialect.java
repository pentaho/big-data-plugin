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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.DatabaseType;
import org.pentaho.database.model.IDatabaseType;

/**
 * User: Dzmitry Stsiapanau Date: 8/28/2015 Time: 10:23
 */
public class ImpalaSimbaDatabaseDialect extends Hive2SimbaDatabaseDialect {
  public static final String DB_TYPE_NAME_SHORT = "IMPALASIMBA";

  public ImpalaSimbaDatabaseDialect() {
    super();
  }

  /**
   * UID for serialization
   */
  private static final long serialVersionUID = -8456961348836455937L;

  protected static final int DEFAULT_PORT = 21050;

  protected static final String JDBC_URL_TEMPLATE = "jdbc:impala://%s:%s/%s;AuthMech=%d%s";

  private static final IDatabaseType DBTYPE =
    new DatabaseType( "Cloudera Impala", DB_TYPE_NAME_SHORT,
      DatabaseAccessType.getList( DatabaseAccessType.NATIVE,
        DatabaseAccessType.JNDI, DatabaseAccessType.ODBC ), DEFAULT_PORT,
      "http://go.cloudera.com/odbc-driver-hive-impala.html",
        "",
        ImmutableMap.<String, String>builder().put( Joiner.on( "." ).join( DB_TYPE_NAME_SHORT, SOCKET_TIMEOUT_OPTION ),
            DEFAULT_SOCKET_TIMEOUT ).build()
    );

  public IDatabaseType getDatabaseType() {
    return DBTYPE;
  }

  @Override
  public String getNativeDriver() {
    return "org.apache.hive.jdbc.ImpalaSimbaDriver";
  }

  @Override
  public String getNativeJdbcPre() {
    return "jdbc:impala://";
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { "ImpalaJDBC41.jar" };
  }


  @Override public boolean initialize( String classname ) {
    return true;
  }
}
