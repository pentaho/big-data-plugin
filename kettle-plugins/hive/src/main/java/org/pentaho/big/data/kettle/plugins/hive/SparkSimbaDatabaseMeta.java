/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hive;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;

import java.util.HashMap;
import java.util.Map;

@DatabaseMetaPlugin( type = "SPARKSIMBA", typeDescription = "SparkSQL" )
public class SparkSimbaDatabaseMeta extends BaseSimbaDatabaseMeta {

  @VisibleForTesting static final String JDBC_URL_PREFIX = "jdbc:spark://";
  @VisibleForTesting static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.SparkSqlSimbaDriver";
  @VisibleForTesting static final String JAR_FILE = "SparkJDBC41.jar";
  @VisibleForTesting static final int DEFAULT_PORT = 10015;
  @VisibleForTesting static final String SOCKET_TIMEOUT_OPTION = "SocketTimeout";
  private final String LIMIT_1 = " LIMIT 1";

  public SparkSimbaDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService ) {
    super( driverLocator, namedClusterService );
  }

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_JNDI };
  }

  @Override protected String getJdbcPrefix() {
    return JDBC_URL_PREFIX;
  }

  @Override
  public String getDriverClass() {
    return DRIVER_CLASS_NAME;
  }

  @Override
  public String getSQLQueryFields( String tableName ) {
    return "SELECT * FROM " + tableName + LIMIT_1;
  }

  @Override
  public String getStartQuote() {
    return "`";
  }

  @Override
  public String getEndQuote() {
    return "`";
  }

  @Override
  public String getSQLTableExists( String tablename ) {
    return "SELECT 1 FROM " + tablename + LIMIT_1;
  }

  @Override
  public String getTruncateTableStatement( String tableName ) {
    return "TRUNCATE TABLE " + tableName;
  }

  @Override
  public String getSQLColumnExists( String columnname, String tablename ) {
    return "SELECT " + columnname + " FROM " + tablename + LIMIT_1;
  }

  @Override
  public String getLimitClause( int nrRows ) {
    return " LIMIT " + nrRows;
  }

  @Override
  public String getSelectCountStatement( String tableName ) {
    return SELECT_COUNT_STATEMENT + " " + tableName;
  }

  @Override
  public String getDropColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean use_autoinc,
                                        String pk, boolean semicolon ) {
    return "";
  }

  @Override
  public String getAddColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean useAutoinc,
                                       String pk, boolean semicolon ) {
    return "";
  }

  @Override
  public String getModifyColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean useAutoinc,
                                          String pk, boolean semicolon ) {
    return "";
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { JAR_FILE };
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }

  @Override public Map<String, String> getDefaultOptions() {
    HashMap<String, String> options = new HashMap<>();
    options.put( String.format( "%s.%s", getPluginId(), SOCKET_TIMEOUT_OPTION ), "10" );

    return options;
  }
}
