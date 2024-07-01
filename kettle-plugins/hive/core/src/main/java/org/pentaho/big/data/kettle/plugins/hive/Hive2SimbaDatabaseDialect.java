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

import org.pentaho.database.DatabaseDialectException;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.DatabaseConnection;
import org.pentaho.database.model.DatabaseType;
import org.pentaho.database.model.IDatabaseConnection;
import org.pentaho.database.model.IDatabaseType;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.pentaho.big.data.kettle.plugins.hive.SimbaUrl.KRB_HOST_FQDN;
import static org.pentaho.big.data.kettle.plugins.hive.SimbaUrl.KRB_SERVICE_NAME;

/**
 * User: Dzmitry Stsiapanau Date: 8/28/2015 Time: 10:23
 */
public class Hive2SimbaDatabaseDialect extends Hive2DatabaseDialect {
  public static final String SOCKET_TIMEOUT_OPTION = "SocketTimeout";
  public static final String DEFAULT_SOCKET_TIMEOUT = "10";

  public Hive2SimbaDatabaseDialect() {
    super();
  }

  /**
   * UID for serialization
   */
  private static final long serialVersionUID = -8456961348836455937L;

  private static final IDatabaseType DBTYPE =
    new DatabaseType( "Hadoop Hive 2 (Simba)", "HIVE2SIMBA",
      DatabaseAccessType.getList( DatabaseAccessType.NATIVE,
        DatabaseAccessType.JNDI ), DEFAULT_PORT,
      "http://www.simba.com/connectors/apache-hadoop-hive-driver" );

  public IDatabaseType getDatabaseType() {
    return DBTYPE;
  }

  @Override
  public String getNativeDriver() {
    return "org.apache.hive.jdbc.HiveSimbaDriver";
  }

  @Override
  public String getURL( IDatabaseConnection databaseConnection ) throws DatabaseDialectException {
    return SimbaUrl.Builder.create()
      .withAccessType( databaseConnection.getAccessType().ordinal() )
      .withDatabaseName( databaseConnection.getDatabaseName() )
      .withPort( databaseConnection.getDatabasePort() )
      .withDefaultPort( getDefaultDatabasePort() )
      .withHostname( databaseConnection.getHostname() )
      .withJdbcPrefix( getNativeJdbcPre() )
      .withUsername( databaseConnection.getUsername() )
      .withPassword( databaseConnection.getPassword() )
      .withIsKerberos( isKerberos( databaseConnection ) )
      .build()
      .getURL();
  }

  private String getExtraProperty( String key, IDatabaseConnection databaseConnection ) {
    return databaseConnection.getAttributes()
      .get( DatabaseConnection.ATTRIBUTE_PREFIX_EXTRA_OPTION + getDatabaseType().getShortName() + "." + key );
  }

  private String getProperty( String key, IDatabaseConnection databaseConnection ) {
    return databaseConnection.getExtraOptions().get( getDatabaseType().getShortName() + "." + key );
  }

  @Override
  public String getNativeJdbcPre() {
    return "jdbc:hive2://";
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { "HiveJDBC41.jar" };
  }

  @Override public boolean initialize( String classname ) {
    return true;
  }

  public boolean isKerberos( IDatabaseConnection databaseConnection ) {
    return !( isNullOrEmpty( getProperty( KRB_HOST_FQDN, databaseConnection ) )
      && isNullOrEmpty( getExtraProperty( KRB_HOST_FQDN, databaseConnection ) )
      && isNullOrEmpty( getProperty( KRB_SERVICE_NAME, databaseConnection ) )
      && isNullOrEmpty( getExtraProperty( KRB_SERVICE_NAME, databaseConnection ) ) );
  }
}
