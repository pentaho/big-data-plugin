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

package org.pentaho.big.data.kettle.plugins.hive;

import org.pentaho.database.DatabaseDialectException;
import org.pentaho.database.model.DatabaseAccessType;
import org.pentaho.database.model.DatabaseConnection;
import org.pentaho.database.model.DatabaseType;
import org.pentaho.database.model.IDatabaseConnection;
import org.pentaho.database.model.IDatabaseType;
import org.pentaho.database.util.Const;

/**
 * User: Dzmitry Stsiapanau Date: 8/28/2015 Time: 10:23
 */
public class Hive2SimbaDatabaseDialect extends Hive2DatabaseDialect {

  public Hive2SimbaDatabaseDialect() {
    super();
  }

  /**
   * UID for serialization
   */
  private static final long serialVersionUID = -8456961348836455937L;

  protected static final String JDBC_URL_TEMPLATE = "jdbc:hive2://%s:%s/%s;AuthMech=%d%s";

  private static final IDatabaseType DBTYPE =
    new DatabaseType( "Hadoop Hive 2 (Simba)", "HIVE2SIMBA",
      DatabaseAccessType.getList( DatabaseAccessType.NATIVE,
        DatabaseAccessType.JNDI, DatabaseAccessType.ODBC ), DEFAULT_PORT,
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
    switch ( databaseConnection.getAccessType() ) {
      case ODBC: {
        return String.format( "jdbc:odbc:%s", databaseConnection.getDatabaseName() );
      }
      case JNDI: {
        return "Url is configured through JNDI";
      }
      case NATIVE:
      default: {
        Integer authMethod = 0;
        StringBuilder additional = new StringBuilder();
        String userName = databaseConnection.getUsername();
        String password = databaseConnection.getPassword();
        String krbFQDN = getProperty( "KrbHostFQDN", databaseConnection );
        String extraKrbFQDN = getExtraProperty( "KrbHostFQDN", databaseConnection );
        String krbPrincipal = getProperty( "KrbServiceName", databaseConnection );
        String extraKrbPrincipal = getExtraProperty( "KrbServiceName", databaseConnection );
        if ( ( !Const.isEmpty( krbPrincipal ) || !Const.isEmpty( extraKrbPrincipal ) ) && ( !Const.isEmpty( krbFQDN )
          || !Const.isEmpty( extraKrbFQDN ) ) ) {
          authMethod = 1;
        } else if ( !Const.isEmpty( userName ) ) {
          additional.append( ";UID=" );
          additional.append( userName );
          if ( !Const.isEmpty( password ) ) {
            authMethod = 3;
            additional.append( ";PWD=" );
            additional.append( password );
          } else {
            authMethod = 2;
          }
        }
        return String.format( getJdbcUrlTemplate(), databaseConnection.getHostname(), databaseConnection.getDatabasePort(),
          databaseConnection.getDatabaseName(), authMethod, additional );
      }
    }
  }

  protected String getJdbcUrlTemplate() {
    return JDBC_URL_TEMPLATE;
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
}
