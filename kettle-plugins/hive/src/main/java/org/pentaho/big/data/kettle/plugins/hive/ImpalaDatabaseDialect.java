/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

public class ImpalaDatabaseDialect extends Hive2DatabaseDialect {

  public ImpalaDatabaseDialect() {
    super();
  }

  /**
   * UID for serialization
   */
  private static final long serialVersionUID = -6685869374136347923L;

  private static final int DEFAULT_PORT = 21050;

  private static final IDatabaseType DBTYPE =
    new DatabaseType( "Impala", "IMPALA", DatabaseAccessType.getList( DatabaseAccessType.NATIVE,
      DatabaseAccessType.JNDI ), DEFAULT_PORT,
      "http://www.cloudera.com/content/support/en/documentation/cloudera-impala/cloudera-impala-documentation-v1"
        + "-latest.html" );

  public IDatabaseType getDatabaseType() {
    return DBTYPE;
  }

  @Override
  public String getNativeDriver() {
    return "org.apache.hive.jdbc.ImpalaDriver";
  }

  @Override
  public String getURL( IDatabaseConnection connection ) throws DatabaseDialectException {
    StringBuffer urlBuffer = new StringBuffer( getNativeJdbcPre() );
    /*
     * String username = connection.getUsername(); if(username != null && !"".equals(username)) {
     * urlBuffer.append(username); String password = connection.getPassword(); if(password != null &&
     * !"".equals(password)) { urlBuffer.append(":"); urlBuffer.append(password); } urlBuffer.append("@"); }
     */
    urlBuffer.append( connection.getHostname() );
    urlBuffer.append( ":" );
    urlBuffer.append( connection.getDatabasePort() );
    urlBuffer.append( "/" );
    urlBuffer.append( connection.getDatabaseName() );

    String principalPropertyName = getDatabaseType().getShortName() + ".principal";
    String principal = connection.getExtraOptions().get( principalPropertyName );
    String extraPrincipal =
      connection.getAttributes().get( DatabaseConnection.ATTRIBUTE_PREFIX_EXTRA_OPTION + principalPropertyName );
    urlBuffer.append( ";impala_db=true" );
    if ( principal != null || extraPrincipal != null ) {
      return urlBuffer.toString();
    }

    urlBuffer.append( ";auth=noSasl" );
    return urlBuffer.toString();
  }

  @Override
  public String getNativeJdbcPre() {
    return "jdbc:hive2://";
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { "pentaho-hadoop-hive-jdbc-shim-1.4-SNAPSHOT.jar" };
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }

  @Override public boolean initialize( String classname ) {
    return true;
  }
}
