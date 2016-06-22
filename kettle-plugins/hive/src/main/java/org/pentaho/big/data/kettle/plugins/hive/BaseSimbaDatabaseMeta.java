/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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
import org.pentaho.big.data.api.jdbc.DriverLocator;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

public abstract class BaseSimbaDatabaseMeta extends Hive2DatabaseMeta {

  @VisibleForTesting static final String ODBC_DRIVER_CLASS_NAME = "sun.jdbc.odbc.JdbcOdbcDriver";
  @VisibleForTesting static final String KRB_HOST_FQDN = "KrbHostFQDN";
  @VisibleForTesting static final String KRB_SERVICE_NAME = "KrbServiceName";
  @VisibleForTesting static final String URL_IS_CONFIGURED_THROUGH_JNDI = "Url is configured through JNDI";
  @VisibleForTesting static final String JDBC_ODBC_S = "jdbc:odbc:%s";

  private final String jdbcUrlTemplate;
  private static final String DEFAULT_DB = "default";

  public BaseSimbaDatabaseMeta( DriverLocator driverLocator ) {
    super( driverLocator );
    jdbcUrlTemplate = getJdbcPrefix() + "%s:%d/%s;AuthMech=%d%s";
  }

  protected abstract String getJdbcPrefix();

  @Override
  public abstract String getDriverClass();

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, DatabaseMeta.TYPE_ACCESS_JNDI };
  }

  @Override public String getURL( String hostname, String port, String databaseName ) {
    Integer portNumber;
    if ( Const.isEmpty( port ) ) {
      portNumber = getDefaultDatabasePort();
    } else {
      portNumber = Integer.valueOf( port );
    }
    if ( Const.isEmpty( databaseName ) ) {
      databaseName = DEFAULT_DB;
    }
    switch ( getAccessType() ) {
      case DatabaseMeta.TYPE_ACCESS_ODBC: {
        return String.format( JDBC_ODBC_S, databaseName );
      }
      case DatabaseMeta.TYPE_ACCESS_JNDI: {
        return URL_IS_CONFIGURED_THROUGH_JNDI;
      }
      case DatabaseMeta.TYPE_ACCESS_NATIVE:
      default: {
        Integer authMethod = 0;
        StringBuilder additional = new StringBuilder();
        String userName = getUsername();
        String password = getPassword();
        String krbFQDN = getProperty( KRB_HOST_FQDN );
        String extraKrbFQDN = getExtraProperty( KRB_HOST_FQDN );
        String krbPrincipal = getProperty( KRB_SERVICE_NAME );
        String extraKrbPrincipal = getExtraProperty( KRB_SERVICE_NAME );
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
        return String.format( jdbcUrlTemplate, hostname, portNumber, databaseName, authMethod, additional );
      }
    }
  }

  private String getExtraProperty( String key ) {
    return getAttributes().getProperty( ATTRIBUTE_PREFIX_EXTRA_OPTION + getPluginId() + "." + key );
  }

  private String getProperty( String key ) {
    return getAttributes().getProperty( key );
  }

  /**
   * This method assumes that Hive has no concept of primary and technical keys and auto increment columns. We are
   * ignoring the tk, pk and useAutoinc parameters.
   */
  @Override
  public String getFieldDefinition( ValueMetaInterface v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldname, boolean addCr ) {
    StringBuilder retval = new StringBuilder();

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldname ) {
      retval.append( fieldname ).append( ' ' );
    }
    int type = v.getType();
    switch ( type ) {
      case ValueMetaInterface.TYPE_BOOLEAN:
        retval.append( "BOOLEAN" );
        break;

      case ValueMetaInterface.TYPE_DATE:
        retval.append( "DATE" );
        break;

      case ValueMetaInterface.TYPE_TIMESTAMP:
        retval.append( "TIMESTAMP" );
        break;

      case ValueMetaInterface.TYPE_STRING:
        retval.append( "VARCHAR" );
        break;

      case ValueMetaInterface.TYPE_NUMBER:
      case ValueMetaInterface.TYPE_INTEGER:
      case ValueMetaInterface.TYPE_BIGNUMBER:
        // Integer values...
        if ( precision == 0 ) {
          if ( length > 9 ) {
            if ( length < 19 ) {
              // can hold signed values between -9223372036854775808 and 9223372036854775807
              // 18 significant digits
              retval.append( "BIGINT" );
            } else {
              retval.append( "FLOAT" );
            }
          } else {
            retval.append( "INT" );
          }
        } else {
          // Floating point values...
          if ( length > 15 ) {
            retval.append( "FLOAT" );
          } else {
            // A double-precision floating-point number is accurate to approximately 15 decimal places.
            // http://mysql.mirrors-r-us.net/doc/refman/5.1/en/numeric-type-overview.html
            retval.append( "DOUBLE" );
          }
        }
        break;
    }
    return retval.toString();
  }
}
