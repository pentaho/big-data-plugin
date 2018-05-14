/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaInterface;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.pentaho.big.data.kettle.plugins.hive.SimbaUrl.KRB_HOST_FQDN;
import static org.pentaho.big.data.kettle.plugins.hive.SimbaUrl.KRB_SERVICE_NAME;

abstract class BaseSimbaDatabaseMeta extends Hive2DatabaseMeta {

  @VisibleForTesting static final String ODBC_DRIVER_CLASS_NAME = "sun.jdbc.odbc.JdbcOdbcDriver";
  @VisibleForTesting static final String URL_IS_CONFIGURED_THROUGH_JNDI = "Url is configured through JNDI";
  @VisibleForTesting static final String JDBC_ODBC_S = "jdbc:odbc:%s";

  BaseSimbaDatabaseMeta( DriverLocator driverLocator ) {
    super( driverLocator );
  }

  protected abstract String getJdbcPrefix();

  @Override
  public abstract String getDriverClass();

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, DatabaseMeta.TYPE_ACCESS_JNDI };
  }

  @Override public String getURL( String hostname, String port, String databaseName ) {
    return SimbaUrl.Builder.create()
      .withAccessType( getAccessType() )
      .withDatabaseName( databaseName )
      .withPort( port )
      .withDefaultPort( getDefaultDatabasePort() )
      .withHostname( hostname )
      .withJdbcPrefix( getJdbcPrefix() )
      .withUsername( getUsername() )
      .withPassword( getPassword() )
      .withIsKerberos( isKerberos() )
      .build()
      .getURL();
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

  /**
   * Assume kerberos if any of the kerb props have been set.
   */
  private boolean isKerberos() {
    return !( isNullOrEmpty( getProperty( KRB_HOST_FQDN ) )
      && isNullOrEmpty( getExtraProperty( KRB_HOST_FQDN ) )
      && isNullOrEmpty( getProperty( KRB_SERVICE_NAME ) )
      && isNullOrEmpty( getExtraProperty( KRB_SERVICE_NAME ) ) );
  }
}
