/*
 * ! ******************************************************************************
 *  *
 *  * Pentaho Data Integration
 *  *
 *  * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *  *
 *  *******************************************************************************
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with
 *  * the License. You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  *
 *  *****************************************************************************
 */

package org.pentaho.di.core.database;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

@DatabaseMetaPlugin( type = "IMPALASIMBA", typeDescription = "Impala with Simba driver" )
public class ImpalaSimbaDatabaseMeta extends ImpalaDatabaseMeta implements DatabaseInterface {

  protected static final String JAR_FILE = "ImpalaJDBC41.jar";
  //protected static final String DRIVER_CLASS_NAME = "com.simba.impala.jdbc41.Driver";
  protected static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.ImpalaSimbaDriver";
  protected static final String JDBC_URL_TEMPLATE = "jdbc:impala://%s:%d/%s;AuthMech=%d%s";

  public ImpalaSimbaDatabaseMeta() throws Throwable {
  }

  /**
   * Package protected constructor for unit testing.
   *
   * @param majorVersion The majorVersion to set for the driver
   * @param minorVersion The minorVersion to set for the driver
   * @throws Throwable
   */
  ImpalaSimbaDatabaseMeta( int majorVersion, int minorVersion ) throws Throwable {
    driverMajorVersion = majorVersion;
    driverMinorVersion = minorVersion;
  }

  @Override public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, DatabaseMeta.TYPE_ACCESS_JNDI };
  }

  @Override
  public String getDriverClass() {
    if ( getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC ) {
      return "sun.jdbc.odbc.JdbcOdbcDriver";
    } else {
      return DRIVER_CLASS_NAME;
    }
  }

  @Override public String getURL( String hostname, String port, String databaseName ) throws KettleDatabaseException {
    Integer portNumber;
    if ( Const.isEmpty( port ) ) {
      portNumber = getDefaultDatabasePort();
    } else {
      portNumber = Integer.decode( port );
    }
    if ( Const.isEmpty( databaseName ) ) {
      databaseName = "default";
    }
    switch ( getAccessType() ) {
      case DatabaseMeta.TYPE_ACCESS_ODBC: {
        return String.format( "jdbc:odbc:%s", databaseName );
      }
      case DatabaseMeta.TYPE_ACCESS_JNDI: {
        return "Url is configured through JNDI";
      }
      case DatabaseMeta.TYPE_ACCESS_NATIVE:
      default: {
        Integer authMethod = 0;
        StringBuilder additional = new StringBuilder();
        String userName = getUsername();
        String password = getPassword();
        String krbFQDN = getProperty( "KrbHostFQDN" );
        String extraKrbFQDN = getExtraProperty( "KrbHostFQDN" );
        String krbPrincipal = getProperty( "KrbServiceName" );
        String extraKrbPrincipal = getExtraProperty( "KrbServiceName" );
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
        return String.format( JDBC_URL_TEMPLATE, hostname, portNumber, databaseName, authMethod, additional );
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

      //  Hive does not support DATE until 0.12
      case ValueMetaInterface.TYPE_DATE:
        if ( isDriverVersion( 0, 12 ) ) {
          retval.append( "DATE" );
        } else {
          throw new IllegalArgumentException( "Date types not supported in this version of Hive" );
        }
        break;

      // Hive does not support DATE until 0.8
      case ValueMetaInterface.TYPE_TIMESTAMP:
        if ( isDriverVersion( 0, 8 ) ) {
          retval.append( "TIMESTAMP" );
        } else {
          throw new IllegalArgumentException( "Timestamp types not supported in this version of Hive" );
        }
        break;

      case ValueMetaInterface.TYPE_STRING:
        if ( length == 1 && isDriverVersion( 0, 13 ) ) {
          retval.append( "CHAR" );
        } else if ( isDriverVersion( 0, 12 ) ) {
          retval.append( "VARCHAR" );
        } else {
          retval.append( "STRING" );
        }
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
