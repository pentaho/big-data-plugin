/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.core.database;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

@DatabaseMetaPlugin( type = "IMPALA", typeDescription = "Impala" )
public class ImpalaDatabaseMeta extends Hive2DatabaseMeta implements DatabaseInterface {

  protected static final String JAR_FILE = "hive-jdbc-cdh4.2.0-release-pentaho.jar";
  protected static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
  protected static final int DEFAULT_PORT = 21050;

  protected Integer driverMajorVersion;
  protected Integer driverMinorVersion;

  public ImpalaDatabaseMeta() throws Throwable {
  }

  /**
   * Package protected constructor for unit testing.
   *
   * @param majorVersion The majorVersion to set for the driver
   * @param minorVersion The minorVersion to set for the driver
   * @throws Throwable
   */
  ImpalaDatabaseMeta( int majorVersion, int minorVersion ) throws Throwable {
    driverMajorVersion = majorVersion;
    driverMinorVersion = minorVersion;
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[]{ DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override
  public String getDriverClass() {

    //  !!!  We will probably have to change this if we are providing our own driver,
    //  i.e., before our code is committed to the Hadoop Hive project.
    return DRIVER_CLASS_NAME;
  }

  /**
   * This method assumes that Hive has no concept of primary and technical keys and auto increment columns. We are
   * ignoring the tk, pk and useAutoinc parameters.
   */
  @Override
  public String getFieldDefinition( ValueMetaInterface v, String tk, String pk, boolean useAutoinc,
                                    boolean addFieldname, boolean addCr ) {

    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if ( addFieldname ) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch ( type ) {

      case ValueMetaInterface.TYPE_BOOLEAN:
        retval += "BOOLEAN";
        break;

      // Hive does not support DATE until 0.12 - check Impala version against Hive
      case ValueMetaInterface.TYPE_DATE:
      case ValueMetaInterface.TYPE_TIMESTAMP:
        if ( isDriverVersion( 0, 8 ) ) {
          retval += "TIMESTAMP";
        } else {
          throw new IllegalArgumentException( "Timestamp types not supported in this version of Impala" );
        }
        break;

      case ValueMetaInterface.TYPE_STRING:
        retval += "STRING";
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
              retval += "BIGINT";
            } else {
              retval += "FLOAT";
            }
          } else {
            retval += "INT";
          }
        } else {
          // Floating point values...
          if ( length > 15 ) {
            retval += "FLOAT";
          } else {
            // A double-precision floating-point number is accurate to approximately 15 decimal places.
            // http://mysql.mirrors-r-us.net/doc/refman/5.1/en/numeric-type-overview.html
            retval += "DOUBLE";
          }
        }

        break;
    }

    return retval;
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) throws KettleDatabaseException {

    if ( Const.isEmpty( port ) ) {
      port = Integer.toString( getDefaultDatabasePort() );
    }
    String principal = getAttributes().getProperty( "principal" );
    String extraPrincipal =
      getAttributes().getProperty( ATTRIBUTE_PREFIX_EXTRA_OPTION + getPluginId() + ".principal" );
    if ( principal != null || extraPrincipal != null ) {
      return "jdbc:hive2://" + hostname + ":" + port + "/" + databaseName;
    } else {
      return "jdbc:hive2://" + hostname + ":" + port + "/" + databaseName + ";auth=noSasl";
    }
  }

  @Override
  public String[] getUsedLibraries() {

    return new String[]{ JAR_FILE };
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }
}
