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

import java.sql.Driver;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

@DatabaseMetaPlugin( type = "HIVE", typeDescription = "Hadoop Hive" )
public class HiveDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

  protected static final String JAR_FILE = "hive-jdbc-cdh4.2.0-release-pentaho.jar";
  protected static final String DRIVER_CLASS_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
  protected static final int DEFAULT_PORT = 10000;

  protected Integer driverMajorVersion;
  protected Integer driverMinorVersion;

  public HiveDatabaseMeta() throws Throwable {
  }

  /**
   * Package protected constructor for unit testing.
   *
   * @param majorVersion The majorVersion to set for the driver
   * @param minorVersion The minorVersion to set for the driver
   * @throws Throwable
   */
  HiveDatabaseMeta( int majorVersion, int minorVersion ) throws Throwable {
    driverMajorVersion = majorVersion;
    driverMinorVersion = minorVersion;
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[]{ DatabaseMeta.TYPE_ACCESS_NATIVE };
  }

  @Override
  public String getAddColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean useAutoinc,
                                       String pk, boolean semicolon ) {

    return "ALTER TABLE " + tablename + " ADD " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );

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

      // Hive does not support DATE until 0.12
      case ValueMetaInterface.TYPE_DATE:
        if ( isDriverVersion( 0, 12 ) ) {
          retval += "DATE";
        } else {
          throw new IllegalArgumentException( "Date types not supported in this version of Hive" );
        }
        break;

      // Hive does not support DATE until 0.8
      case ValueMetaInterface.TYPE_TIMESTAMP:
        if ( isDriverVersion( 0, 8 ) ) {
          retval += "TIMESTAMP";
        } else {
          throw new IllegalArgumentException( "Timestamp types not supported in this version of Hive" );
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
  public String getModifyColumnStatement( String tablename, ValueMetaInterface v, String tk, boolean useAutoinc,
                                          String pk, boolean semicolon ) {

    return "ALTER TABLE " + tablename + " MODIFY " + getFieldDefinition( v, tk, pk, useAutoinc, true, false );
  }

  @Override
  public String getURL( String hostname, String port, String databaseName ) throws KettleDatabaseException {

    if ( Const.isEmpty( port ) ) {
      Integer.toString( getDefaultDatabasePort() );
    }

    return "jdbc:hive://" + hostname + ":" + port + "/" + databaseName;
  }

  @Override
  public String[] getUsedLibraries() {

    return new String[]{ JAR_FILE };
  }

  /**
   * Build the SQL to count the number of rows in the passed table.
   *
   * @param tableName
   * @return
   */
  @Override
  public String getSelectCountStatement( String tableName ) {
    return "select count(1) from " + tableName;
  }

  @Override
  public String generateColumnAlias( int columnIndex, String suggestedName ) {
    if ( isDriverVersion( 0, 6 ) ) {
      return suggestedName;
    } else {
      // For version 0.5 and prior:
      // Column aliases are currently not supported in Hive.  The default column alias
      // generated is in the format '_col##' where ## = column index.  Use this format
      // so the result can be mapped back correctly.
      return "_col" + String.valueOf( columnIndex ); //$NON-NLS-1$
    }
  }

  protected synchronized void initDriverInfo() {
    Integer majorVersion = 0;
    Integer minorVersion = 0;

    try {
      // Load the driver version number
      Class<?> driverClass = Class.forName( DRIVER_CLASS_NAME ); //$NON-NLS-1$
      if ( driverClass != null ) {
        Driver driver = (Driver) driverClass.getConstructor().newInstance();
        majorVersion = driver.getMajorVersion();
        minorVersion = driver.getMinorVersion();
      }
    } catch ( Exception e ) {
      // Failed to load the driver version, leave at the defaults
    }

    driverMajorVersion = majorVersion;
    driverMinorVersion = minorVersion;
  }

  /**
   * Check that the version of the driver being used is at least the driver you want. If you do not care about the minor
   * version, pass in a 0 (The assumption being that the minor version will ALWAYS be 0 or greater)
   *
   * @return true: the version being used is equal to or newer than the one you requested false: the version being used
   * is older than the one you requested
   */
  protected boolean isDriverVersion( int majorVersion, int minorVersion ) {
    if ( driverMajorVersion == null ) {
      initDriverInfo();
    }

    if ( majorVersion < driverMajorVersion ) {
      // Driver major version is newer than the requested version
      return true;
    } else if ( majorVersion == driverMajorVersion ) {
      // Driver major version is the same as requested, check the minor version
      if ( minorVersion <= driverMinorVersion ) {
        // Driver minor version is the same, or newer than requested
        return true;
      }
    }

    return false;
  }

  /**
   * Quotes around table names are not valid Hive QL
   * <p/>
   * return an empty string for the start quote
   */
  public String getStartQuote() {
    return "";
  }

  /**
   * Quotes around table names are not valid Hive QL
   * <p/>
   * return an empty string for the end quote
   */
  public String getEndQuote() {
    return "";
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }

  /**
   * @return a list of table types to retrieve tables for the database
   */
  @Override
  public String[] getTableTypes() {
    return null;
  }

  /**
   * @return a list of table types to retrieve views for the database
   */
  @Override
  public String[] getViewTypes() {
    return new String[]{ "VIEW", "VIRTUAL_VIEW" };
  }

  /**
   * @param tableName The table to be truncated.
   * @return The SQL statement to truncate a table: remove all rows from it without a transaction
   */
  @Override
  public String getTruncateTableStatement( String tableName ) {
    if ( isDriverVersion(0, 11) ) {
      return "TRUNCATE TABLE " + tableName;
    }
    return null;
  }

  @Override
  public boolean supportsSetCharacterStream() {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() {
    return false;
  }

  private StringBuilder getConnectSqlForNotDefaultDatabaseName() {
    StringBuilder sql = new StringBuilder( "use " );
    sql.append( getDatabaseName() ).append( ';' ).append( ' ' );
    return sql;
  }

  /**
   * @return The SQL to execute right after connecting
   */
  @Override
  public String getConnectSQL() {
    StringBuilder sql = getConnectSqlForNotDefaultDatabaseName();
    String superSql = super.getConnectSQL();
    if ( !Const.isEmpty( superSql ) ) {
      int startPos = superSql.indexOf( "use " );
      if ( startPos >= 0 ) {
        int endPos = superSql.indexOf( ";", startPos );
        if ( endPos >= 0 ) {
          if ( endPos < superSql.length() ) {
            endPos++;
          }
          String superSql1 = superSql.substring( 0, startPos ) + superSql.substring( endPos );
          superSql = superSql1;
        }
      }
      return sql.append( superSql ).toString();
    }
    return sql.toString();
  }

  /**
   * @param sql The SQL to execute right after connecting
   */
  @Override
  public void setConnectSQL( String sql ) {
    super.setConnectSQL( sql.replaceAll( getConnectSqlForNotDefaultDatabaseName().toString(), "" ) );
  }

  @Override
  public boolean supportsTimeStampToDateConversion() {
    return false;
  }
}
