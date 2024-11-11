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

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@DatabaseMetaPlugin( type = "IMPALA", typeDescription = "Impala" )
public class ImpalaDatabaseMeta extends Hive2DatabaseMeta implements DatabaseInterface {

  public static final String AUTH_NO_SASL = ";auth=noSasl";
  protected static final String JAR_FILE = "hive-jdbc-cdh4.2.0-release-pentaho.jar";
  protected static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
  protected static final int DEFAULT_PORT = 21050;

  private static final Logger logChannel = LoggerFactory.getLogger( ImpalaDatabaseMeta.class );

  @VisibleForTesting
  ImpalaDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService,
                             MetastoreLocator metastoreLocator ) {
    super( driverLocator, namedClusterService,  metastoreLocator );
  }

  // OSGi constructor
  public ImpalaDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService ) {
    super( driverLocator, namedClusterService );
  }

  @Override
  public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE };
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
  public String getURL( String hostname, String port, String databaseName ) {
    StringBuilder urlBuffer = new StringBuilder();
    if ( Const.isEmpty( port ) ) {
      port = Integer.toString( getDefaultDatabasePort() );
    }
    String principal = getAttributes().getProperty( "principal" );
    String extraPrincipal =
      getAttributes().getProperty( ATTRIBUTE_PREFIX_EXTRA_OPTION + getPluginId() + ".principal" );
    urlBuffer.append( "jdbc:hive2://" ).append( hostname ).append( ":" ).append( port ).append( "/" )
      .append( databaseName );
    if ( principal == null && extraPrincipal == null ) {
      urlBuffer.append( AUTH_NO_SASL );
    }
    urlBuffer.append( ";impala_db=true" );
    return urlBuffer.toString();
  }

  @Override
  public String[] getUsedLibraries() {

    return new String[] { JAR_FILE };
  }

  @Override
  public int getDefaultDatabasePort() {
    return DEFAULT_PORT;
  }

  @Override public List<String> getNamedClusterList() {
    try {
      return namedClusterService.listNames( metastoreLocator.getMetastore() );
    } catch ( MetaStoreException e ) {
      logChannel.error( e.getMessage(), e );
      return Collections.emptyList();
    }
  }

  @Override
  public void putOptionalOptions( Map<String, String> extraOptions ) {
    if ( getNamedCluster() != null && getNamedCluster().trim().length() > 0 ) {
      extraOptions.put( getPluginId() + ".pentahoNamedCluster", getNamedCluster() );
    }
  }
}
