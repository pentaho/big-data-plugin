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


package org.pentaho.big.data.kettle.plugins.hbase;

import org.pentaho.di.core.util.StringUtil;

public class HbaseUtil {
  public static final String HBASE_NAMESPACE_DELIMITER = ":";
  public static final String HBASE_DEFAULT_NAMESPACE = "default";

  private HbaseUtil() {
  }

  public static String parseNamespaceFromTableName( String tableName ) {
    return parseNamespaceFromTableName( tableName, HBASE_DEFAULT_NAMESPACE );
  }

  public static String parseNamespaceFromTableName( String tableName, String defaultNamespaceIfNoneSpecified ) {
    String nameSpace = null;
    if ( tableName.contains( HBASE_NAMESPACE_DELIMITER ) ) {
      nameSpace = tableName.substring( 0, tableName.indexOf( HBASE_NAMESPACE_DELIMITER ) ).trim();
    }
    if ( nameSpace == null || nameSpace.isEmpty() ) {
      return defaultNamespaceIfNoneSpecified;
    } else {
      return nameSpace;
    }
  }

  public static String parseQualifierFromTableName( String tableName ) {
    if ( tableName.contains( HBASE_NAMESPACE_DELIMITER ) ) {
      return tableName.substring( tableName.indexOf( HBASE_NAMESPACE_DELIMITER ) + 1 ).trim();
    } else {
      return tableName.trim();
    }
  }

  /**
   * Force the namespace on the qualifier received.  If the qualifier already has a namespace, ignore it.
   *
   * @param namespace
   * @param qualifier
   * @return
   */
  public static String expandTableName( String namespace, String qualifier ) {
    if ( namespace == null || namespace.isEmpty() || qualifier == null ) {
      throw new IllegalArgumentException( "Namespace must have a value, qualifier must not be null" );
    }
    if ( qualifier.indexOf( HBASE_NAMESPACE_DELIMITER ) > -1 ) {
      return namespace + HBASE_NAMESPACE_DELIMITER + qualifier
        .substring( qualifier.indexOf( HBASE_NAMESPACE_DELIMITER ) + 1 );
    }
    return namespace + HBASE_NAMESPACE_DELIMITER + qualifier;
  }

  /**
   * returns a fully qualified table name.  If the incoming name has a namespace it will honor it, otherwise it will
   * return the default namespace.
   *
   * @param qualifier
   * @return namespace:qualifier
   */
  public static String expandTableName( String qualifier ) {
    if ( qualifier == null ) {
      return HBASE_DEFAULT_NAMESPACE + HBASE_NAMESPACE_DELIMITER;
    }
    int pos = qualifier.indexOf( HBASE_NAMESPACE_DELIMITER );
    if ( pos > 0 ) {
      return qualifier;
    }
    if ( pos == 0 ) {
      return HBASE_DEFAULT_NAMESPACE + qualifier;
    }
    return HBASE_DEFAULT_NAMESPACE + HBASE_NAMESPACE_DELIMITER + qualifier;
  }

  public static String expandLegacyTableNameOnLoad( String qualifier ) {
    if ( qualifier == null ) {
      return expandTableName( "" );
    }
    int pos = Math.min( positionOfString( qualifier, StringUtil.UNIX_OPEN ),
      positionOfString( qualifier, StringUtil.WINDOWS_OPEN ) );
    if ( pos == qualifier.length() ) {
      // No variables in qualifier
      return expandTableName( qualifier );
    }

    int delimPos = qualifier.indexOf( HBASE_NAMESPACE_DELIMITER );
    if ( delimPos > -1 && delimPos < pos ) {
      // hard delimeter exists before the variables, so ok to parse
      return expandTableName( qualifier );
    }
    // variable could be the namespace, or not, we can't tell without substitution
    return qualifier;
  }

  private static int positionOfString( String target, String search ) {
    int pos = target.indexOf( search );
    if ( pos == -1 ) {
      return target.length();
    }
    return pos;
  }
}
