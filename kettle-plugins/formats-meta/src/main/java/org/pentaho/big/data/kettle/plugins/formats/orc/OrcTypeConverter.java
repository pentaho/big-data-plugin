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

package org.pentaho.big.data.kettle.plugins.formats.orc;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

/**
 * Created by rmansoor on 8/8/2018.
 */
public class OrcTypeConverter {

  public static String convertToOrcType( int pdiType ) {
    switch ( pdiType ) {
      case ValueMetaInterface.TYPE_INET:
      case ValueMetaInterface.TYPE_STRING:
        return OrcSpec.DataType.STRING.getName();
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return OrcSpec.DataType.TIMESTAMP.getName();
      case ValueMetaInterface.TYPE_BINARY:
        return OrcSpec.DataType.BINARY.getName();
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return OrcSpec.DataType.DECIMAL.getName();
      case ValueMetaInterface.TYPE_BOOLEAN:
        return OrcSpec.DataType.BOOLEAN.getName();
      case ValueMetaInterface.TYPE_DATE:
        return OrcSpec.DataType.DATE.getName();
      case ValueMetaInterface.TYPE_INTEGER:
        return OrcSpec.DataType.INTEGER.getName();
      case ValueMetaInterface.TYPE_NUMBER:
        return OrcSpec.DataType.DOUBLE.getName();
      default:
        return OrcSpec.DataType.NULL.getName();
    }
  }

  public static String convertToOrcType( String type ) {
    int pdiType = ValueMetaFactory.getIdForValueMeta( type );
    if ( pdiType > 0 ) {
      return convertToOrcType( pdiType );
    } else {
      return type;
    }
  }
}
