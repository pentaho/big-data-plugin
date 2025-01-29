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

package org.pentaho.big.data.kettle.plugins.formats.parquet;

import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

/**
 * Created by rmansoor on 8/8/2018.
 */
public class ParquetTypeConverter {


  public static String convertToParquetType( String pdiType ) {
    int pdiTypeId = -1;
    for ( int i = 0; i < ValueMetaInterface.typeCodes.length; i++ ) {
      if ( ValueMetaInterface.typeCodes[ i ].equals( pdiType ) ) {
        pdiTypeId = i;
        break;
      }
    }
    return convertToParquetType( pdiTypeId );
  }


  public static String convertToParquetType( int pdiType ) {
    switch ( pdiType ) {
      case ValueMetaInterface.TYPE_INET:
      case ValueMetaInterface.TYPE_STRING:
        return ParquetSpec.DataType.UTF8.getName();
      case ValueMetaInterface.TYPE_TIMESTAMP:
        return ParquetSpec.DataType.TIMESTAMP_MILLIS.getName();
      case ValueMetaInterface.TYPE_BINARY:
        return ParquetSpec.DataType.BINARY.getName();
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return ParquetSpec.DataType.DECIMAL.getName();
      case ValueMetaInterface.TYPE_BOOLEAN:
        return ParquetSpec.DataType.BOOLEAN.getName();
      case ValueMetaInterface.TYPE_DATE:
        return ParquetSpec.DataType.DATE.getName();
      case ValueMetaInterface.TYPE_INTEGER:
        return ParquetSpec.DataType.INT_64.getName();
      case ValueMetaInterface.TYPE_NUMBER:
        return ParquetSpec.DataType.DOUBLE.getName();
      default:
        return ParquetSpec.DataType.NULL.getName();
    }
  }

}
