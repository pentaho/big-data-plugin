/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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
