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
