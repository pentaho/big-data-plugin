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
package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatOutputField;
import org.pentaho.big.data.kettle.plugins.formats.parquet.ParquetTypeConverter;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hadoop.shim.api.format.IParquetOutputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

public class ParquetOutputField extends BaseFormatOutputField implements IParquetOutputField {

  @Override
  public ParquetSpec.DataType getParquetType() {
    for ( ParquetSpec.DataType type : ParquetSpec.DataType.values() ) {
      if ( type.getId() == formatType ) {
        return type;
      }
    }
    return null;
  }

  public void setFormatType( ParquetSpec.DataType formatType ) {
    this.formatType = formatType.getId();
  }

  @Injection( name = "FIELD_PARQUET_TYPE", group = "FIELDS" )
  public void setFormatType( String typeName ) {
    try  {
      setFormatType( Integer.parseInt( typeName ) );
    } catch ( NumberFormatException nfe ) {
      for ( ParquetSpec.DataType parquetType : ParquetSpec.DataType.values() ) {
        if ( parquetType.getName().equals( typeName ) ) {
          this.formatType = parquetType.getId();
          break;
        }
      }
    }
  }

  @Injection( name = "FIELD_TYPE", group = "FIELDS" )
  @Deprecated
  public void setPentahoType( String typeName ) {
    for ( int i = 0; i < ValueMetaInterface.typeCodes.length; i++ ) {
      if ( typeName.equals( ValueMetaInterface.typeCodes[ i ] ) ) {
        setFormatType( ParquetTypeConverter.convertToParquetType( i ) );
        break;
      }
    }
  }

  public boolean isDecimalType() {
    return getParquetType().equals( ParquetSpec.DataType.DECIMAL );
  }


  @Override
  public void setPrecision( String precision ) {
    if ( ( precision == null ) || ( precision.trim().length() == 0 ) ) {
      this.precision = isDecimalType() ? ParquetSpec.DEFAULT_DECIMAL_PRECISION : 0;
    } else {
      this.precision = Integer.valueOf( precision );
      if ( ( this.precision <= 0 ) && isDecimalType() ) {
        this.precision = ParquetSpec.DEFAULT_DECIMAL_PRECISION;
      }
    }
  }

  @Override
  public void setScale( String scale ) {
    if ( ( scale == null ) || ( scale.trim().length() == 0 ) ) {
      this.scale = isDecimalType() ? ParquetSpec.DEFAULT_DECIMAL_SCALE : 0;
    } else {
      this.scale = Integer.valueOf( scale );
      if ( ( this.scale < 0 ) ) {
        this.scale = isDecimalType() ? ParquetSpec.DEFAULT_DECIMAL_SCALE : 0;
      }
    }
  }

}
