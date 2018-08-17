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
package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatInputField;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.IParquetInputField;
import org.pentaho.hadoop.shim.api.format.ParquetSpec;

public class ParquetInputField extends BaseFormatInputField implements IParquetInputField {
  @Override
  public void setParquetType( ParquetSpec.DataType parquetType ) {
    setFormatType( parquetType.getId() );
  }

  @Injection( name = "PARQUET_TYPE", group = "FIELDS" )
  @Override
  public void setParquetType( String parquetType ) {
    for ( ParquetSpec.DataType tmpType : ParquetSpec.DataType.values() ) {
      if ( tmpType.getName().equalsIgnoreCase( parquetType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  @Override
  public ParquetSpec.DataType getParquetType() {
    return ParquetSpec.DataType.getDataType( getFormatType() );
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }
}
