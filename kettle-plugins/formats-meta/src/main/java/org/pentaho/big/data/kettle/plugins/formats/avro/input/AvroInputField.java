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

package org.pentaho.big.data.kettle.plugins.formats.avro.input;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatInputField;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

/**
 * Base class for format's input/output field - path added.
 *
 * @author JRice <joseph.rice@hitachivantara.com>
 */
public class AvroInputField extends BaseFormatInputField implements IAvroInputField {

  @Override
  public String getAvroFieldName() {
    return formatFieldName;
  }

  @Override public void setAvroFieldName( String avroFieldName ) {
    this.formatFieldName = avroFieldName;
  }

  @Override
  public AvroSpec.DataType getAvroType() {
    return AvroSpec.DataType.getDataType( getFormatType() );
  }

  @Override
  public void setAvroType( AvroSpec.DataType avroType ) {
    setFormatType( avroType.getId() );
  }

  @Injection( name = "AVRO_TYPE", group = "FIELDS" )
  @Override
  public void setAvroType( String avroType ) {
    for ( AvroSpec.DataType tmpType : AvroSpec.DataType.values() ) {
      if ( tmpType.getName().equalsIgnoreCase( avroType ) ) {
        setFormatType( tmpType.getId() );
        break;
      }
    }
  }

  @Override
  public String getDisplayableAvroFieldName() {
    String displayableAvroFieldName = formatFieldName;
    if ( formatFieldName.contains( FILENAME_DELIMITER ) ) {
      displayableAvroFieldName = formatFieldName.split( FILENAME_DELIMITER )[ 0 ];
    }

    return displayableAvroFieldName;
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( getPentahoType() );
  }
}
