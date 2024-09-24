/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018-2021 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.orc.output;

import org.pentaho.big.data.kettle.plugins.formats.BaseFormatOutputField;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.hadoop.shim.api.format.IOrcOutputField;
import org.pentaho.hadoop.shim.api.format.OrcSpec;

public class OrcOutputField extends BaseFormatOutputField implements IOrcOutputField {
  public OrcSpec.DataType getOrcType() {
    return OrcSpec.DataType.values()[ formatType ];
  }

  @Override
  public void setFormatType( OrcSpec.DataType orcType ) {
    this.formatType = orcType.getId();
  }

  @Override
  public void setFormatType( int formatType ) {
    for ( OrcSpec.DataType orcType : OrcSpec.DataType.values() ) {
      if ( orcType.getId() == formatType ) {
        this.formatType = formatType;
      }
    }
  }

  @Injection( name = "FIELD_TYPE", group = "FIELDS" )
  public void setFormatType( String typeName ) {
    try {
      setFormatType( Integer.parseInt( typeName ) );
    } catch ( NumberFormatException nfe ) {
      for ( OrcSpec.DataType orcType : OrcSpec.DataType.values() ) {
        //Match on Name( for dialog ) or Enum Name ( For metadata injection ), note that the former uses "Int" and
        // the later uses "INTEGER"
        if ( orcType.getName().equalsIgnoreCase( typeName ) || orcType.toString().equalsIgnoreCase( typeName ) ) {
          this.formatType = orcType.getId();
          return;
        }
      }
    }
  }

  public boolean isDecimalType() {
    return getOrcType().equals( OrcSpec.DataType.DECIMAL );
  }

  @Override
  public void setPrecision( String precision ) {
    if ( ( precision == null ) || ( precision.trim().length() == 0 ) ) {
      this.precision = isDecimalType() ? OrcSpec.DEFAULT_DECIMAL_PRECISION : 0;
    } else {
      this.precision = Integer.valueOf( precision );
      if ( ( this.precision <= 0 ) && isDecimalType() ) {
        this.precision = OrcSpec.DEFAULT_DECIMAL_PRECISION;
      }
    }
  }

  @Override
  public void setScale( String scale ) {
    if ( ( scale == null ) || ( scale.trim().length() == 0 ) ) {
      this.scale = isDecimalType() ? OrcSpec.DEFAULT_DECIMAL_SCALE : 0;
    } else {
      this.scale = Integer.valueOf( scale );
      if ( ( this.scale < 0 ) ) {
        this.scale = isDecimalType() ? OrcSpec.DEFAULT_DECIMAL_SCALE : 0;
      }
    }
  }
}
