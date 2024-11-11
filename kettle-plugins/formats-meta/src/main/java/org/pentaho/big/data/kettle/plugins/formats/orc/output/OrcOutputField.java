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
