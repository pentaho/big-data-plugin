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

package org.pentaho.big.data.kettle.plugins.formats.avro.output;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroOutputField;

/**
 * Base class for format's input/output field - path added.
 * 
 * @author JRice <joseph.rice@hitachivantara.com>
 */
public class AvroOutputField implements IAvroOutputField {
  @Injection( name = "FIELD_PATH", group = "FIELDS" )
  protected String avroFieldName;

  @Injection( name = "FIELD_NAME", group = "FIELDS" )
  private String pentahoFieldName;

  @Injection( name = "FIELD_NULL_STRING", group = "FIELDS" )
  private boolean allowNull;

  @Injection( name = "FIELD_IF_NULL", group = "FIELDS" )
  private String defaultValue;

  private AvroSpec.DataType avroType;

  private int precision;

  private int scale;

  @Override
  public String getAvroFieldName() {
    return avroFieldName;
  }

  @Override
  public void setAvroFieldName( String avroFieldName ) {
    this.avroFieldName = avroFieldName;
  }

  @Override
  public String getPentahoFieldName() {
    return pentahoFieldName;
  }

  @Override
  public void setPentahoFieldName( String pentahoFieldName ) {
    this.pentahoFieldName = pentahoFieldName;
  }

  @Override
  public boolean getAllowNull() {
    return allowNull;
  }

  @Override
  public void setAllowNull( boolean allowNull ) {
    this.allowNull = allowNull;
  }

  @Override
  public String getDefaultValue() {
    return defaultValue;
  }

  @Override
  public void setDefaultValue( String defaultValue ) {
    this.defaultValue = defaultValue;
  }

  @Override
  public AvroSpec.DataType getAvroType() {
    return avroType;
  }

  @Override
  public void setAvroType( AvroSpec.DataType avroType ) {
    this.avroType = avroType;
  }

  @Injection( name = "FIELD_NULL_STRING", group = "FIELDS" )
  public void setAllowNull( String allowNull ) {
    if ( allowNull != null && allowNull.length() > 0 ) {
      if ( allowNull.equalsIgnoreCase( "yes" ) || allowNull.equalsIgnoreCase( "y" ) ) {
        this.allowNull = true;
      } else if ( allowNull.equalsIgnoreCase( "no" ) || allowNull.equalsIgnoreCase( "n" ) ) {
        this.allowNull = false;
      } else {
        this.allowNull = Boolean.parseBoolean( allowNull );
      }
    }
  }

  @Injection( name = "FIELD_TYPE", group = "FIELDS" )
  public void setAvroType( String typeName ) {
    try  {
      setAvroType( Integer.parseInt( typeName ) );
    } catch ( NumberFormatException nfe ) {
      for ( AvroSpec.DataType avroType : AvroSpec.DataType.values() ) {
        if ( avroType.getName().equals( typeName ) ) {
          this.avroType = avroType;
        }
      }
    }
  }

  public void setAvroType( int typeOrdinal ) {
    for ( AvroSpec.DataType avroType : AvroSpec.DataType.values() ) {
      if ( avroType.ordinal() == typeOrdinal ) {
        this.avroType = avroType;
      }
    }
  }

  public boolean isDecimalType() {
    return avroType.getName().equals( AvroSpec.DataType.DECIMAL.getName() );
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision( String precision ) {
    if ( precision == null ) {
      this.precision = AvroSpec.DEFAULT_DECIMAL_PRECISION;
    } else {
      this.precision = Integer.valueOf( precision );
      if ( this.precision <= 0 ) {
        this.precision = AvroSpec.DEFAULT_DECIMAL_PRECISION;
      }
    }
  }

  public int getScale() {
    return scale;
  }

  public void setScale( String scale ) {
    if ( scale == null ) {
      this.scale = AvroSpec.DEFAULT_DECIMAL_SCALE;
    } else {
      this.scale = Integer.valueOf( scale );
      if ( this.scale < 0 ) {
        this.scale = AvroSpec.DEFAULT_DECIMAL_SCALE;
      }
    }
  }
}
