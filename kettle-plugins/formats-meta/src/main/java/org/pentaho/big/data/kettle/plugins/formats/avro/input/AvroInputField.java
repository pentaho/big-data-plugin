/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.hadoop.shim.api.format.AvroSpec;
import org.pentaho.hadoop.shim.api.format.IAvroInputField;

/**
 * Base class for format's input/output field - path added.
 * 
 * @author JRice <joseph.rice@hitachivantara.com>
 */
public class AvroInputField implements IAvroInputField {
  @Injection( name = "FIELD_PATH", group = "FIELDS" )
  protected String avroFieldName;

  @Injection( name = "FIELD_NAME", group = "FIELDS" )
  private String pentahoFieldName;

  private int pentahoType;

  private AvroSpec.DataType avroType = null;

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
  public int getPentahoType() {
    return pentahoType;
  }

  @Override
  public void setPentahoType( int pentahoType ) {
    this.pentahoType = pentahoType;
  }

  @Override
  public AvroSpec.DataType getAvroType() {
    return avroType;
  }

  @Override
  public void setAvroType( AvroSpec.DataType avroType ) {

  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( pentahoType );
  }

  @Injection( name = "FIELD_TYPE", group = "FIELDS" )
  public void setType( String value ) {
    this.pentahoType = ValueMetaFactory.getIdForValueMeta( value );
  }

}
