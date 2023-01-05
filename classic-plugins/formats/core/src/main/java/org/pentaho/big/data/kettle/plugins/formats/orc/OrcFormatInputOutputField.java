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

package org.pentaho.big.data.kettle.plugins.formats.orc;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.row.value.ValueMetaFactory;

/**
 * Base class for format's input/output field - path added.
 * 
 * @author JRice <joseph.rice@hitachivantara.com>
 */
public class OrcFormatInputOutputField {
  @Injection( name = "FIELD_PATH", group = "FIELDS" )
  protected String path;

  @Injection( name = "FIELD_NAME", group = "FIELDS" )
  private String name;

  @Injection( name = "FIELD_NULL_STRING", group = "FIELDS" )
  private String nullString;

  @Injection( name = "FIELD_IF_NULL", group = "FIELDS" )
  private String ifNullValue;

  private int type;

  public String getPath() {
    return path;
  }

  public void setPath( String path ) {
    this.path = path;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getNullString() {
    return nullString;
  }

  public void setNullString( String nullString ) {
    this.nullString = nullString;
  }

  public String getIfNullValue() {
    return ifNullValue;
  }

  public void setIfNullValue( String ifNullValue ) {
    this.ifNullValue = ifNullValue;
  }

  public int getType() {
    return type;
  }

  public void setType( int type ) {
    this.type = type;
  }

  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( type );
  }

  @Injection( name = "FIELD_TYPE", group = "FIELDS" )
  public void setType( String value ) {
    this.type = ValueMetaFactory.getIdForValueMeta( value );
  }

}
