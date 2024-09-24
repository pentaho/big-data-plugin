/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.pentaho.di.core.injection.Injection;

public class OutputFieldDefinition {

  @Injection( name = "OUTPUT_FIELD_ALIAS", group = "OUTPUT_FIELDS" )
  private String alias;

  @Injection( name = "OUTPUT_FIELD_KEY", group = "OUTPUT_FIELDS" )
  private boolean key;

  @Injection( name = "OUTPUT_FIELD_COLUMN_NAME", group = "OUTPUT_FIELDS" )
  private String columnName;

  @Injection( name = "OUTPUT_FIELD_FAMILY", group = "OUTPUT_FIELDS" )
  private String family;

  @Injection( name = "OUTPUT_FIELD_TYPE", group = "OUTPUT_FIELDS" )
  private String hbaseType;

  @Injection( name = "OUTPUT_FIELD_FORMAT", group = "OUTPUT_FIELDS" )
  private String format;

  public boolean isKey() {
    return key;
  }

  public void setKey( boolean key ) {
    this.key = key;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias( String alias ) {
    this.alias = alias;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }

  public String getFamily() {
    return family;
  }

  public void setFamily( String family ) {
    this.family = family;
  }

  public String getHbaseType() {
    return hbaseType;
  }

  public void setHbaseType( String hbaseType ) {
    this.hbaseType = hbaseType;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat( String format ) {
    this.format = format;
  }
}
