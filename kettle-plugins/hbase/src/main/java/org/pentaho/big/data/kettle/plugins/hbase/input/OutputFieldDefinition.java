/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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
