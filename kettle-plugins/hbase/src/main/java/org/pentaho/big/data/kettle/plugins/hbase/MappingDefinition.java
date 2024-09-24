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

package org.pentaho.big.data.kettle.plugins.hbase;

import java.util.List;

import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;

public class MappingDefinition {

  @Injection( name = "TABLE_NAME", group = "MAPPING" )
  private String tableName;

  @Injection( name = "MAPPING_NAME", group = "MAPPING" )
  private String mappingName;

  @InjectionDeep
  private List<MappingColumn> mappingColumns;

  public String getTableName() {
    return tableName;
  }

  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  public String getMappingName() {
    return mappingName;
  }

  public void setMappingName( String mappingName ) {
    this.mappingName = mappingName;
  }

  public List<MappingColumn> getMappingColumns() {
    return mappingColumns;
  }

  public void setMappingColumns( List<MappingColumn> mappingColumns ) {
    this.mappingColumns = mappingColumns;
  }

  public static class MappingColumn {

    @Injection( name = "MAPPING_ALIAS", group = "MAPPING" )
    private String alias;

    @Injection( name = "MAPPING_KEY", group = "MAPPING" )
    private boolean key;

    @Injection( name = "MAPPING_COLUMN_FAMILY", group = "MAPPING" )
    private String columnFamily;

    @Injection( name = "MAPPING_COLUMN_NAME", group = "MAPPING" )
    private String columnName;

    @Injection( name = "MAPPING_TYPE", group = "MAPPING" )
    private String type;

    @Injection( name = "MAPPING_INDEXED_VALUES", group = "MAPPING" )
    private String indexedValues;

    public String getAlias() {
      return alias;
    }

    public void setAlias( String alias ) {
      this.alias = alias;
    }

    public boolean isKey() {
      return key;
    }

    public void setKey( boolean key ) {
      this.key = key;
    }

    public String getColumnFamily() {
      return columnFamily;
    }

    public void setColumnFamily( String columnFamily ) {
      this.columnFamily = columnFamily;
    }

    public String getColumnName() {
      return columnName;
    }

    public void setColumnName( String columnName ) {
      this.columnName = columnName;
    }

    public String getType() {
      return type;
    }

    public void setType( String type ) {
      this.type = type;
    }

    public String getIndexedValues() {
      return indexedValues;
    }

    public void setIndexedValues( String indexedValues ) {
      this.indexedValues = indexedValues;
    }
  }

}
