/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.bigdata.api.hbase.mapping;

import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.w3c.dom.Node;

import java.util.Map;

/**
 * Created by bryan on 1/19/16.
 */
public interface Mapping {

  String addMappedColumn( HBaseValueMetaInterface column, boolean isTupleColumn ) throws Exception;

  String getTableName();

  void setTableName( String tableName );

  String getMappingName();

  void setMappingName( String mappingName );

  String getKeyName();

  void setKeyName( String keyName );

  void setKeyTypeAsString( String type ) throws Exception;

  Mapping.KeyType getKeyType();

  void setKeyType( Mapping.KeyType type );

  boolean isTupleMapping();

  void setTupleMapping( boolean t );

  String getTupleFamilies();

  String[] getTupleFamiliesSplit();

  void setTupleFamilies( String f );

  int numMappedColumns();

  Map<String, HBaseValueMetaInterface> getMappedColumns();

  void setMappedColumns( Map<String, HBaseValueMetaInterface> cols );

  void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException;

  String getXML();

  boolean loadXML( Node stepnode ) throws KettleXMLException;

  boolean readRep( Repository rep, ObjectId id_step ) throws KettleException;

  String getFriendlyName();

  Object decodeKeyValue( byte[] rawval ) throws KettleException;

  enum TupleMapping {
    KEY( "KEY" ),
    FAMILY( "Family" ),
    COLUMN( "Column" ),
    VALUE( "Value" ),
    TIMESTAMP( "Timestamp" );

    private final String m_stringVal;

    TupleMapping( String name ) {
      this.m_stringVal = name;
    }

    public String toString() {
      return this.m_stringVal;
    }
  }

  enum KeyType {
    STRING( "String" ),
    INTEGER( "Integer" ),
    UNSIGNED_INTEGER( "UnsignedInteger" ),
    LONG( "Long" ),
    UNSIGNED_LONG( "UnsignedLong" ),
    DATE( "Date" ),
    UNSIGNED_DATE( "UnsignedDate" ),
    BINARY( "Binary" );

    private final String m_stringVal;

    KeyType( String name ) {
      this.m_stringVal = name;
    }

    public String toString() {
      return this.m_stringVal;
    }
  }
}
