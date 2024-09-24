/*
 * *****************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *  *******************************************************************************
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License. You may obtain a copy of the
 *  License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 *
 */

package org.pentaho.big.data.kettle.plugins.hbase.meta;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.w3c.dom.Node;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AELHBaseMappingImpl implements Mapping, Serializable {
  private static final long serialVersionUID = 1L;

  private String tableName;
  private String mappingName;
  private String keyName;
  private KeyType keyType;
  private String keyTypeAsString;
  private int numMappedColumns;
  private Map<String, HBaseValueMetaInterface> mappedColumns;

  public AELHBaseMappingImpl() {
  }

  @Override
  public String addMappedColumn( HBaseValueMetaInterface hBaseValueMetaInterface, boolean b ) throws Exception {
    if ( mappedColumns == null ) {
      mappedColumns = new HashMap<>();
    }

    mappedColumns.put( hBaseValueMetaInterface.getAlias(), hBaseValueMetaInterface );
    this.numMappedColumns++;

    return hBaseValueMetaInterface.getAlias();
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  @Override
  public String getMappingName() {
    return mappingName;
  }

  @Override
  public void setMappingName( String mappingName ) {
    this.mappingName = mappingName;
  }

  @Override
  public String getKeyName() {
    return keyName;
  }

  @Override
  public void setKeyName( String keyName ) {
    this.keyName = keyName;
  }

  @Override
  public KeyType getKeyType() {
    return keyType;
  }

  @Override
  public void setKeyType( KeyType keyType ) {
    this.keyType = keyType;
  }

  @Override
  public Map<String, HBaseValueMetaInterface> getMappedColumns() {
    return mappedColumns;
  }

  @Override
  public void setMappedColumns( Map<String, HBaseValueMetaInterface> mappedColumns ) {
    this.mappedColumns = mappedColumns;
  }

  @Override
  public void setKeyTypeAsString( String s ) throws Exception {
    this.keyTypeAsString = s;
  }

  @Override
  public boolean isTupleMapping() {
    return false;
  }

  @Override
  public void setTupleMapping( boolean b ) {

  }

  @Override
  public String getTupleFamilies() {
    return null;
  }

  @Override
  public String[] getTupleFamiliesSplit() {
    return new String[0];
  }

  @Override
  public void setTupleFamilies( String s ) {

  }

  @Override
  public int numMappedColumns() {
    return this.numMappedColumns;
  }

  @Override
  public void saveRep( Repository repository, ObjectId objectId, ObjectId objectId1 ) throws KettleException {
    //noop on AEL
  }

  @Override
  public String getXML() {
    if ( Const.isEmpty( getKeyName() ) ) {
      return ""; // nothing defined
    }

    String retString = "";

    retString += XMLHandler.openTag( "mapping" );
    retString += XMLHandler.addTagValue( "mapping_name", getMappingName() );
    retString += XMLHandler.addTagValue( "table_name", getTableName() );
    retString += XMLHandler.addTagValue( "key", getKeyName() );
    retString += XMLHandler.addTagValue( "key_type", getKeyType().toString() );
    if ( mappedColumns.size() > 0 ) {
      retString += XMLHandler.openTag( "mapped_columns" );

      for ( String alias : mappedColumns.keySet() ) {
        HBaseValueMetaInterface vm = mappedColumns.get( alias );

        retString += XMLHandler.openTag( "mapped_column" );
        retString += XMLHandler.addTagValue( "alias", alias );
        retString += XMLHandler.addTagValue( "column_family", vm.getColumnFamily() );
        retString += XMLHandler.addTagValue( "column_name", vm.getColumnName() );
        retString += XMLHandler.addTagValue( "type", vm.getHBaseTypeDesc() );
        retString += XMLHandler.closeTag( "mapped_column" );
      }

      retString += XMLHandler.closeTag( "mapped_columns" );
    }

    retString += XMLHandler.closeTag( "mapping" );

    return retString;
  }

  @Override
  public boolean loadXML( Node node ) throws KettleXMLException {
    node = XMLHandler.getSubNode( node, "mapping" );

    if ( node == null
        || Const.isEmpty( XMLHandler.getTagValue( node, "key" ) ) ) {
      return false; // no mapping info in XML
    }

    setMappingName( XMLHandler.getTagValue( node, "mapping_name" ) );
    setTableName( XMLHandler.getTagValue( node, "table_name" ) );

    String keyName = XMLHandler.getTagValue( node, "key" );
    if ( keyName.indexOf( ',' ) > 0 ) {
      setTupleMapping( true );
      setKeyName( keyName.substring( 0, keyName.indexOf( ',' ) ) );
      if ( keyName.indexOf( ',' ) != keyName.length() - 1 ) {
        // specific families have been supplied
        String familiesList = keyName.substring( keyName.indexOf( ',' ) + 1,
            keyName.length() );
        if ( !Const.isEmpty( familiesList.trim() ) ) {
          setTupleFamilies( familiesList );
        }
      }
    } else {
      setKeyName( keyName );
    }

    String keyTypeS = XMLHandler.getTagValue( node, "key_type" );
    for ( KeyType k : KeyType.values() ) {
      if ( k.toString().equalsIgnoreCase( keyTypeS ) ) {
        setKeyType( k );
        break;
      }
    }

    Node fields = XMLHandler.getSubNode( node, "mapped_columns" );
    if ( fields != null && XMLHandler.countNodes( fields, "mapped_column" ) > 0 ) {
      int nrfields = XMLHandler.countNodes( fields, "mapped_column" );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( fields, "mapped_column", i );
        String alias = XMLHandler.getTagValue( fieldNode, "alias" );
        String colFam = XMLHandler.getTagValue( fieldNode, "column_family" );
        if ( colFam == null ) {
          colFam = "";
        }
        String colName = XMLHandler.getTagValue( fieldNode, "column_name" );
        if ( colName == null ) {
          colName = "";
        }
        String type = XMLHandler.getTagValue( fieldNode, "type" );

        AELHBaseValueMetaImpl vm = new AELHBaseValueMetaImpl( false, alias, colName, colFam, getMappingName(), getTableName() );
        vm.setHBaseTypeFromString( type );

        try {
          addMappedColumn( vm, isTupleMapping() );
        } catch ( Exception ex ) {
          throw new KettleXMLException( ex );
        }
      }
    }

    return true;
  }

  @Override
  public boolean readRep( Repository repository, ObjectId objectId ) throws KettleException {
    return false;
  }

  @Override
  public String getFriendlyName() {
    return null;
  }

  @Override
  public Object decodeKeyValue( byte[] bytes ) throws KettleException {
    return null;
  }
}

