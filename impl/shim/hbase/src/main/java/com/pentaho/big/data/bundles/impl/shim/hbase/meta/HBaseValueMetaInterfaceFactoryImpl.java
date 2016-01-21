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

package com.pentaho.big.data.bundles.impl.shim.hbase.meta;

import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 1/22/16.
 */
public class HBaseValueMetaInterfaceFactoryImpl implements HBaseValueMetaInterfaceFactory {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public HBaseValueMetaInterfaceFactoryImpl( HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override
  public HBaseValueMetaInterfaceImpl createHBaseValueMetaInterface( String family, String column, String alias,
                                                                    int type,
                                                                    int length, int precision )
    throws IllegalArgumentException {
    return createHBaseValueMetaInterface( family + HBaseValueMeta.SEPARATOR + column + HBaseValueMeta.SEPARATOR + alias,
      type, length, precision );
  }

  @Override
  public HBaseValueMetaInterfaceImpl createHBaseValueMetaInterface( String name, int type, int length, int precision )
    throws IllegalArgumentException {
    return new HBaseValueMetaInterfaceImpl( name, type, length, precision, hBaseBytesUtilShim );
  }

  @Override public List<HBaseValueMetaInterface> createListFromRepository( Repository rep, ObjectId id_step )
    throws KettleException {
    int nrfields = rep.countNrStepAttributes( id_step, "table_name" );

    List<HBaseValueMetaInterface> m_outputFields = new ArrayList<>( nrfields );

    if ( nrfields > 0 ) {
      for ( int i = 0; i < nrfields; i++ ) {
        m_outputFields.add( createFromRepository( rep, id_step, i ) );
      }
    }
    return m_outputFields;
  }

  @Override public HBaseValueMetaInterfaceImpl createFromRepository( Repository rep, ObjectId id_step, int i )
    throws KettleException {
    String colFamily = rep.getStepAttributeString( id_step, i, "family" );
    if ( !Const.isEmpty( colFamily ) ) {
      colFamily = colFamily.trim();
    }
    String colName = rep.getStepAttributeString( id_step, i, "column" );
    if ( !Const.isEmpty( colName ) ) {
      colName = colName.trim();
    }
    String alias = rep.getStepAttributeString( id_step, i, "alias" );
    if ( !Const.isEmpty( alias ) ) {
      alias = alias.trim();
    }
    String typeS = rep.getStepAttributeString( id_step, i, "type" );
    if ( !Const.isEmpty( typeS ) ) {
      typeS = typeS.trim();
    }
    boolean isKey = rep.getStepAttributeBoolean( id_step, i, "key" );
    HBaseValueMetaInterfaceImpl vm =
      createHBaseValueMetaInterface( colFamily, colName, alias, ValueMeta.getType( typeS ), -1, -1 );
    vm.setTableName( rep.getStepAttributeString( id_step, i, "table_name" ) );
    vm.setMappingName( rep.getStepAttributeString( id_step, i, "mapping_name" ) );
    vm.setKey( isKey );

    String format = rep.getStepAttributeString( id_step, i, "format" );
    if ( !Const.isEmpty( format ) ) {
      vm.setConversionMask( format );
    }

    String indexValues = rep.getStepAttributeString( id_step, i, "index_values" );
    if ( !Const.isEmpty( indexValues ) ) {
      String[] labels = indexValues.replace( "{", "" ).replace( "}", "" ).split( "," );
      if ( labels.length < 1 ) {
        throw new KettleXMLException( "Indexed/nominal type must have at least one " + "label declared" );
      }
      for ( int j = 0; j < labels.length; j++ ) {
        labels[ j ] = labels[ j ].trim();
      }
      vm.setIndex( labels );
      vm.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    }
    return vm;
  }


  @Override public List<HBaseValueMetaInterface> createListFromNode( Node stepnode ) throws KettleXMLException {
    Node fields = XMLHandler.getSubNode( stepnode, "output_fields" );

    int nrfields = XMLHandler.countNodes( fields, "field" );
    List<HBaseValueMetaInterface> m_outputFields = new ArrayList<>( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      m_outputFields.add( createFromNode( XMLHandler.getSubNodeByNr( fields, "field", i ) ) );
    }
    return m_outputFields;
  }

  @Override public HBaseValueMetaInterfaceImpl createFromNode( Node fieldNode ) throws KettleXMLException {
    String isKey = XMLHandler.getTagValue( fieldNode, "key" ).trim();
    String alias = XMLHandler.getTagValue( fieldNode, "alias" ).trim();
    String colFamily = "";
    String colName = alias;
    if ( !isKey.equalsIgnoreCase( "Y" ) ) {
      if ( XMLHandler.getTagValue( fieldNode, "family" ) != null ) {
        colFamily = XMLHandler.getTagValue( fieldNode, "family" ).trim();
      }

      if ( XMLHandler.getTagValue( fieldNode, "column" ) != null ) {
        colName = XMLHandler.getTagValue( fieldNode, "column" ).trim();
      }
    }

    String typeS = XMLHandler.getTagValue( fieldNode, "type" ).trim();
    HBaseValueMetaInterfaceImpl vm =
      createHBaseValueMetaInterface( colFamily, colName, alias, ValueMeta.getType( typeS ), -1, -1 );
    vm.setTableName( XMLHandler.getTagValue( fieldNode, "table_name" ) );
    vm.setMappingName( XMLHandler.getTagValue( fieldNode, "mapping_name" ) );
    vm.setKey( isKey.equalsIgnoreCase( "Y" ) );

    String format = XMLHandler.getTagValue( fieldNode, "format" );
    if ( !Const.isEmpty( format ) ) {
      vm.setConversionMask( format );
    }

    String indexValues = XMLHandler.getTagValue( fieldNode, "index_values" );
    if ( !Const.isEmpty( indexValues ) ) {
      String[] labels = indexValues.replace( "{", "" ).replace( "}", "" ).split( "," );
      if ( labels.length < 1 ) {
        throw new KettleXMLException( "Indexed/nominal type must have at least one " + "label declared" );
      }
      for ( int j = 0; j < labels.length; j++ ) {
        labels[ j ] = labels[ j ].trim();
      }
      vm.setIndex( labels );
      vm.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    }

    return vm;
  }

  @Override public HBaseValueMetaInterfaceImpl copy( HBaseValueMetaInterface hBaseValueMetaInterface ) {
    HBaseValueMetaInterfaceImpl result =
      createHBaseValueMetaInterface( hBaseValueMetaInterface.getColumnFamily(), hBaseValueMetaInterface.getColumnName(),
        hBaseValueMetaInterface.getName(), hBaseValueMetaInterface.getType(), hBaseValueMetaInterface.getLength(),
        hBaseValueMetaInterface.getPrecision() );
    result.setTableName( hBaseValueMetaInterface.getTableName() );
    result.setMappingName( hBaseValueMetaInterface.getMappingName() );
    result.setKey( hBaseValueMetaInterface.isKey() );
    result.setConversionMask( hBaseValueMetaInterface.getConversionMask() );
    result.setIndex( hBaseValueMetaInterface.getIndex() );
    result.setStorageType( hBaseValueMetaInterface.getStorageType() );
    return result;
  }

  public HBaseValueMetaInterfaceImpl copy( HBaseValueMeta hBaseValueMeta ) {
    HBaseValueMetaInterfaceImpl result =
      createHBaseValueMetaInterface( hBaseValueMeta.getColumnFamily(), hBaseValueMeta.getColumnName(),
        hBaseValueMeta.getName(), hBaseValueMeta.getType(), hBaseValueMeta.getLength(), hBaseValueMeta.getPrecision() );
    result.setTableName( hBaseValueMeta.getTableName() );
    result.setMappingName( hBaseValueMeta.getMappingName() );
    result.setKey( hBaseValueMeta.isKey() );
    result.setConversionMask( hBaseValueMeta.getConversionMask() );
    result.setIndex( hBaseValueMeta.getIndex() );
    result.setStorageType( hBaseValueMeta.getStorageType() );
    return result;
  }
}
