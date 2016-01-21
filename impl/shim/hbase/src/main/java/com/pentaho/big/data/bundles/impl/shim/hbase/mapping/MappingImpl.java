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

package com.pentaho.big.data.bundles.impl.shim.hbase.mapping;

import com.google.common.collect.Maps;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.w3c.dom.Node;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bryan on 1/21/16.
 */
public class MappingImpl implements Mapping {
  private final org.pentaho.hbase.shim.api.Mapping delegate;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;

  public MappingImpl( org.pentaho.hbase.shim.api.Mapping delegate, HBaseBytesUtilShim hBaseBytesUtilShim,
                      HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory ) {
    this.delegate = delegate;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
  }

  @Override public String addMappedColumn( HBaseValueMetaInterface column, boolean isTupleColumn ) throws Exception {
    return delegate.addMappedColumn( (HBaseValueMeta) column, isTupleColumn );
  }

  @Override public String getTableName() {
    return delegate.getTableName();
  }

  @Override public void setTableName( String tableName ) {
    delegate.setTableName( tableName );
  }

  @Override public String getMappingName() {
    return delegate.getMappingName();
  }

  @Override public void setMappingName( String mappingName ) {
    delegate.setMappingName( mappingName );
  }

  @Override public String getKeyName() {
    return delegate.getKeyName();
  }

  @Override public void setKeyName( String keyName ) {
    delegate.setKeyName( keyName );
  }

  @Override public void setKeyTypeAsString( String type ) throws Exception {
    delegate.setKeyTypeAsString( type );
  }

  @Override public KeyType getKeyType() {
    org.pentaho.hbase.shim.api.Mapping.KeyType keyType = delegate.getKeyType();
    if ( keyType == null ) {
      return null;
    }
    return KeyType.valueOf( keyType.name() );
  }

  @Override public void setKeyType( KeyType type ) {
    if ( type == null ) {
      delegate.setKeyType( null );
    } else {
      delegate.setKeyType( org.pentaho.hbase.shim.api.Mapping.KeyType.valueOf( type.name() ) );
    }
  }

  @Override public boolean isTupleMapping() {
    return delegate.isTupleMapping();
  }

  @Override public void setTupleMapping( boolean t ) {
    delegate.setTupleMapping( t );
  }

  @Override public String getTupleFamilies() {
    return delegate.getTupleFamilies();
  }

  @Override public void setTupleFamilies( String f ) {
    delegate.setTupleFamilies( f );
  }

  @Override public int numMappedColumns() {
    return delegate.getMappedColumns().size();
  }

  @Override public String[] getTupleFamiliesSplit() {
    return getTupleFamilies().split( HBaseValueMeta.SEPARATOR );
  }

  @Override public Map<String, HBaseValueMetaInterface> getMappedColumns() {
    return Collections.unmodifiableMap( Maps.transformEntries( delegate.getMappedColumns(),
      new Maps.EntryTransformer<String, HBaseValueMeta, HBaseValueMetaInterface>() {
        @Override
        public HBaseValueMetaInterface transformEntry( String key, HBaseValueMeta value ) {
          if ( value instanceof HBaseValueMetaInterface ) {
            return (HBaseValueMetaInterface) value;
          }
          return hBaseValueMetaInterfaceFactory.copy( value );
        }
      } ) );
  }

  @Override public void setMappedColumns( Map<String, HBaseValueMetaInterface> cols ) {
    delegate.setMappedColumns( new HashMap<String, HBaseValueMeta>( Maps.transformEntries( cols,
      new Maps.EntryTransformer<String, HBaseValueMetaInterface, HBaseValueMeta>() {
        @Override
        public HBaseValueMeta transformEntry( String key, HBaseValueMetaInterface value ) {
          if ( value instanceof HBaseValueMeta ) {
            return (HBaseValueMeta) value;
          }
          return hBaseValueMetaInterfaceFactory.copy( value );
        }
      } ) ) );
  }

  @Override public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    delegate.saveRep( rep, id_transformation, id_step );
  }

  @Override public String getXML() {
    return delegate.getXML();
  }

  @Override public boolean loadXML( Node stepnode ) throws KettleXMLException {
    return delegate.loadXML( stepnode );
  }

  @Override public boolean readRep( Repository rep, ObjectId id_step ) throws KettleException {
    return delegate.readRep( rep, id_step );
  }

  @Override public String getFriendlyName() {
    return delegate.getMappingName() + HBaseValueMeta.SEPARATOR + delegate.getTableName();
  }

  @Override public Object decodeKeyValue( byte[] rawval ) throws KettleException {
    return HBaseValueMeta.decodeKeyValue( rawval, delegate, hBaseBytesUtilShim );
  }

  @Override public String toString() {
    return delegate.toString();
  }
}
