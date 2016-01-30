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

import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.mapping.MappingFactory;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

/**
 * Created by bryan on 1/21/16.
 */
public class MappingFactoryImpl implements MappingFactory {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;

  public MappingFactoryImpl( HBaseBytesUtilShim hBaseBytesUtilShim,
                             HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
  }

  @Override public Mapping createMapping() {
    return new MappingImpl( new org.pentaho.hbase.shim.api.Mapping(), hBaseBytesUtilShim,
      hBaseValueMetaInterfaceFactory );
  }

  @Override public Mapping createMapping( String tableName, String mappingName ) {
    return new MappingImpl( new org.pentaho.hbase.shim.api.Mapping( tableName, mappingName ), hBaseBytesUtilShim,
      hBaseValueMetaInterfaceFactory );
  }

  @Override
  public Mapping createMapping( String tableName, String mappingName, String keyName, Mapping.KeyType keyType ) {
    org.pentaho.hbase.shim.api.Mapping.KeyType type = null;
    if ( keyType != null ) {
      type = org.pentaho.hbase.shim.api.Mapping.KeyType.valueOf( keyType.name() );
    }
    return new MappingImpl( new org.pentaho.hbase.shim.api.Mapping( tableName, mappingName, keyName, type ), hBaseBytesUtilShim,
      hBaseValueMetaInterfaceFactory );
  }
}
