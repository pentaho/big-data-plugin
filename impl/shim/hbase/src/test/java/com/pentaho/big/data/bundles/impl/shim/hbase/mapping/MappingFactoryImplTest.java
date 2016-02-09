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
import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 2/9/16.
 */
public class MappingFactoryImplTest {
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private HBaseValueMetaInterfaceFactoryImpl
    hBaseValueMetaInterfaceFactory;
  private MappingFactoryImpl mappingFactory;

  @Before
  public void setup() {
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    hBaseValueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactoryImpl.class );
    mappingFactory = new MappingFactoryImpl( hBaseBytesUtilShim, hBaseValueMetaInterfaceFactory );
  }

  @Test
  public void testCreateMapping() {
    assertNotNull( mappingFactory.createMapping() );
  }

  @Test
  public void testCreateMappingTableNameMappingName() {
    String table = "table";
    String mappingName = "mapping";

    Mapping mapping = mappingFactory.createMapping( table, mappingName );

    assertEquals( table, mapping.getTableName() );
    assertEquals( mappingName, mapping.getMappingName() );
  }

  @Test
  public void testCreateMappingTableNameMappingNameKeyNameKeyType() {
    String table = "table";
    String mappingName = "mapping";
    String keyName = "keyName";

    for ( Mapping.KeyType type : Mapping.KeyType.values() ) {
      Mapping mapping = mappingFactory.createMapping( table, mappingName, keyName, type );

      assertEquals( table, mapping.getTableName() );
      assertEquals( mappingName, mapping.getMappingName() );
      assertEquals( keyName, mapping.getKeyName() );
      assertEquals( type, mapping.getKeyType() );
    }
    Mapping mapping = mappingFactory.createMapping( table, mappingName, keyName, null );

    assertEquals( table, mapping.getTableName() );
    assertEquals( mappingName, mapping.getMappingName() );
    assertEquals( keyName, mapping.getKeyName() );
    assertNull( mapping.getKeyType() );
  }
}
