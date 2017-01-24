/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.rowdecoder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

/**
 * @author Tatsiana_Kasiankova
 *
 */
public class HBaseRowDecoderMetaTest {

  private static final String MAPPING_NAME = "MappingName";
  private static final String TABLE_NAME = "TableName";
  private static final String ALIAS = "alias";
  private static final String MAPPING_KEY_NAME = "mappingKeyName";
  private static final String ORIGIN = "HBase Row Decoder";
  private HBaseRowDecoderMeta hbRowDecoderMeta;
  private RowMeta rowMeta;
  private VariableSpace vsMock = mock( VariableSpace.class );
  private NamedClusterServiceLocator ncLocatorMock = mock( NamedClusterServiceLocator.class );
  private NamedClusterService ncsMock = mock( NamedClusterService.class );
  private NamedCluster ncMock = mock( NamedCluster.class );
  private MappingDefinition mapDefMock = mock( MappingDefinition.class );

  @Before
  public void setup() {
    hbRowDecoderMeta = new HBaseRowDecoderMeta( ncLocatorMock, ncsMock, mock( RuntimeTestActionService.class ), mock( RuntimeTester.class ) );
    rowMeta = new RowMeta();
  }

  @After
  public void tearDown() {
    rowMeta.clear();
  }

  @Test
  public void testRowMetaIsFilled_WhenMappingHasTableNameAndMappingName() throws Exception {
    // Mapping from HBase: having both table name and mapping name
    hbRowDecoderMeta.setMapping( getMapping( TABLE_NAME, MAPPING_NAME ) );

    hbRowDecoderMeta.getFields( rowMeta, ORIGIN, null, null, null );

    assertRowMetaIsFilledWithFields();
  }

  @Test
  public void testRowMetaIsFilled_WhenMappingHasNoMappingName() throws Exception {
    // "local" Mapping: no mapping name
    hbRowDecoderMeta.setMapping( getMapping( null, null ) );

    hbRowDecoderMeta.getFields( rowMeta, ORIGIN, null, null, null );

    assertRowMetaIsFilledWithFields();
  }

  private void assertRowMetaIsFilledWithFields() {
    assertEquals( 2, rowMeta.getValueMetaList().size() );
    ValueMetaInterface vmi = rowMeta.getValueMeta( 0 );
    assertEquals( MAPPING_KEY_NAME, vmi.getName() );
    vmi = rowMeta.getValueMeta( 1 );
    assertEquals( ALIAS, vmi.getName() );
  }

  private Mapping getMapping( String tableName, String mappingName ) throws Exception {
    Mapping maping = mock( Mapping.class );
    when( maping.getKeyName() ).thenReturn( MAPPING_KEY_NAME );
    Map<String, HBaseValueMetaInterface> map = new HashMap<>();
    HBaseValueMetaInterface value = mock( HBaseValueMetaInterface.class );
    when( value.getName() ).thenReturn( ALIAS );
    map.put( ALIAS, value );
    when( maping.getMappedColumns() ).thenReturn( map );
    return maping;
  }

  @Test
  public void testNotAppliedInjectionWhenMappingDefinitionNull() throws Exception {
    when( ncsMock.getClusterTemplate() ).thenReturn( ncMock );
    hbRowDecoderMeta.setNamedCluster( ncsMock.getClusterTemplate() );
    hbRowDecoderMeta.setMappingDefinition( null );
    hbRowDecoderMeta.applyInjection( vsMock );
    verify( ncLocatorMock, times( 0 ) ).getService( ncMock, HBaseService.class );
  }
}
