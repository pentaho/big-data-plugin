/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.HBaseConnectionException;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition.MappingColumn;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

/**
 * @author Tatsiana_Kasiankova
 */
public class MappingUtilsTest {

  private static final String STRING_TYPE = "String";

  private static final String TEST_TABLE_NAME = "TEST_TABLE_NAME";

  private static final String TEST_MAPPING_NAME = "TEST_MAPPING_NAME";

  private static final String ALIAS_STRING = "alias";

  private static final String VALUE_STRING = "value";

  private static final String KEY_STRING = "key";

  private static final int FAMILIY_ARG_INDEX = 0;

  private static final int NAME_ARG_INDEX = 1;

  private static final int ALIAS_ARG_INDEX = 2;

  /**
   *
   */
  private static final String UNABLE_TO_CONNECT_TO_H_BASE = "Unable to connect to HBase";
  private ConfigurationProducer cProducerMock = mock( ConfigurationProducer.class );
  private HBaseConnection hbConnectionMock = mock( HBaseConnection.class );

  @Test
  public void testGetMappingAdmin_NoException() {
    try {
      when( cProducerMock.getHBaseConnection() ).thenReturn( hbConnectionMock );
      MappingAdmin mappingAdmin = MappingUtils.getMappingAdmin( cProducerMock );
      assertNotNull( mappingAdmin );
      assertSame( hbConnectionMock, mappingAdmin.getConnection() );
      verify( hbConnectionMock ).checkHBaseAvailable();
    } catch ( Exception e ) {
      fail( "No exception expected but it occurs!" );
    }
  }

  @Test
  public void testGetMappingAdmin_ClusterInitializationExceptionToHBaseConnectionException() throws Exception {
    ClusterInitializationException clusterInitializationException =
      new ClusterInitializationException( new Exception( "ClusterInitializationException" ) );
    try {
      when( cProducerMock.getHBaseConnection() ).thenThrow( clusterInitializationException );
      MappingUtils.getMappingAdmin( cProducerMock );
      fail( "Expected HBaseConnectionException but it doen not occur!" );
    } catch ( HBaseConnectionException e ) {
      assertEquals( UNABLE_TO_CONNECT_TO_H_BASE, e.getMessage() );
      assertSame( clusterInitializationException, e.getCause() );
    }
  }

  @Test
  public void testGetMappingAdmin_IOExceptionToHBaseConnectionException() throws Exception {
    IOException ioException = new IOException( "IOException" );
    try {
      when( cProducerMock.getHBaseConnection() ).thenThrow( ioException );
      MappingUtils.getMappingAdmin( cProducerMock );
      fail( "Expected HBaseConnectionException but it doen not occur!" );
    } catch ( HBaseConnectionException e ) {
      assertEquals( UNABLE_TO_CONNECT_TO_H_BASE, e.getMessage() );
      assertSame( ioException, e.getCause() );
    }
  }

  @Test
  public void testIsTupleMappingColumn() {
    for ( Mapping.TupleMapping tupleColumn : Mapping.TupleMapping.values() ) {
      boolean result = MappingUtils.isTupleMappingColumn( tupleColumn.toString() );
      assertTrue( result );
    }
  }

  @Test
  public void testIsTupleMappingColumn_NotTupleColumn() {
    boolean result = MappingUtils.isTupleMappingColumn( "NOT_A_TUPLE_COLUMN" );
    assertFalse( result );
  }

  @Test
  public void testIsTupleMapping() {
    MappingDefinition tupleMappingDefinition = new MappingDefinition();
    tupleMappingDefinition.setMappingColumns( buildTupleMapping() );

    boolean result = MappingUtils.isTupleMapping( tupleMappingDefinition );
    assertTrue( result );
  }

  @Test
  public void testIsTupleMapping_NoTupleMapping() {
    MappingDefinition tupleMappingDefinition = new MappingDefinition();
    tupleMappingDefinition.setMappingColumns( buildNoTupleMapping() );

    boolean result = MappingUtils.isTupleMapping( tupleMappingDefinition );
    assertFalse( result );
  }

  @Test
  public void testGetMappingAdmin() throws IOException {
    HBaseService hBaseService = mock( HBaseService.class );
    HBaseConnection hBaseConnection = mock( HBaseConnection.class );
    when(
      hBaseService.getHBaseConnection( any( VariableSpace.class ), anyString(), anyString(),
        any( LogChannelInterface.class ) ) ).thenReturn( hBaseConnection );
    VariableSpace variableSpace = mock( VariableSpace.class );

    MappingUtils.getMappingAdmin( hBaseService, variableSpace, "SITE_CONFIG", "DEFAULT_CONFIG" );
  }

  @Test
  public void testBuildNonKeyValueMeta() throws KettleException {
    HBaseService hBaseService = mock( HBaseService.class );
    ByteConversionUtil byteConversionUtil = mock( ByteConversionUtil.class );
    when( hBaseService.getByteConversionUtil() ).thenReturn( byteConversionUtil );
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactory.class );
    when( hBaseService.getHBaseValueMetaInterfaceFactory() ).thenReturn( valueMetaInterfaceFactory );
    HBaseValueMetaInterface valueMeta = mock( HBaseValueMetaInterface.class );
    when( valueMeta.isString() ).thenReturn( true );
    when(
      valueMetaInterfaceFactory.createHBaseValueMetaInterface( same( "FAMILY" ), same( "COLUMN_NAME" ),
        same( "ALIAS" ), anyInt(), anyInt(), anyInt() ) ).thenReturn( valueMeta );

    HBaseValueMetaInterface column =
      MappingUtils.buildNonKeyValueMeta( "ALIAS", "FAMILY", "COLUMN_NAME", STRING_TYPE, "INDEXED_VALS", hBaseService );

    assertNotNull( column );
    verify( valueMeta ).setHBaseTypeFromString( STRING_TYPE );
    verify( valueMeta ).setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_UndefinedMappingName() throws Exception {
    HBaseService hBaseService = mock( HBaseService.class );
    MappingDefinition mappingDefinition = buildMappingDefinitionForGetMapping();
    mappingDefinition.setMappingName( "" );
    MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_UndefinedColumns() throws Exception {
    HBaseService hBaseService = mock( HBaseService.class );
    MappingDefinition mappingDefinition = buildMappingDefinitionForGetMapping();
    mappingDefinition.setMappingColumns( Collections.<MappingColumn>emptyList() );
    MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_NoKeyDefined() throws Exception {
    HBaseService hBaseService = mockHBaseService();
    MappingUtils.getMapping( buildMappingDefinitionWithoutKey(), hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_TwoKeysDefined() throws Exception {
    HBaseService hBaseService = mockHBaseService();
    MappingUtils.getMapping( buildMappingDefinitionWithTwoKeys(), hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_keyColumnWithoutAlias() throws Exception {
    HBaseService hBaseService = mockHBaseService();
    MappingDefinition mappingDefinition = createMappingDefinition();
    MappingColumn keyColumn = buildKeyColumn( null, STRING_TYPE );
    mappingDefinition.setMappingColumns( Collections.singletonList( keyColumn ) );

    MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_keyColumnWithoutType() throws Exception {
    HBaseService hBaseService = mockHBaseService();
    MappingDefinition mappingDefinition = createMappingDefinition();
    MappingColumn keyColumn = buildKeyColumn( KEY_STRING, null );
    mappingDefinition.setMappingColumns( Collections.singletonList( keyColumn ) );

    MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_columnWithoutFamilyName() throws Exception {
    HBaseService hBaseService = mockHBaseService();
    MappingDefinition mappingDefinition = createMappingDefinition();
    List<MappingColumn> columns = new ArrayList<MappingColumn>();
    MappingColumn keyColumn = buildKeyColumn( KEY_STRING, STRING_TYPE );
    columns.add( keyColumn );
    MappingColumn otherColumn = buildNoKeyColumn( ALIAS_STRING, null, "columnName", STRING_TYPE );
    columns.add( otherColumn );
    mappingDefinition.setMappingColumns( columns );

    MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  @Test( expected = KettleException.class )
  public void testGetMapping_columnWithoutColumnName() throws Exception {
    HBaseService hBaseService = mockHBaseService();
    MappingDefinition mappingDefinition = createMappingDefinition();
    List<MappingColumn> columns = new ArrayList<MappingColumn>();
    MappingColumn keyColumn = buildKeyColumn( KEY_STRING, STRING_TYPE );
    columns.add( keyColumn );
    MappingColumn otherColumn = buildNoKeyColumn( ALIAS_STRING, "family", null, STRING_TYPE );
    columns.add( otherColumn );
    mappingDefinition.setMappingColumns( columns );

    MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  @Test
  public void testGetMapping() throws Exception {
    HBaseService hBaseService = mock( HBaseService.class );
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactory.class );
    when( hBaseService.getHBaseValueMetaInterfaceFactory() ).thenReturn( valueMetaInterfaceFactory );
    HBaseValueMetaInterface keyValueMeta = mock( HBaseValueMetaInterface.class );
    when( keyValueMeta.isString() ).thenReturn( true );
    when(
      valueMetaInterfaceFactory.createHBaseValueMetaInterface( anyString(), anyString(), same( KEY_STRING ),
        anyInt(), anyInt(), anyInt() ) ).thenReturn( keyValueMeta );

    HBaseValueMetaInterface valueValueMeta = mock( HBaseValueMetaInterface.class );
    when( keyValueMeta.isString() ).thenReturn( true );
    when(
      valueMetaInterfaceFactory.createHBaseValueMetaInterface( anyString(), anyString(), same( VALUE_STRING ),
        anyInt(), anyInt(), anyInt() ) ).thenReturn( valueValueMeta );

    MappingFactory mappingFactory = mock( MappingFactory.class );
    when( hBaseService.getMappingFactory() ).thenReturn( mappingFactory );
    Mapping mapping = mock( Mapping.class );
    when( mappingFactory.createMapping( TEST_TABLE_NAME, TEST_MAPPING_NAME ) ).thenReturn( mapping );

    Mapping result = MappingUtils.getMapping( buildMappingDefinitionForGetMapping(), hBaseService );
    assertNotNull( result );

    verify( mapping ).setKeyName( KEY_STRING );
    verify( mapping ).addMappedColumn( valueValueMeta, false );
  }

  private static HBaseService mockHBaseService() {
    HBaseService hBaseService = mock( HBaseService.class );
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactory.class );
    when( hBaseService.getHBaseValueMetaInterfaceFactory() ).thenReturn( valueMetaInterfaceFactory );
    when(
      valueMetaInterfaceFactory.createHBaseValueMetaInterface( anyString(), anyString(), anyString(), anyInt(),
        anyInt(), anyInt() ) ).thenAnswer( new Answer<HBaseValueMetaInterface>() {

          @Override
          public HBaseValueMetaInterface answer( InvocationOnMock invocation ) throws Throwable {
            Object[] args = invocation.getArguments();
            String columnFamily = (String) args[ FAMILIY_ARG_INDEX ];
            String columnName = (String) args[ NAME_ARG_INDEX ];
            String alias = (String) args[ ALIAS_ARG_INDEX ];
            HBaseValueMetaInterface valueMeta = mock( HBaseValueMetaInterface.class );
            when( valueMeta.getAlias() ).thenReturn( alias );
            when( valueMeta.getColumnFamily() ).thenReturn( columnFamily );
            when( valueMeta.getColumnName() ).thenReturn( columnName );
            return valueMeta;
          }
        } );

    MappingFactory mappingFactory = mock( MappingFactory.class );
    when( hBaseService.getMappingFactory() ).thenReturn( mappingFactory );
    Mapping mapping = mock( Mapping.class );
    when( mappingFactory.createMapping( TEST_TABLE_NAME, TEST_MAPPING_NAME ) ).thenReturn( mapping );
    return hBaseService;
  }

  private static MappingDefinition buildMappingDefinitionWithoutKey() {
    MappingDefinition mappingDefinition = createMappingDefinition();
    MappingColumn valueColumn = new MappingColumn();
    valueColumn.setAlias( VALUE_STRING );
    valueColumn.setType( STRING_TYPE );
    valueColumn.setColumnFamily( "family" );
    valueColumn.setColumnName( "name" );
    mappingDefinition.setMappingColumns( Collections.singletonList( valueColumn ) );
    return mappingDefinition;
  }

  private static MappingDefinition buildMappingDefinitionWithTwoKeys() {
    MappingDefinition mappingDefinition = createMappingDefinition();
    List<MappingColumn> mappingColumns = new ArrayList<MappingColumn>();
    MappingColumn keyColumn = new MappingColumn();
    keyColumn.setAlias( KEY_STRING );
    keyColumn.setKey( true );
    keyColumn.setType( STRING_TYPE );
    mappingColumns.add( keyColumn );

    MappingColumn keyColumn2 = new MappingColumn();
    keyColumn2.setAlias( "key2" );
    keyColumn2.setKey( true );
    keyColumn2.setType( STRING_TYPE );
    mappingColumns.add( keyColumn2 );

    mappingDefinition.setMappingColumns( mappingColumns );
    return mappingDefinition;
  }

  private static MappingDefinition buildMappingDefinitionForGetMapping() {
    MappingDefinition mappingDefinition = createMappingDefinition();
    List<MappingColumn> mappingColumns = new ArrayList<MappingColumn>();
    MappingColumn keyColumn = buildKeyColumn( KEY_STRING, STRING_TYPE );
    mappingColumns.add( keyColumn );

    MappingColumn valueColumn = buildNoKeyColumn( VALUE_STRING, "family", "name", STRING_TYPE );
    mappingColumns.add( valueColumn );
    mappingDefinition.setMappingColumns( mappingColumns );
    return mappingDefinition;
  }

  private static MappingColumn buildKeyColumn( String alias, String type ) {
    MappingColumn keyColumn = new MappingColumn();
    keyColumn.setAlias( alias );
    keyColumn.setKey( true );
    keyColumn.setType( type );
    return keyColumn;
  }

  public static MappingColumn buildNoKeyColumn( String alias, String family, String name, String type ) {
    MappingColumn valueColumn = new MappingColumn();
    valueColumn.setAlias( alias );
    valueColumn.setType( STRING_TYPE );
    valueColumn.setColumnFamily( family );
    valueColumn.setColumnName( name );
    return valueColumn;
  }

  private static MappingDefinition createMappingDefinition() {
    MappingDefinition mappingDefinition = new MappingDefinition();
    mappingDefinition.setTableName( TEST_TABLE_NAME );
    mappingDefinition.setMappingName( TEST_MAPPING_NAME );
    return mappingDefinition;
  }

  private static List<MappingColumn> buildTupleMapping() {
    List<MappingColumn> mappingColumns = new ArrayList<MappingColumn>();
    MappingColumn keyColumn = new MappingColumn();
    keyColumn.setAlias( "KEY" );
    mappingColumns.add( keyColumn );
    MappingColumn familyColumn = new MappingColumn();
    familyColumn.setAlias( "Family" );
    mappingColumns.add( familyColumn );
    MappingColumn columnColumn = new MappingColumn();
    columnColumn.setAlias( "Column" );
    mappingColumns.add( columnColumn );
    MappingColumn valueColumn = new MappingColumn();
    valueColumn.setAlias( "Value" );
    mappingColumns.add( valueColumn );
    MappingColumn timestampColumn = new MappingColumn();
    timestampColumn.setAlias( "Timestamp" );
    mappingColumns.add( timestampColumn );
    return mappingColumns;
  }

  private static List<MappingColumn> buildNoTupleMapping() {
    List<MappingColumn> mappingColumns = new ArrayList<MappingColumn>();
    MappingColumn keyColumn = new MappingColumn();
    keyColumn.setAlias( KEY_STRING );
    mappingColumns.add( keyColumn );
    MappingColumn valueColumn = new MappingColumn();
    valueColumn.setAlias( VALUE_STRING );
    mappingColumns.add( valueColumn );
    return mappingColumns;
  }

}
