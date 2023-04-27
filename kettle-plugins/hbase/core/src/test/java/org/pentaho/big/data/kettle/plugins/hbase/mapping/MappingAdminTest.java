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


package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import com.pentaho.big.data.bundles.impl.shim.hbase.ByteConversionUtilImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.mapping.MappingFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.Result;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseDelete;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTable;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTableWriteOperationManager;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScanner;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScannerBuilder;
import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by Aliaksandr_Zhuk on 2/14/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class MappingAdminTest {

  private TransMeta transMeta;
  private BaseStepMeta stepMeta;
  private StepMeta parentStepMeta;
  private MappingAdmin mappingAdmin;
  @Mock
  private HBaseConnection mockHbaseConnection;
  @Mock
  private HBaseTable mockPopulatedMappingTable;
  @Mock
  HBaseDelete mockHBaseDelete;
  @Mock
  HBasePut mockHBasePut;

  private HBaseBytesUtilShim hBaseBytesUtilShim = new MockHBaseByteConverterUsingJavaByteBuffer();
  private ByteConversionUtil mockByteConversionUtil = new ByteConversionUtilImpl( hBaseBytesUtilShim );

  private final static String MAPPING_TABLE_NAME = "pentaho_mappings";

  @Before
  public void setUp() throws KettleException {
    KettleEnvironment.init();
    transMeta = Mockito.spy( new TransMeta() );
    stepMeta = Mockito.spy( new BaseStepMeta() );
    parentStepMeta = Mockito.spy( new StepMeta() );
    parentStepMeta.setParentTransMeta( transMeta );
    stepMeta.setParentStepMeta( parentStepMeta );

    when( mockHbaseConnection.getByteConversionUtil() ).thenReturn( mockByteConversionUtil );
    mappingAdmin = new MappingAdmin( mockHbaseConnection );
  }

  @Test
  public void testGetTableNameFromVariable_whenVariableValueExists() {

    String expectedTableName = "hbweblogs";

    transMeta.setVariable( "hb_weblogs", "hbweblogs" );

    String tableName = MappingAdmin.getTableNameFromVariable( stepMeta, "${hb_weblogs}" );

    assertEquals( expectedTableName, tableName );
  }

  @Test
  public void testGetTableNameFromVariable_whenNoVariable() {

    String expectedTableName = "hbweblogs";
    String expectedResult = "${hb_weblogs}";

    String tableName = MappingAdmin.getTableNameFromVariable( stepMeta, "${hb_weblogs}" );

    assertNotEquals( expectedTableName, tableName );
    assertEquals( expectedResult, tableName );
  }

  @Test
  public void setAndGetMappingTableName() {
    mappingAdmin.setMappingTableName( "mappingtbl" );
    assertEquals( "mappingtbl", mappingAdmin.getMappingTableName() );
  }

  @Test
  public void createMappingTable() throws Exception {
    HBaseTable mockHbaseMappingTable = mock( HBaseTable.class );
    when( mockHbaseConnection.getTable( "ns:" + MAPPING_TABLE_NAME ) ).thenReturn( mockHbaseMappingTable );
    when( mockHbaseMappingTable.exists() ).thenReturn( false );

    mappingAdmin.createMappingTable( "ns:tablename" );
    verify( mockHbaseMappingTable, times( 1 ) ).create( any(), any() );
  }

  @Test( expected = IOException.class )
  public void createMappingTableWhenExists() throws Exception {
    HBaseTable mockHbaseMappingTable = mock( HBaseTable.class );
    when( mockHbaseConnection.getTable( "ns:" + MAPPING_TABLE_NAME ) ).thenReturn( mockHbaseMappingTable );
    when( mockHbaseMappingTable.exists() ).thenReturn( true );

    mappingAdmin.createMappingTable( "ns:tablename" );
  }

  @Test
  public void mappingExists() throws Exception {
    setupMappingStructure();
    assertTrue( mappingAdmin.mappingExists( "populated:table1", "map1" ) );
  }

  @Test
  public void testMappingExistsNegative() throws Exception {
    setupMappingStructure();
    assertFalse( mappingAdmin.mappingExists( "populated:table1", "mapx" ) );
  }

  @Test
  public void getMappedTables() throws Exception {
    setupMappingStructure();

    Set<String> mappedTables = mappingAdmin.getMappedTables( null );
    assertEquals( 2, mappedTables.size() );
    assertTrue( mappedTables.contains( "populated:table1" ) );
    assertTrue( mappedTables.contains( "populated:table2" ) );
  }

  private void setupMappingStructure() throws Exception {
    when( mockHbaseConnection.listNamespaces() ).thenReturn( Arrays.asList( "populated", "unpopulated" ) );
    when( mockHbaseConnection.getTable( "populated:" + MAPPING_TABLE_NAME ) ).thenReturn( mockPopulatedMappingTable );
    when( mockPopulatedMappingTable.exists() ).thenReturn( true );
    when( mockPopulatedMappingTable.keyExists( "table1,map1".getBytes() ) ).thenReturn( true );
    ResultScannerBuilder mockResultScannerBuilder = mock( ResultScannerBuilder.class );
    when( mockPopulatedMappingTable.createScannerBuilder( any(), any() ) ).thenReturn( mockResultScannerBuilder );
    ResultScanner mockResultScanner = mock( ResultScanner.class );
    when( mockResultScannerBuilder.build() ).thenReturn( mockResultScanner );
    Result result1 = mock( Result.class );
    when( result1.getRow() ).thenReturn( "table1,map1".getBytes() );
    Result result2 = mock( Result.class );
    when( result2.getRow() ).thenReturn( "table1,map2".getBytes() );
    Result result3 = mock( Result.class );
    when( result3.getRow() ).thenReturn( "table2,map1".getBytes() );
    when( mockResultScanner.next() ).thenReturn( result1, result2, result3, null );

    HBaseTable mockTwoMappingTable = mock( HBaseTable.class );
    when( mockHbaseConnection.getTable( "unpopulated:" + MAPPING_TABLE_NAME ) ).thenReturn( mockTwoMappingTable );
    when( mockTwoMappingTable.exists() ).thenReturn( false );

    // From here down added for getMapping test
    NavigableMap<byte[], byte[]> keyFamilyMap = new TreeMap<>( new ByteArrayComparator() );
    keyFamilyMap.put( "key".getBytes(), "String".getBytes() );
    when( result1.getFamilyMap( "key" ) ).thenReturn( keyFamilyMap );

    NavigableMap<byte[], byte[]> columnFamilyMap = new TreeMap<>( new ByteArrayComparator() );
    columnFamilyMap.put( "colFamily,colName1,aliascol1".getBytes(), "String".getBytes() );
    columnFamilyMap.put( "colFamily,colName2,aliascol2".getBytes(), "Integer".getBytes() );
    when( result1.getFamilyMap( "columns" ) ).thenReturn( columnFamilyMap );

    HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory =
      new HBaseValueMetaInterfaceFactoryImpl( hBaseBytesUtilShim );
    when( mockHbaseConnection.getHBaseValueMetaInterfaceFactory() ).thenReturn( hBaseValueMetaInterfaceFactory );
    MappingFactory mappingFactory = new MappingFactoryImpl( hBaseBytesUtilShim, hBaseValueMetaInterfaceFactory );
    when( mockHbaseConnection.getMappingFactory() ).thenReturn( mappingFactory );

    // From here down added for deleteMapping test
    HBaseTableWriteOperationManager mockHBaseTableWriteOperationManager = mock( HBaseTableWriteOperationManager.class );
    when( mockPopulatedMappingTable.createWriteOperationManager( null ) )
      .thenReturn( mockHBaseTableWriteOperationManager );
    when( mockHBaseTableWriteOperationManager.createDelete( "table1,map1".getBytes() ) ).thenReturn( mockHBaseDelete );

    // From here down added for putMapping test
    when( mockHBaseTableWriteOperationManager.createPut( "table1,map1".getBytes() ) ).thenReturn( mockHBasePut );
  }

  @Test
  public void getMappingNames() throws Exception {
    setupMappingStructure();
    List<String> mappingNames = mappingAdmin.getMappingNames( "populated:table1" );
    assertEquals( 2, mappingNames.size() );
    assertTrue( mappingNames.contains( "map1" ) );
    assertTrue( mappingNames.contains( "map2" ) );
  }

  @Test
  public void getMapping() throws Exception {
    setupMappingStructure();
    Mapping mapping = mappingAdmin.getMapping( "populated:table1", "map1" );
    assertEquals( "map1", mapping.getMappingName() );
    assertEquals( "populated:table1", mapping.getTableName() );
    assertEquals( "key", mapping.getKeyName() );
    assertEquals( Mapping.KeyType.STRING, mapping.getKeyType() );
    Map<String, HBaseValueMetaInterface> mappedColumns = mapping.getMappedColumns();
    assertTrue( mappedColumns.containsKey( "aliascol1" ) );
    assertTrue( mappedColumns.containsKey( "aliascol2" ) );
    assertEquals( "map1", mappedColumns.get( "aliascol1" ).getMappingName() );
    assertEquals( "colFamily", mappedColumns.get( "aliascol1" ).getColumnFamily() );
    assertEquals( "colName1", mappedColumns.get( "aliascol1" ).getColumnName() );
  }

  @Test
  public void deleteMapping() throws Exception {
    setupMappingStructure();


    Mapping mapping = mappingAdmin.getMapping( "populated:table1", "map1" );
    assertNotNull( mapping );
    mappingAdmin.deleteMapping( mapping );
    verify( mockHBaseDelete ).execute();
  }

  @Test
  public void putMapping() throws Exception {
    setupMappingStructure();
    Mapping mapping = mappingAdmin.getMapping( "populated:table1", "map1" );
    assertNotNull( mapping );

    mappingAdmin.putMapping( mapping, true );
    verify( mockHBasePut, times( 1 ) ).createColumnName( "colFamily", "colName1", "aliascol1" );
    verify( mockHBasePut, times( 1 ) ).createColumnName( "colFamily", "colName2", "aliascol2" );
    verify( mockHBasePut, times( 1 ) ).createColumnName( "key" );
    verify( mockHBasePut, times( 1 ) ).execute();
  }

  @Test
  public void describeMapping() throws Exception {
    setupMappingStructure();
    Mapping mapping = mappingAdmin.getMapping( "populated:table1", "map1" );
    assertNotNull( mapping );

    String desc = mappingAdmin.describeMapping( mapping );
    assertNotNull( desc );
    assertTrue( !desc.isEmpty() );
  }

  @Test
  public void close() throws Exception {
    mappingAdmin.close();
    verify( mockHbaseConnection ).close();
  }

  @Test
  public void getConnection() {
    assertEquals( mockHbaseConnection, mappingAdmin.getConnection() );
  }

  static class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare( byte[] a, byte[] b ) {
      if ( a == b ) {
        return 0;
      }
      if ( a == null || b == null ) {
        throw new NullPointerException();
      }

      int length = a.length;
      int cmp;
      if ( ( cmp = Integer.compare( length, b.length ) ) != 0 ) {
        return cmp;
      }

      for ( int i = 0; i < length; i++ ) {
        if ( ( cmp = Byte.compare( a[ i ], b[ i ] ) ) != 0 ) {
          return cmp;
        }
      }

      return 0;
    }
  }
}
