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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.shim.api.ColumnFilter;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.Charset;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/3/16.
 */
public class HBaseConnectionWrapperTest {
  private HBaseConnectionTestImpls.HBaseConnectionWithResultField delegate;
  private HBaseConnectionWrapper hBaseConnectionWrapper;
  private byte[] testBytes;
  private byte[] testBytes2;
  private String tableName;
  private List list;
  private Properties properties;
  private NavigableMap navigableMap;

  @Before
  public void setup() {
    delegate = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    hBaseConnectionWrapper = new HBaseConnectionWrapper( delegate );
    testBytes = "testBytes".getBytes( Charset.forName( "UTF-8" ) );
    testBytes2 = "testBytes2".getBytes( Charset.forName( "UTF-8" ) );
    tableName = "tableName";
    list = mock( List.class );
    properties = mock( Properties.class );
    navigableMap = mock( NavigableMap.class );
  }

  @Test
  public void testGetBytesUtil() throws Exception {
    HBaseBytesUtilShim bytesUtilShim = mock( HBaseBytesUtilShim.class );
    when( delegate.getBytesUtil() ).thenReturn( bytesUtilShim );
    assertEquals( bytesUtilShim, hBaseConnectionWrapper.getBytesUtil() );
  }

  @Test
  public void testConfigureConnection() throws Exception {
    hBaseConnectionWrapper.configureConnection( properties, list );
    verify( delegate ).configureConnection( properties, list );
  }

  @Test
  public void testCheckHBaseAvailable() throws Exception {
    hBaseConnectionWrapper.checkHBaseAvailable();
    verify( delegate ).checkHBaseAvailable();
  }

  @Test
  public void testListTableNames() throws Exception {
    when( delegate.listTableNames() ).thenReturn( list );
    assertEquals( list, hBaseConnectionWrapper.listTableNames() );
  }

  @Test
  public void testTableExists() throws Exception {
    when( delegate.tableExists( tableName ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.tableExists( tableName ) );
    assertFalse( hBaseConnectionWrapper.tableExists( tableName ) );
  }

  @Test
  public void testIsTableDisabled() throws Exception {
    when( delegate.isTableDisabled( tableName ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.isTableDisabled( tableName ) );
    assertFalse( hBaseConnectionWrapper.isTableDisabled( tableName ) );
  }

  @Test
  public void testIsTableAvailable() throws Exception {
    when( delegate.isTableAvailable( tableName ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.isTableAvailable( tableName ) );
    assertFalse( hBaseConnectionWrapper.isTableAvailable( tableName ) );
  }

  @Test
  public void testDisableTable() throws Exception {
    hBaseConnectionWrapper.disableTable( tableName );
    verify( delegate ).disableTable( tableName );
  }

  @Test
  public void testEnableTable() throws Exception {
    hBaseConnectionWrapper.enableTable( tableName );
    verify( delegate ).enableTable( tableName );
  }

  @Test
  public void testDeleteTable() throws Exception {
    hBaseConnectionWrapper.deleteTable( tableName );
    verify( delegate ).deleteTable( tableName );
  }

  @Test
  public void testExecuteTargetTableDelete() throws Exception {
    hBaseConnectionWrapper.executeTargetTableDelete( testBytes );
    verify( delegate ).executeTargetTableDelete( testBytes );
  }

  @Test
  public void testCreateTable() throws Exception {
    hBaseConnectionWrapper.createTable( tableName, list, properties );
    verify( delegate ).createTable( tableName, list, properties );
  }

  @Test
  public void testGetTableFamilies() throws Exception {
    when( delegate.getTableFamiles( tableName ) ).thenReturn( list );
    assertEquals( list, hBaseConnectionWrapper.getTableFamiles( tableName ) );
  }

  @Test
  public void testNewSourceTable() throws Exception {
    hBaseConnectionWrapper.newSourceTable( tableName );
    verify( delegate ).newSourceTable( tableName );
  }

  @Test
  public void testSourceTableRowExists() throws Exception {
    when( delegate.sourceTableRowExists( testBytes ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.sourceTableRowExists( testBytes ) );
    assertFalse( hBaseConnectionWrapper.sourceTableRowExists( testBytes ) );
  }

  @Test
  public void testNewSourceTableScan() throws Exception {
    hBaseConnectionWrapper.newSourceTableScan( testBytes, testBytes2, 1 );
    verify( delegate ).newSourceTableScan( testBytes, testBytes2, 1 );
  }

  @Test
  public void testNewTargetTablePut() throws Exception {
    hBaseConnectionWrapper.newTargetTablePut( testBytes, true );
    verify( delegate ).newTargetTablePut( testBytes, true );
    hBaseConnectionWrapper.newTargetTablePut( testBytes2, false );
    verify( delegate ).newTargetTablePut( testBytes2, false );
  }

  @Test
  public void testTargetTableIsAutoFlush() throws Exception {
    when( delegate.targetTableIsAutoFlush() ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.targetTableIsAutoFlush() );
    assertFalse( hBaseConnectionWrapper.targetTableIsAutoFlush() );
  }

  @Test
  public void testExecuteTargetTablePut() throws Exception {
    hBaseConnectionWrapper.executeTargetTablePut();
    verify( delegate ).executeTargetTablePut();
  }

  @Test
  public void testFlushCommitsTargetTable() throws Exception {
    hBaseConnectionWrapper.flushCommitsTargetTable();
    verify( delegate ).flushCommitsTargetTable();
  }

  @Test
  public void testAddColumnToTargetPut() throws Exception {
    String string1 = "string1";
    String string2 = "string2";
    hBaseConnectionWrapper.addColumnToTargetPut( string1, string2, true, testBytes );
    verify( delegate ).addColumnToTargetPut( string1, string2, true, testBytes );
    hBaseConnectionWrapper.addColumnToTargetPut( string2, string1, false, testBytes2 );
    verify( delegate ).addColumnToTargetPut( string2, string1, false, testBytes2 );
  }

  @Test
  public void testAddColumnToScan() throws Exception {
    String string1 = "string1";
    String string2 = "string2";
    hBaseConnectionWrapper.addColumnToScan( string1, string2, true );
    verify( delegate ).addColumnToScan( string1, string2, true );
    hBaseConnectionWrapper.addColumnToScan( string1, string2, false );
    verify( delegate ).addColumnToScan( string1, string2, false );
  }

  @Test
  public void testAddColumnFilterToScan() throws Exception {
    ColumnFilter columnFilter = mock( ColumnFilter.class );
    HBaseValueMeta hBaseValueMeta = mock( HBaseValueMeta.class );
    VariableSpace variableSpace = mock( VariableSpace.class );
    hBaseConnectionWrapper.addColumnFilterToScan( columnFilter, hBaseValueMeta, variableSpace, true );
    verify( delegate ).addColumnFilterToScan( columnFilter, hBaseValueMeta, variableSpace, true );
    hBaseConnectionWrapper.addColumnFilterToScan( columnFilter, hBaseValueMeta, variableSpace, false );
    verify( delegate ).addColumnFilterToScan( columnFilter, hBaseValueMeta, variableSpace, false );
  }

  @Test
  public void testExecuteSourceTableScan() throws Exception {
    hBaseConnectionWrapper.executeSourceTableScan();
    verify( delegate ).executeSourceTableScan();
  }

  @Test
  public void testResultSetNextRow() throws Exception {
    when( delegate.resultSetNextRow() ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.resultSetNextRow() );
    assertFalse( hBaseConnectionWrapper.resultSetNextRow() );
  }

  @Test
  public void testGetRowKey() throws Exception {
    Object o = new Object();
    when( delegate.getRowKey( o ) ).thenReturn( testBytes );
    assertEquals( testBytes, hBaseConnectionWrapper.getRowKey( o ) );
  }

  @Test
  public void testGetResultSetCurrentRowKey() throws Exception {
    when( delegate.getResultSetCurrentRowKey() ).thenReturn( testBytes );
    assertEquals( testBytes, hBaseConnectionWrapper.getResultSetCurrentRowKey() );
  }

  @Test
  public void testGetRowColumnLatest() throws Exception {
    Object o = new Object();
    String string1 = "string1";
    String string2 = "string2";
    when( delegate.getRowColumnLatest( o, string1, string2, true ) ).thenReturn( testBytes );
    assertEquals( testBytes, hBaseConnectionWrapper.getRowColumnLatest( o, string1, string2, true ) );
    when( delegate.getRowColumnLatest( o, string1, string2, false ) ).thenReturn( testBytes2 );
    assertEquals( testBytes2, hBaseConnectionWrapper.getRowColumnLatest( o, string1, string2, false ) );
  }

  @Test
  public void testCheckForHBaseRow() {
    Object o = new Object();
    when( delegate.checkForHBaseRow( o ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.checkForHBaseRow( o ) );
    assertFalse( hBaseConnectionWrapper.checkForHBaseRow( o ) );
  }

  @Test
  public void testGetResultSetCurrentRowColumnLatest() throws Exception {
    String string1 = "string1";
    String string2 = "string2";
    when( delegate.getResultSetCurrentRowColumnLatest( string1, string2, true ) ).thenReturn( testBytes );
    assertEquals( testBytes, hBaseConnectionWrapper.getResultSetCurrentRowColumnLatest( string1, string2, true ) );
    when( delegate.getResultSetCurrentRowColumnLatest( string1, string2, false ) ).thenReturn( testBytes2 );
    assertEquals( testBytes2, hBaseConnectionWrapper.getResultSetCurrentRowColumnLatest( string1, string2, false ) );
  }

  @Test
  public void testGetRowFamilyMap() throws Exception {
    Object o = new Object();
    String string = "string";
    when( delegate.getRowFamilyMap( o, string ) ).thenReturn( navigableMap );
    assertEquals( navigableMap, hBaseConnectionWrapper.getRowFamilyMap( o, string ) );
  }

  @Test
  public void testGetResultSetCurrentRowFamilyMap() throws Exception {
    String string = "string";
    when( delegate.getResultSetCurrentRowFamilyMap( string ) ).thenReturn( navigableMap );
    assertEquals( navigableMap, hBaseConnectionWrapper.getResultSetCurrentRowFamilyMap( string ) );
  }

  @Test
  public void testGetRowMap() throws Exception {
    Object o = new Object();
    when( delegate.getRowMap( o ) ).thenReturn( navigableMap );
    assertEquals( navigableMap, hBaseConnectionWrapper.getRowMap( o ) );
  }

  @Test
  public void testGetResultSetCurrentRowMap() throws Exception {
    when( delegate.getResultSetCurrentRowMap() ).thenReturn( navigableMap );
    assertEquals( navigableMap, hBaseConnectionWrapper.getResultSetCurrentRowMap() );
  }

  @Test
  public void testCloseSourceTable() throws Exception {
    hBaseConnectionWrapper.closeSourceTable();
    verify( delegate ).closeSourceTable();
  }

  @Test
  public void testCloseSourceResultSet() throws Exception {
    hBaseConnectionWrapper.closeSourceResultSet();
    verify( delegate ).closeSourceResultSet();
  }

  @Test
  public void testNewTargetTable() throws Exception {
    String string = "string";
    hBaseConnectionWrapper.newTargetTable( string, properties );
    verify( delegate ).newTargetTable( string, properties );
  }

  @Test
  public void testCloseTargetTable() throws Exception {
    hBaseConnectionWrapper.closeTargetTable();
    verify( delegate ).closeTargetTable();
  }

  @Test
  public void testIsImmutableBytesWritable() {
    Object o = new Object();
    when( delegate.isImmutableBytesWritable( o ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseConnectionWrapper.isImmutableBytesWritable( o ) );
    assertFalse( hBaseConnectionWrapper.isImmutableBytesWritable( o ) );
  }

  @Test
  public void testClose() throws Exception {
    hBaseConnectionWrapper.close();
    verify( delegate ).close();
  }

  @Test
  public void testGetCurrentResult() throws IllegalAccessException {
    Field resultSetRowField = hBaseConnectionWrapper.getResultSetRowField();
    Result result = mock( Result.class );
    resultSetRowField.set( hBaseConnectionWrapper.getRealImpl(), result );
    assertEquals( result, hBaseConnectionWrapper.getCurrentResult() );
  }

  @Test
  public void testGetCurrentResultIllegalAccessException() throws IllegalAccessException {
    Field resultSetRowField = HBaseConnectionWrapper.getResultSetRowField( hBaseConnectionWrapper.getRealImpl() );
    Result result = mock( Result.class );
    resultSetRowField.setAccessible( true );
    resultSetRowField.set( hBaseConnectionWrapper.getRealImpl(), result );
    resultSetRowField = hBaseConnectionWrapper.getResultSetRowField();
    try {
      resultSetRowField.setAccessible( false );
      assertNull( hBaseConnectionWrapper.getCurrentResult() );
    } finally {
      resultSetRowField.setAccessible( true );
    }
  }

  @Test
  public void testGetFieldNull() {
    assertNull( HBaseConnectionWrapper.getField( Object.class, "fake" ) );
  }

  @Test
  public void testFindRealImplWrappedProxyAndDelegate() throws IllegalAccessException {
    final HBaseConnectionTestImpls.HBaseConnectionWithResultField delegate =
      mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.Subclass.class );
    Object instance = Proxy.newProxyInstance( getClass().getClassLoader(), new Class[] { HBaseConnection.class },
      new InvocationHandler() {
        private final boolean a = false;
        private final HBaseConnectionWrapper b = new HBaseConnectionWrapper( delegate );
        private final int c = 0;

        @Override public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
          return null;
        }
      } );
    HBaseConnectionTestImpls.HBaseConnectionWithMismatchedDelegate hBaseConnection =
      mock( HBaseConnectionTestImpls.HBaseConnectionWithMismatchedDelegate.class );
    Field delegateField = HBaseConnectionWrapper.getField( hBaseConnection.getClass(), "delegate" );
    delegateField.setAccessible( true );
    delegateField.set( hBaseConnection, instance );
    assertEquals( delegate, HBaseConnectionWrapper.findRealImpl( hBaseConnection ) );
  }

  @Test
  public void testUnwrapProxyFailure() {
    Object instance = Proxy.newProxyInstance( getClass().getClassLoader(), new Class[] { HBaseConnection.class },
      new InvocationHandler() {
        private final boolean a = false;
        private final int c = 0;

        @Override public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
          return null;
        }
      } );
    assertNull( HBaseConnectionWrapper.findRealImpl( instance ) );
  }

  @Test
  public void testFindRealImplNull() throws IllegalAccessException {
    HBaseConnectionTestImpls.HBaseConnectionWithMismatchedDelegate hBaseConnection =
      mock( HBaseConnectionTestImpls.HBaseConnectionWithMismatchedDelegate.class );
    Field delegateField = HBaseConnectionWrapper.getField( hBaseConnection.getClass(), "delegate" );
    delegateField.setAccessible( true );
    delegateField.set( hBaseConnection, new Object() );

    assertNull( HBaseConnectionWrapper.findRealImpl( new Object() ) );
    assertNull( HBaseConnectionWrapper.findRealImpl( mock( org.pentaho.hbase.shim.spi.HBaseConnection.class ) ) );
    assertNull( HBaseConnectionWrapper.findRealImpl( hBaseConnection ) );
  }

  @Test
  public void testGetFieldValuePublic() throws IllegalAccessException {
    HBaseConnectionTestImpls.HBaseConnectionWithPublicDelegate hBaseConnection =
      mock( HBaseConnectionTestImpls.HBaseConnectionWithPublicDelegate.class );
    Field delegateField = HBaseConnectionWrapper.getField( hBaseConnection.getClass(), "delegate" );
    Object value = new Object();
    delegateField.set( hBaseConnection, value );

    assertEquals( value, HBaseConnectionWrapper.getFieldValue( delegateField, hBaseConnection ) );
  }
}
