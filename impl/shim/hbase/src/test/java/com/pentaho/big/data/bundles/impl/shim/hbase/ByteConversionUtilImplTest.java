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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 2/3/16.
 */
public class ByteConversionUtilImplTest {
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private ByteConversionUtilImpl byteConversionUtil;
  private byte[] testBytes;

  @Before
  public void setup() {
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    byteConversionUtil = new ByteConversionUtilImpl( hBaseBytesUtilShim );
    testBytes = "testBytes".getBytes( Charset.forName( "UTF-8" ) );
  }

  @Test
  public void testGetSizeOfFloat() {
    when( hBaseBytesUtilShim.getSizeOfFloat() ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.getSizeOfFloat() );
  }

  @Test
  public void testGetSizeOfDouble() {
    when( hBaseBytesUtilShim.getSizeOfDouble() ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.getSizeOfDouble() );
  }

  @Test
  public void testGetSizeOfInt() {
    when( hBaseBytesUtilShim.getSizeOfInt() ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.getSizeOfInt() );
  }

  @Test
  public void testGetSizeOfLong() {
    when( hBaseBytesUtilShim.getSizeOfLong() ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.getSizeOfLong() );
  }

  @Test
  public void testGetSizeOfShort() {
    when( hBaseBytesUtilShim.getSizeOfShort() ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.getSizeOfShort() );
  }

  @Test
  public void testGetSizeOfByte() {
    when( hBaseBytesUtilShim.getSizeOfByte() ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.getSizeOfByte() );
  }

  @Test
  public void testToBytesString() {
    String string = "string";
    when( hBaseBytesUtilShim.toBytes( string ) ).thenReturn( testBytes );
    assertArrayEquals( testBytes, byteConversionUtil.toBytes( string ) );
  }

  @Test
  public void testToBytesInt() {
    when( hBaseBytesUtilShim.toBytes( 42 ) ).thenReturn( testBytes );
    assertArrayEquals( testBytes, byteConversionUtil.toBytes( 42 ) );
  }

  @Test
  public void testToBytesLong() {
    when( hBaseBytesUtilShim.toBytes( 42L ) ).thenReturn( testBytes );
    assertArrayEquals( testBytes, byteConversionUtil.toBytes( 42L ) );
  }

  @Test
  public void testToBytesFloat() {
    when( hBaseBytesUtilShim.toBytes( 42F ) ).thenReturn( testBytes );
    assertArrayEquals( testBytes, byteConversionUtil.toBytes( 42F ) );
  }

  @Test
  public void testToBytesDouble() {
    when( hBaseBytesUtilShim.toBytes( 42D ) ).thenReturn( testBytes );
    assertArrayEquals( testBytes, byteConversionUtil.toBytes( 42D ) );
  }

  @Test
  public void testToBytesBinary() {
    String string = "string";
    when( hBaseBytesUtilShim.toBytesBinary( string ) ).thenReturn( testBytes );
    assertArrayEquals( testBytes, byteConversionUtil.toBytesBinary( string ) );
  }

  @Test
  public void testToString() {
    String string = "string";
    when( hBaseBytesUtilShim.toString( testBytes ) ).thenReturn( string );
    assertEquals( string, byteConversionUtil.toString( testBytes ) );
  }

  @Test
  public void testToLong() {
    when( hBaseBytesUtilShim.toLong( testBytes ) ).thenReturn( 42L );
    assertEquals( 42L, byteConversionUtil.toLong( testBytes ) );
  }

  @Test
  public void testToInt() {
    when( hBaseBytesUtilShim.toInt( testBytes ) ).thenReturn( 42 );
    assertEquals( 42, byteConversionUtil.toInt( testBytes ) );
  }

  @Test
  public void testToFloat() {
    when( hBaseBytesUtilShim.toFloat( testBytes ) ).thenReturn( 42F );
    assertEquals( 42F, byteConversionUtil.toFloat( testBytes ), 0 );
  }

  @Test
  public void testToDouble() {
    when( hBaseBytesUtilShim.toDouble( testBytes ) ).thenReturn( 42D );
    assertEquals( 42D, byteConversionUtil.toDouble( testBytes ), 0 );
  }

  @Test
  public void testToShort() {
    when( hBaseBytesUtilShim.toShort( testBytes ) ).thenReturn( (short) 42 );
    assertEquals( (short) 42, byteConversionUtil.toShort( testBytes ) );
  }

  @Test
  public void testEncodeKeyValue() throws KettleException {
    String testValue = "testValue";
    when( hBaseBytesUtilShim.toBytes( testValue ) ).thenReturn( testBytes );
    assertEquals( testBytes, byteConversionUtil.encodeKeyValue( testValue, Mapping.KeyType.STRING ) );
  }

  @Test
  public void testEncodeObject() throws IOException {
    String testValue = "testValue";
    assertArrayEquals( HBaseValueMeta.encodeObject( testValue ), byteConversionUtil.encodeObject( testValue ) );
  }

  @Test
  public void testCompoundKey() throws IOException {
    String key = "test,key,val";
    when( hBaseBytesUtilShim.toBytes( eq( key ) ) ).thenReturn( testBytes );
    assertEquals( testBytes, byteConversionUtil.compoundKey( key.split( HBaseValueMeta.SEPARATOR ) ) );
  }

  @Test
  public void testCompoundKey0Length() throws IOException {
    when( hBaseBytesUtilShim.toBytes( "" ) ).thenReturn( testBytes );
    assertEquals( testBytes, byteConversionUtil.compoundKey() );
  }

  @Test
  public void testSplitKey() throws IOException {
    String key = "test,key,val";
    when( hBaseBytesUtilShim.toString( testBytes ) ).thenReturn( key );
    assertArrayEquals( key.split( HBaseValueMeta.SEPARATOR ), byteConversionUtil.splitKey( testBytes ) );
  }

  @Test
  public void testObjectIndexValuesToString() {
    Object[] values = new Object[] { 1, "b", false };
    assertEquals( HBaseValueMeta.objectIndexValuesToString( values ),
      byteConversionUtil.objectIndexValuesToString( values ) );
  }

  @Test
  public void testStringIndexListToObjects() {
    String list = "a,1,true";
    assertArrayEquals( HBaseValueMeta.stringIndexListToObjects( list ),
      byteConversionUtil.stringIndexListToObjects( list ) );
  }

  @Test
  public void testEncodeKeyValueVMI() throws KettleException {
    String value = "value";
    when( hBaseBytesUtilShim.toBytes( value ) ).thenReturn( testBytes );
    ValueMetaInterface valueMetaInterface = mock( ValueMetaInterface.class );
    when( valueMetaInterface.getString( value ) ).thenReturn( value );
    assertEquals( testBytes,
      byteConversionUtil.encodeKeyValue( value, valueMetaInterface, Mapping.KeyType.STRING ) );
  }

  @Test
  public void testIsImmutableBytesWritable() {
    assertTrue( byteConversionUtil.isImmutableBytesWritable( mock( ImmutableBytesWritable.class ) ) );
    assertFalse( byteConversionUtil.isImmutableBytesWritable( new Object() ) );
  }
}
