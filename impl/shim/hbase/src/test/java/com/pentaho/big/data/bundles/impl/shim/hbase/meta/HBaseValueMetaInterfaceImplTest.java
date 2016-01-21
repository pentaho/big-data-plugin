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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.api.Mapping;
import org.pentaho.hbase.shim.common.CommonHBaseBytesUtil;
import org.pentaho.hbase.shim.fake.FakeHBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

public class HBaseValueMetaInterfaceImplTest {
  private HBaseBytesUtilShim hBaseBytesUtilShim;

  @Before
  public void setup() {
    this.hBaseBytesUtilShim = new CommonHBaseBytesUtil();
  }

  @Test
  public void testDecodeKeyValueBinary() throws Exception {
    byte[] testVal = hBaseBytesUtilShim.toBytes( "My test value" );
    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.BINARY );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof byte[] );
    assertEquals( ( (byte[]) result ).length, testVal.length );

    FakeHBaseConnection.BytesComparator comp = new FakeHBaseConnection.BytesComparator();
    Assert.assertEquals( comp.compare( testVal, (byte[]) result ), 0 );
  }

  @Test
  public void testDecodeKeyValueString() throws Exception {
    byte[] testVal = hBaseBytesUtilShim.toBytes( "My test value" );
    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.STRING );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof String );
    assertEquals( result.toString(), "My test value" );
  }

  @Test
  public void testDecodeKeyValueUnsignedLong() throws Exception {
    byte[] testVal = hBaseBytesUtilShim.toBytes( 42L );
    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.UNSIGNED_LONG );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof Long );
    assertEquals( ( (Long) result ).longValue(), 42L );
  }

  @Test
  public void testDecodeKeyValueUnsignedDate() throws Exception {
    long currTime = System.currentTimeMillis();
    byte[] testVal = hBaseBytesUtilShim.toBytes( currTime );
    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.UNSIGNED_DATE );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof Date );
    assertEquals( ( (Date) result ).getTime(), currTime );
  }

  @Test
  public void testDecodeKeyValueUnsignedInteger() throws Exception {
    byte[] testVal = hBaseBytesUtilShim.toBytes( 42 );
    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.UNSIGNED_INTEGER );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof Long ); // returns a long as kettle uses longs
                                          // internally
    assertEquals( ( (Long) result ).longValue(), 42L );
  }

  @Test
  public void testDecodeKeyValueSignedInteger() throws Exception {
    // flip the sign bit so keys sort correctly
    int tempInt = -42;
    tempInt ^= ( 1 << 31 );
    byte[] testVal = hBaseBytesUtilShim.toBytes( tempInt );

    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.INTEGER );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof Long ); // returns a long as kettle uses longs
                                          // internally
    assertEquals( ( (Long) result ).longValue(), -42L );
  }

  @Test
  public void testDecodeKeyValueSignedLong() throws Exception {
    // flip the sign bit so keys sort correctly
    long tempVal = -42L;
    tempVal ^= ( 1L << 63 );
    byte[] testVal = hBaseBytesUtilShim.toBytes( tempVal );

    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.LONG );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof Long );
    assertEquals( ( (Long) result ).longValue(), -42L );
  }

  @Test
  public void testDecodeKeyValueSignedDate() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd" );
    Date aDate = sdf.parse( "1969-08-28" );
    long negTime = aDate.getTime();
    long negTimeOrig = negTime;
    negTime ^= ( 1L << 63 );

    byte[] testVal = hBaseBytesUtilShim.toBytes( negTime );
    Mapping dummy = new Mapping( "DummyTable", "DummyMapping", "MyKey", Mapping.KeyType.DATE );
    Object result = HBaseValueMeta.decodeKeyValue( testVal, dummy, hBaseBytesUtilShim );

    assertTrue( result instanceof Date );
    assertEquals( ( (Date) result ).getTime(), negTimeOrig );
  }

  @Test
  public void testEncodeKeyValueBinary() throws Exception {
    byte[] testVal = hBaseBytesUtilShim.toBytes( "Blah" );

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.BINARY, hBaseBytesUtilShim );

    assertTrue( result != null );
    assertEquals( result.length, testVal.length );
    FakeHBaseConnection.BytesComparator comp = new FakeHBaseConnection.BytesComparator();

    // encoding a binary value to binary should just return the same value
    Assert.assertEquals( comp.compare( testVal, result ), 0 );
  }

  @Test
  public void testEncodeKeyValueHexKeyAsBinary() throws Exception {
    String testVal = "\\x00\\x04";

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.BINARY, hBaseBytesUtilShim );

    assertTrue( result != null );
    assertTrue( result.length == 2 );
    assertEquals( result[1], 4 );
  }

  @Test
  public void testEncodeKeyValueString() throws Exception {
    String testVal = "Blah";

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.STRING, hBaseBytesUtilShim );
    assertTrue( result != null );
    Assert.assertEquals( hBaseBytesUtilShim.toString( result ), testVal );
  }

  @Test
  public void testEncodeKeyValueUnsignedLong() throws Exception {
    Long testVal = new Long( 42L );

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.UNSIGNED_LONG, hBaseBytesUtilShim );
    assertTrue( result != null );
    Assert.assertEquals( hBaseBytesUtilShim.toLong( result ), 42L );
  }

  @Test
  public void testEncodeKeyValueUnsignedDate() throws Exception {
    long time = System.currentTimeMillis();
    Date testVal = new Date( time );

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.UNSIGNED_DATE, hBaseBytesUtilShim );
    assertTrue( result != null );
    Assert.assertEquals( hBaseBytesUtilShim.toLong( result ), time );
  }

  @Test
  public void testEncodeKeyValueUnsignedInteger() throws Exception {
    Integer testVal = new Integer( 42 );

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.UNSIGNED_INTEGER, hBaseBytesUtilShim );
    assertTrue( result != null );
    Assert.assertEquals( hBaseBytesUtilShim.toInt( result ), 42 );
  }

  @Test
  public void testEncodeKeyValueSignedInteger() throws Exception {
    Integer testVal = new Integer( -42 );

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.INTEGER, hBaseBytesUtilShim );
    assertTrue( result != null );
    int resultInt = hBaseBytesUtilShim.toInt( result );
    resultInt ^= ( 1 << 31 );
    assertEquals( resultInt, -42 );
  }

  @Test
  public void testEncodeKeyValueSignedLong() throws Exception {
    Long testVal = new Long( -42L );

    byte[] result = HBaseValueMeta.encodeKeyValue( testVal, Mapping.KeyType.LONG, hBaseBytesUtilShim );
    assertTrue( result != null );

    long longResult = hBaseBytesUtilShim.toLong( result );
    longResult ^= ( 1L << 63 );

    assertEquals( longResult, -42L );
  }

  @Test
  public void testEncodeKeyValueSignedDate() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd" );
    Date aDate = sdf.parse( "1969-08-28" );
    long negTime = aDate.getTime();

    byte[] result = HBaseValueMeta.encodeKeyValue( aDate, Mapping.KeyType.DATE, hBaseBytesUtilShim );

    assertTrue( result != null );
    long timeResult = hBaseBytesUtilShim.toLong( result );
    timeResult ^= ( 1L << 63 );

    assertEquals( timeResult, negTime );
  }

  @Test
  public void testEncodeColumnValueString() throws Exception {
    String value = "My test value";
    ValueMetaInterface valMeta = new ValueMeta( "TestString", ValueMetaInterface.TYPE_STRING );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_STRING, -1, -1, hBaseBytesUtilShim );

    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( hBaseBytesUtilShim.toString( encoded ), value );
  }

  @Test
  public void testEncodeColumnValueLong() throws Exception {
    Long value = new Long( 42 );
    ValueMetaInterface valMeta = new ValueMeta( "TestLong", ValueMetaInterface.TYPE_INTEGER );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_INTEGER, -1, -1, hBaseBytesUtilShim );
    mappingMeta.setIsLongOrDouble( true );
    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( encoded.length, hBaseBytesUtilShim.getSizeOfLong() );
    Assert.assertEquals( hBaseBytesUtilShim.toLong( encoded ), value.longValue() );
  }

  @Test
  public void testEncodeColumnValueInteger() throws Exception {
    Long value = new Long( 42 );
    ValueMetaInterface valMeta = new ValueMeta( "TestInt", ValueMetaInterface.TYPE_INTEGER );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_INTEGER, -1, -1, hBaseBytesUtilShim );
    mappingMeta.setIsLongOrDouble( false );
    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( encoded.length, hBaseBytesUtilShim.getSizeOfInt() );
    Assert.assertEquals( hBaseBytesUtilShim.toInt( encoded ), value.intValue() );
  }

  @Test
  public void testEncodeColumnValueDouble() throws Exception {
    Double value = new Double( 42.0 );
    ValueMetaInterface valMeta = new ValueMeta( "TestDouble", ValueMetaInterface.TYPE_NUMBER );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_NUMBER, -1, -1, hBaseBytesUtilShim );
    mappingMeta.setIsLongOrDouble( true );
    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( encoded.length, hBaseBytesUtilShim.getSizeOfDouble() );
    Assert.assertEquals( hBaseBytesUtilShim.toDouble( encoded ), value.doubleValue(), 0.0001 );
  }

  @Test
  public void testEncodeColumnValueFloat() throws Exception {
    Double value = new Double( 42.0 );
    ValueMetaInterface valMeta = new ValueMeta( "TestFloat", ValueMetaInterface.TYPE_NUMBER );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_NUMBER, -1, -1, hBaseBytesUtilShim );
    mappingMeta.setIsLongOrDouble( false );
    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( encoded.length, hBaseBytesUtilShim.getSizeOfFloat() );
    Assert.assertEquals( hBaseBytesUtilShim.toFloat( encoded ), value.floatValue(), 0.0001f );
  }

  @Test
  public void testEncodeColumnValueDate() throws Exception {
    Date value = new Date();
    ValueMetaInterface valMeta = new ValueMeta( "TestDate", ValueMetaInterface.TYPE_DATE );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_DATE, -1, -1, hBaseBytesUtilShim );

    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( encoded.length, hBaseBytesUtilShim.getSizeOfLong() );
    Assert.assertEquals( hBaseBytesUtilShim.toLong( encoded ), value.getTime() );
  }

  @Test
  public void testEncodeColumnValueBoolean() throws Exception {
    Boolean value = new Boolean( true );
    ValueMetaInterface valMeta = new ValueMeta( "TestBool", ValueMetaInterface.TYPE_BOOLEAN );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BOOLEAN, -1, -1, hBaseBytesUtilShim );

    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    assertEquals( encoded.length, 1 );
    Assert.assertEquals( hBaseBytesUtilShim.toString( encoded ), "Y" );
  }

  @Test
  public void testEncodeColumnValueBigNumber() throws Exception {
    BigDecimal value = new BigDecimal( 42 );
    ValueMetaInterface valMeta = new ValueMeta( "TestBigNum", ValueMetaInterface.TYPE_BIGNUMBER );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BIGNUMBER, -1, -1, hBaseBytesUtilShim );

    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    Assert.assertEquals( hBaseBytesUtilShim.toString( encoded ), value.toString() );
  }

  @Test
  public void testEncodeColumnValueSerializable() throws Exception {
    String value = "Hi there bob!";
    ValueMetaInterface valMeta = new ValueMeta( "TestSerializable", ValueMetaInterface.TYPE_STRING );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_SERIALIZABLE, -1, -1, hBaseBytesUtilShim );

    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    byte[] reference = HBaseValueMeta.encodeObject( value );

    assertTrue( encoded != null );
    assertEquals( encoded.length, reference.length );
    assertEquals( HBaseValueMeta.decodeObject( encoded ), value );
  }

  @Test
  public void testEncodeColumnValueBinary() throws Exception {
    byte[] value = new String( "Hi there bob!" ).getBytes();
    ValueMetaInterface valMeta = new ValueMeta( "TestBinary", ValueMetaInterface.TYPE_BINARY );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BINARY, -1, -1, hBaseBytesUtilShim );

    byte[] encoded = HBaseValueMeta.encodeColumnValue( value, valMeta, mappingMeta, hBaseBytesUtilShim );

    assertTrue( encoded != null );
    assertEquals( encoded.length, value.length );
    for ( int i = 0; i < encoded.length; i++ ) {
      assertEquals( encoded[i], value[i] );
    }
  }

  @Test
  public void testDecodeColumnValueString() throws Exception {
    String value = "Hi there bob";
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_STRING, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );

    assertTrue( decoded != null );
    assertEquals( decoded, value );
  }

  @Test
  public void testDecodeColumnValueStringIndexedStorage() throws Exception {
    String value = "Value2";
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_STRING, -1, -1, hBaseBytesUtilShim );
    mappingMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    Object[] legalVals = new Object[] { "Value1", "Value2", "Value3" };
    mappingMeta.setIndex( legalVals );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );

    assertTrue( decoded != null );
    assertEquals( decoded, new Integer( 1 ) );
  }

  @Test
  public void testDecodeColumnValueStringIndexedStorageIllegalValue() throws Exception {
    String value = "Bogus";
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_STRING, -1, -1, hBaseBytesUtilShim );
    mappingMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
    Object[] legalVals = new Object[] { "Value1", "Value2", "Value3" };
    mappingMeta.setIndex( legalVals );

    try {
      Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
      fail( "Was expecting an exception because the supplied value is not in " + "the list of indexed values" );
    } catch ( Exception ex ) {
      //ignored
    }
  }

  @Test
  public void testDecodeColumnValueFloat() throws Exception {
    float value = 42f;
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_NUMBER, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Double );
    assertEquals( decoded, new Double( 42 ) );
  }

  @Test
  public void testDecodeColumnValueDouble() throws Exception {
    double value = 42;
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_NUMBER, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Double );
    assertEquals( decoded, new Double( 42 ) );
  }

  @Test
  public void testDecodeColumnValueInteger() throws Exception {
    int value = 42;
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_INTEGER, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Long );
    assertEquals( decoded, new Long( 42 ) );
  }

  @Test
  public void testDecodeColumnValueLong() throws Exception {
    long value = 42;
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_INTEGER, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Long );
    assertEquals( decoded, new Long( 42 ) );
  }

  @Test
  public void testDecodeColumnValueBooleanAsString() throws Exception {
    String value = "TRUE";
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BOOLEAN, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Boolean );
    assertEquals( decoded, new Boolean( true ) );
  }

  @Test
  public void testDecodeColumnValueBooleanAsInteger() throws Exception {
    Integer value = 0;
    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BOOLEAN, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Boolean );
    assertEquals( decoded, new Boolean( false ) );
  }

  @Test
  public void testDecodeColumnValueBigDecimal() throws Exception {
    BigDecimal bd = new BigDecimal( 42 );
    String value = bd.toString();

    byte[] encoded = hBaseBytesUtilShim.toBytes( value );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BIGNUMBER, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof BigDecimal );
    assertEquals( decoded, bd );
  }

  @Test
  public void testDecodeColumnValueSerializable() throws Exception {
    byte[] value = HBaseValueMeta.encodeObject( new String( "Hi there bob!" ) );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_SERIALIZABLE, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( value, mappingMeta, hBaseBytesUtilShim );

    assertTrue( decoded != null );
    assertTrue( decoded instanceof String );
    assertEquals( decoded, "Hi there bob!" );
  }

  @Test
  public void testDecodeColumnValueDate() throws Exception {
    Date value = new Date();
    byte[] encoded = hBaseBytesUtilShim.toBytes( value.getTime() );

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_DATE, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( encoded, mappingMeta, hBaseBytesUtilShim );
    assertTrue( decoded != null );
    assertTrue( decoded instanceof Date );
    assertEquals( decoded, value );
  }

  @Test
  public void testDecodeColumnValueBinary() throws Exception {
    byte[] value = new String( "Hi there bob!" ).getBytes();

    HBaseValueMeta mappingMeta =
        new HBaseValueMetaInterfaceImpl( "famliy1" + HBaseValueMeta.SEPARATOR + "testcol" + HBaseValueMeta.SEPARATOR + "anAlias",
            ValueMetaInterface.TYPE_BINARY, -1, -1, hBaseBytesUtilShim );

    Object decoded = HBaseValueMeta.decodeColumnValue( value, mappingMeta, hBaseBytesUtilShim );

    assertTrue( decoded != null );
    assertTrue( decoded instanceof byte[] );
    assertEquals( new String( (byte[]) decoded ), "Hi there bob!" );
  }
}
