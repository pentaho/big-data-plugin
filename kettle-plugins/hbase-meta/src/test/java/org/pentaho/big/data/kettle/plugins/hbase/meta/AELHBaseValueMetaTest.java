/*
 * *****************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *  *******************************************************************************
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License. You may obtain a copy of the
 *  License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 *
 */

package org.pentaho.big.data.kettle.plugins.hbase.meta;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.core.exception.KettleException;

import java.math.BigDecimal;
import java.util.Date;

@RunWith( MockitoJUnitRunner.class )
public class AELHBaseValueMetaTest {
  private AELHBaseValueMetaImpl stubValueMeta;

  @Before
  public void setup() throws Exception {
    stubValueMeta = new AELHBaseValueMetaImpl( true, "testAlias",
        "testColumnName", "testColumnFamily", "testMappingName",
        "testTableName" );

    stubValueMeta.setMappingName( "testMappingName" );
    stubValueMeta.setTableName( "testTableName" );

    stubValueMeta.setType( 5 );
    stubValueMeta.setIsLongOrDouble( false );
  }

  @Test
  public void getXmlSerializationTest() {
    StringBuilder sb = new StringBuilder(  );

    stubValueMeta.getXml( sb );

    Assert.assertTrue( sb.toString().contains( "Y" ) );
    Assert.assertTrue( sb.toString().contains( "testAlias" ) );
    Assert.assertTrue( sb.toString().contains( "testColumnName" ) );
  }

  @Test
  public void getHBaseTypeDescTest() {
    String stubType = stubValueMeta.getHBaseTypeDesc();

    Assert.assertEquals( "Integer", stubType );
  }

  @Test
  public void getHBaseTypeDescNumberTest() {
    stubValueMeta.setType( 1 );
    String stubType = stubValueMeta.getHBaseTypeDesc();

    Assert.assertEquals( "Float", stubType );
  }

  @Test
  public void decodeNullBytesTest() throws KettleException {
    Object shouldBeNull = stubValueMeta.decodeColumnValue( null );

    Assert.assertNull( shouldBeNull );
  }

  @Test
  public void decodeStringIntoObject() throws KettleException {
    stubValueMeta.setType( 2 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( "stubString" ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeNumberIntoObject() throws KettleException {
    stubValueMeta.setType( 1 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 2.2 ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeFloadIntoObject() throws KettleException {
    stubValueMeta.setType( 1 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 2.2f ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeIntegerIntoObject() throws KettleException {
    stubValueMeta.setType( 5 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1 ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeLongIntoObject() throws KettleException {
    stubValueMeta.setType( 5 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1L ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeShortIntoObject() throws KettleException {
    stubValueMeta.setType( 5 );
    short i = 1;
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( i ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBigNumberIntoObject() throws KettleException {
    stubValueMeta.setType( 6 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes(  "9.9999999" ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBooleanStringIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( "1" ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBooleanFloatIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1.0f ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBooleanLongIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1L ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBooleanDoubleIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1.0 ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBooleanBytesIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    byte i = 1;
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( i ) );

    Assert.assertNotNull( str );
  }

  public void decodeBooleanShortIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    short i = 1;
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( i ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBooleanIntoObject() throws KettleException {
    stubValueMeta.setType( 4 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1 ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeBytesIntoObject() throws KettleException {
    stubValueMeta.setType( 8 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1010 ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void decodeDateIntoObject() throws KettleException {
    stubValueMeta.setType( 3 );
    Object str = stubValueMeta.decodeColumnValue( Bytes.toBytes( 1539717565559l ) );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeNullBytesTest() throws KettleException {
    Object shouldBeNull = stubValueMeta.encodeColumnValue( null, stubValueMeta );

    Assert.assertNull( shouldBeNull );
  }

  @Test
  public void encodeStringIntoBytes() throws KettleException {
    stubValueMeta.setType( 2 );
    Object str = stubValueMeta.encodeColumnValue( "stubString", stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeNumberIntoBytes() throws KettleException {
    stubValueMeta.setType( 1 );
    Object str = stubValueMeta.encodeColumnValue(2.2, stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeIntegerIntoBytes() throws KettleException {
    stubValueMeta.setType( 5 );
    Object str = stubValueMeta.encodeColumnValue(1L, stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeBigNumberIntoBytes() throws KettleException {
    stubValueMeta.setType( 6 );
    Object str = stubValueMeta.encodeColumnValue( new BigDecimal(  9.9999999 ), stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeDateIntoBytes() throws KettleException {
    stubValueMeta.setType( 3 );
    Object str = stubValueMeta.encodeColumnValue( new Date(), stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeBooleanIntoBytes() throws KettleException {
    stubValueMeta.setType( 4 );
    Object str = stubValueMeta.encodeColumnValue( Boolean.TRUE, stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void encodeBinaryIntoBytes() throws KettleException {
    stubValueMeta.setType( 8 );
    Object str = stubValueMeta.encodeColumnValue( new byte[]{ 1, 0, 1 }, stubValueMeta );

    Assert.assertNotNull( str );
  }

  @Test
  public void integerIsNotLongOrDoubleTest() {
    stubValueMeta.setHBaseTypeFromString( "Integer" );

    Assert.assertFalse( stubValueMeta.getIsLongOrDouble() );
  }

  @Test
  public void longIsLongOrDouble() {
    stubValueMeta.setHBaseTypeFromString( "Long" );

    Assert.assertTrue( stubValueMeta.getIsLongOrDouble() );
  }

  @Test
  public void floatIsNotLongOrDouble() {
    stubValueMeta.setHBaseTypeFromString( "Float" );

    Assert.assertFalse( stubValueMeta.getIsLongOrDouble() );
  }

  @Test
  public void doubleIsLongOrDouble() {
    stubValueMeta.setHBaseTypeFromString( "Double" );

    Assert.assertTrue( stubValueMeta.getIsLongOrDouble() );
  }
}

