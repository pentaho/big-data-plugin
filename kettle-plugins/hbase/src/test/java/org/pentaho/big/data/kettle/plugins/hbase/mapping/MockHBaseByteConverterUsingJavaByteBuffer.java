/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import org.pentaho.hadoop.shim.api.internal.hbase.HBaseBytesUtilShim;

import java.nio.ByteBuffer;

/**
 * @author Vasilina Terehova
 */

public class MockHBaseByteConverterUsingJavaByteBuffer implements HBaseBytesUtilShim {

  @Override public int getSizeOfFloat() {
    return Float.SIZE / Byte.SIZE;
  }

  @Override public int getSizeOfDouble() {
    return Double.SIZE / Byte.SIZE;
  }

  @Override public int getSizeOfInt() {
    return Integer.SIZE / Byte.SIZE;
  }

  @Override public int getSizeOfLong() {
    return Long.SIZE / Byte.SIZE;
  }

  @Override public int getSizeOfShort() {
    return Short.SIZE / Byte.SIZE;
  }

  @Override public int getSizeOfByte() {
    return 1;
  }

  @Override public byte[] toBytes( String aString ) {
    return aString.getBytes();
  }

  @Override public byte[] toBytes( int anInt ) {
    return ByteBuffer.allocate( getSizeOfInt() ).putInt( anInt ).array();
  }

  @Override public byte[] toBytes( long aLong ) {
    return ByteBuffer.allocate( getSizeOfLong() ).putLong( aLong ).array();
  }

  @Override public byte[] toBytes( float aFloat ) {
    return ByteBuffer.allocate( getSizeOfFloat() ).putFloat( aFloat ).array();
  }

  @Override public byte[] toBytes( double aDouble ) {
    return ByteBuffer.allocate( getSizeOfDouble() ).putDouble( aDouble ).array();
  }

  @Override public byte[] toBytesBinary( String value ) {
    return value.getBytes();
  }

  @Override public String toString( byte[] value ) {
    return new String( value );
  }

  @Override public long toLong( byte[] value ) {
    return ByteBuffer.wrap( value ).getLong();
  }

  @Override public int toInt( byte[] value ) {
    return ByteBuffer.wrap( value ).getInt();
  }

  @Override public float toFloat( byte[] value ) {
    return ByteBuffer.wrap( value ).getFloat();
  }

  @Override public double toDouble( byte[] value ) {
    return ByteBuffer.wrap( value ).getDouble();
  }

  @Override public short toShort( byte[] value ) {
    return ByteBuffer.wrap( value ).getShort();
  }
}
