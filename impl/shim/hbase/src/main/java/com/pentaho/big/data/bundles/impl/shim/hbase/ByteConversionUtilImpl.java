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
import org.pentaho.bigdata.api.hbase.ByteConversionUtil;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.hbase.shim.api.HBaseValueMeta;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/21/16.
 */
public class ByteConversionUtilImpl implements ByteConversionUtil {
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ByteConversionUtilImpl( HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
  }

  @Override public int getSizeOfFloat() {
    return hBaseBytesUtilShim.getSizeOfFloat();
  }

  @Override public int getSizeOfDouble() {
    return hBaseBytesUtilShim.getSizeOfDouble();
  }

  @Override public int getSizeOfInt() {
    return hBaseBytesUtilShim.getSizeOfInt();
  }

  @Override public int getSizeOfLong() {
    return hBaseBytesUtilShim.getSizeOfLong();
  }

  @Override public int getSizeOfShort() {
    return hBaseBytesUtilShim.getSizeOfShort();
  }

  @Override public int getSizeOfByte() {
    return hBaseBytesUtilShim.getSizeOfByte();
  }

  @Override public byte[] toBytes( String var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( int var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( long var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( float var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytes( double var1 ) {
    return hBaseBytesUtilShim.toBytes( var1 );
  }

  @Override public byte[] toBytesBinary( String var1 ) {
    return hBaseBytesUtilShim.toBytesBinary( var1 );
  }

  @Override public String toString( byte[] var1 ) {
    return hBaseBytesUtilShim.toString( var1 );
  }

  @Override public long toLong( byte[] var1 ) {
    return hBaseBytesUtilShim.toLong( var1 );
  }

  @Override public int toInt( byte[] var1 ) {
    return hBaseBytesUtilShim.toInt( var1 );
  }

  @Override public float toFloat( byte[] var1 ) {
    return hBaseBytesUtilShim.toFloat( var1 );
  }

  @Override public double toDouble( byte[] var1 ) {
    return hBaseBytesUtilShim.toDouble( var1 );
  }

  @Override public short toShort( byte[] var1 ) {
    return hBaseBytesUtilShim.toShort( var1 );
  }

  @Override public byte[] encodeKeyValue( Object keyValue, Mapping.KeyType keyType ) throws KettleException {
    return HBaseValueMeta
      .encodeKeyValue( keyValue, org.pentaho.hbase.shim.api.Mapping.KeyType.valueOf( keyType.name() ),
        hBaseBytesUtilShim );
  }

  @Override public byte[] encodeObject( Object obj ) throws IOException {
    return HBaseValueMeta.encodeObject( obj );
  }

  @Override public byte[] compoundKey( String... keys ) throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    for ( String key : keys ) {
      stringBuilder.append( key );
      stringBuilder.append( HBaseValueMeta.SEPARATOR );
    }
    if ( stringBuilder.length() > 0 ) {
      stringBuilder.setLength( stringBuilder.length() - HBaseValueMeta.SEPARATOR.length() );
    }
    return toBytes( stringBuilder.toString() );
  }

  @Override public String[] splitKey( byte[] compoundKey ) throws IOException {
    return toString( compoundKey ).split( HBaseValueMeta.SEPARATOR );
  }

  @Override public String objectIndexValuesToString( Object[] values ) {
    return HBaseValueMeta.objectIndexValuesToString( values );
  }

  @Override public Object[] stringIndexListToObjects( String list ) throws IllegalArgumentException {
    return HBaseValueMeta.stringIndexListToObjects( list );
  }

  @Override public byte[] encodeKeyValue( Object o, ValueMetaInterface valueMetaInterface, Mapping.KeyType keyType )
    throws KettleException {
    return HBaseValueMeta
      .encodeKeyValue( o, valueMetaInterface, org.pentaho.hbase.shim.api.Mapping.KeyType.valueOf( keyType.name() ),
        hBaseBytesUtilShim );
  }

  @Override public boolean isImmutableBytesWritable( Object o ) {
    return o instanceof ImmutableBytesWritable;
  }
}
