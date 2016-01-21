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

package org.pentaho.bigdata.api.hbase;

import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.io.IOException;

/**
 * Created by bryan on 1/19/16.
 */
public interface ByteConversionUtil {
  int getSizeOfFloat();

  int getSizeOfDouble();

  int getSizeOfInt();

  int getSizeOfLong();

  int getSizeOfShort();

  int getSizeOfByte();

  byte[] toBytes( String var1 );

  byte[] toBytes( int var1 );

  byte[] toBytes( long var1 );

  byte[] toBytes( float var1 );

  byte[] toBytes( double var1 );

  byte[] toBytesBinary( String var1 );

  String toString( byte[] var1 );

  long toLong( byte[] var1 );

  int toInt( byte[] var1 );

  float toFloat( byte[] var1 );

  double toDouble( byte[] var1 );

  short toShort( byte[] var1 );

  byte[] encodeKeyValue( Object keyValue, Mapping.KeyType keyType ) throws KettleException;

  byte[] encodeObject( Object obj ) throws IOException;

  byte[] compoundKey( String... keys ) throws IOException;

  String[] splitKey( byte[] compoundKey ) throws IOException;

  String objectIndexValuesToString( Object[] values );

  Object[] stringIndexListToObjects( String list ) throws IllegalArgumentException;

  byte[] encodeKeyValue( Object o, ValueMetaInterface valueMetaInterface, Mapping.KeyType keyType )
    throws KettleException;

  boolean isImmutableBytesWritable( Object o );
}
