/*
 * *****************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaBase;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;

import java.math.BigDecimal;
import java.util.Date;

public class AELHBaseValueMetaImpl extends ValueMetaBase implements HBaseValueMetaInterface {
  private boolean isKey;
  private String alias;
  private String columnName;
  private String columnFamily;
  private String mappingName;
  private String tableName;
  private boolean isLongOrDouble = true;

  public AELHBaseValueMetaImpl( boolean isKey, String alias, String columnName, String columnFamily, String mappingName, String tableName ) {
    super( alias );
    this.isKey = isKey;
    this.alias = alias;
    this.columnName = columnName;
    this.columnFamily = columnFamily;
    this.mappingName = mappingName;
    this.tableName = tableName;
  }

  @Override
  public boolean isKey() {
    return isKey;
  }

  @Override
  public void setKey( boolean key ) {
    isKey = key;
  }

  @Override
  public String getAlias() {
    return getName();
  }

  @Override
  public void setAlias( String alias ) {
    this.alias = alias;
    setName( alias );
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public void setColumnName( String columnName ) {
    this.columnName = columnName;
  }

  @Override
  public String getColumnFamily() {
    return columnFamily;
  }

  @Override
  public void setColumnFamily( String columnFamily ) {
    this.columnFamily = columnFamily;
  }

  @Override
  public void setHBaseTypeFromString( String hbaseType ) throws IllegalArgumentException {
    if ( hbaseType.equalsIgnoreCase( "Integer" ) ) {
      setType( ValueMeta.getType( hbaseType ) );
      setIsLongOrDouble( false );
      return;
    }
    if ( hbaseType.equalsIgnoreCase( "Long" ) ) {
      setType( ValueMeta.getType( "Integer" ) );
      setIsLongOrDouble( true );
      return;
    }
    if ( hbaseType.equals( "Float" ) ) {
      setType( ValueMeta.getType( "Number" ) );
      setIsLongOrDouble( false );
      return;
    }
    if ( hbaseType.equals( "Double" ) ) {
      setType( ValueMeta.getType( "Number" ) );
      setIsLongOrDouble( true );
      return;
    }

    // default
    int type = ValueMeta.getType( hbaseType );
    if ( type == ValueMetaInterface.TYPE_NONE ) {
      throw new IllegalArgumentException( BaseMessages.getString( PKG,
          "HBaseValueMeta.Error.UnknownType", hbaseType ) );
    }

    setType( type );
  }

  @Override
  public String getHBaseTypeDesc() {
    if ( isInteger() ) {
      return ( getIsLongOrDouble() ? "Long" : "Integer" );
    }
    if ( isNumber() ) {
      return ( getIsLongOrDouble() ? "Double" : "Float" );
    }

    return ValueMeta.getTypeDesc( getType() );
  }

  @Override
  public Object decodeColumnValue( byte[] rawColValue ) throws KettleException {
    // just return null if this column doesn't have a value for the row
    if ( rawColValue == null ) {
      return null;
    }

    if ( isString() ) {
      String convertedString = Bytes.toString( rawColValue );
      if ( getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED ) {
        // need to return the integer index of this value
        Object[] legalVals = getIndex();
        int foundIndex = -1;
        for ( int i = 0; i < legalVals.length; i++ ) {
          if ( legalVals[ i ].toString().trim().equals( convertedString.trim() ) ) {
            foundIndex = i;
            break;
          }
        }
        if ( foundIndex >= 0 ) {
          return new Integer( foundIndex );
        }
        throw new KettleException( BaseMessages.getString( PKG,
            "HBaseValueMeta.Error.IllegalIndexedColumnValue", convertedString,
            getAlias() ) );
      } else {
        return convertedString;
      }
    }

    if ( isNumber() ) {
      if ( rawColValue.length == Bytes.SIZEOF_FLOAT ) {
        float floatResult = Bytes.toFloat( rawColValue );
        return new Double( floatResult );
      }

      if ( rawColValue.length == Bytes.SIZEOF_DOUBLE ) {
        return new Double( Bytes.toDouble( rawColValue ) );
      }
    }

    if ( isInteger() ) {
      if ( rawColValue.length == Bytes.SIZEOF_INT ) {
        int intResult = Bytes.toInt( rawColValue );
        return new Long( intResult );
      }

      if ( rawColValue.length == Bytes.SIZEOF_LONG ) {
        return new Long( Bytes.toLong( rawColValue ) );
      }
      if ( rawColValue.length == Bytes.SIZEOF_SHORT ) {
        // be lenient on reading from HBase - accept and convert shorts
        // even though our mapping defines only longs and integers
        // TODO add short to the types that can be mapped?
        short tempShort = Bytes.toShort( rawColValue );
        return new Long( tempShort );
      }

      throw new KettleException( BaseMessages.getString( PKG,
          "HBaseValueMeta.Error.IllegalIntegerLength" ) );

    }

    if ( isBigNumber() ) {
      String temp = Bytes.toString( rawColValue );

      BigDecimal result = new BigDecimal( temp );

      if ( result == null ) {
        throw new KettleException( BaseMessages.getString( PKG,
            "HBaseValueMeta.Error.UnableToDecodeBigDecimal" ) );
      }

      return result;
    }

    if ( isBinary() ) {
      // just return the raw array of bytes
      return rawColValue;
    }

    if ( isBoolean() ) {
      // try as a string first
      Boolean result = decodeBoolFromString( rawColValue );
      if ( result == null ) {
        // try as a number
        result = decodeBoolFromNumber( rawColValue );
      }

      if ( result != null ) {
        return result;
      }

      throw new KettleException( BaseMessages.getString( PKG,
          "HBaseValueMeta.Error.UnableToDecodeBoolean" ) );
    }

    if ( isDate() ) {
      if ( rawColValue.length != Bytes.SIZEOF_LONG ) {
        throw new KettleException( BaseMessages.getString( PKG,
            "HBaseValueMeta.Error.DateValueLengthNotEqualToLong" ) );
      }
      long millis = Bytes.toLong( rawColValue );
      Date d = new Date( millis );
      return d;
    }

    throw new KettleException( BaseMessages.getString( PKG,
        "HBaseValueMeta.Error.UnknownTypeForColumn" ) );
  }

  @Override
  public byte[] encodeColumnValue( Object columnValue, ValueMetaInterface colMeta ) throws KettleException {
    if ( columnValue == null ) {
      return null;
    }

    byte[] encoded = null;

    /**
     * BACKLOG-26151 -
     * When doing type conversions, the type of this HBase value
     * is given by outputType, the colMeta then converts based on
     * the type of the incoming value
     */
    int outputType = this.getType();

    switch ( outputType ) {
      case TYPE_STRING:
        String toEncode = colMeta.getString( columnValue );
        encoded = Bytes.toBytes( toEncode );
        break;
      case TYPE_INTEGER:
        Long l = colMeta.getInteger( columnValue );
        if ( getIsLongOrDouble() ) {
          encoded = Bytes.toBytes( l.longValue() );
        } else {
          encoded = Bytes.toBytes( l.intValue() );
        }
        break;
      case TYPE_NUMBER:
        Double d = colMeta.getNumber( columnValue );
        if ( getIsLongOrDouble() ) {
          encoded = Bytes.toBytes( d.doubleValue() );
        } else {
          encoded = Bytes.toBytes( d.floatValue() );
        }
        break;
      case TYPE_DATE:
        Date date = colMeta.getDate( columnValue );
        encoded = Bytes.toBytes( date.getTime() );
        break;
      case TYPE_BOOLEAN:
        Boolean b = colMeta.getBoolean( columnValue );
        String boolString = ( b.booleanValue() ) ? "Y" : "N";
        encoded = Bytes.toBytes( boolString );
        break;
      case TYPE_BIGNUMBER:
        BigDecimal bd = colMeta.getBigNumber( columnValue );
        String bds = bd.toString();
        encoded = Bytes.toBytes( bds );
        break;
      case TYPE_BINARY:
        encoded = colMeta.getBinary( columnValue );
        break;
    }
    return encoded;
  }

  @Override
  public String getMappingName() {
    return mappingName;
  }

  @Override
  public void setMappingName( String mappingName ) {
    this.mappingName = mappingName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public void setTableName( String tableName ) {
    this.tableName = tableName;
  }

  @Override
  public boolean getIsLongOrDouble() {
    return isLongOrDouble;
  }

  @Override
  public void setIsLongOrDouble( boolean ld ) {
    this.isLongOrDouble = ld;
  }

  @Override
  public void getXml( StringBuilder retval ) {
    String format = getConversionMask();
    retval.append( "\n        " ).append( XMLHandler.openTag( "field" ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "table_name", getTableName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "mapping_name", getMappingName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "alias", getAlias() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "family", getColumnFamily() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "column", getColumnName() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "key", isKey() ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "type", ValueMetaBase.getTypeDesc( getType() ) ) );
    retval.append( "\n            " ).append( XMLHandler.addTagValue( "format", format ) );
    retval.append( "\n        " ).append( XMLHandler.closeTag( "field" ) );
  }

  @Override
  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step, int count ) throws KettleException {
    //noop in AEL
  }

  /**
   * Decodes a boolean value from an array of bytes that is assumed to hold a string.]
   * Lifted from Shim to support AEL conversions
   *
   * @param rawEncoded an array of bytes holding the string representation of a boolean value
   * @return a Boolean object or null if it can't be decoded from the supplied array of bytes.
   */
  public static Boolean decodeBoolFromString( byte[] rawEncoded ) {

    String tempString = Bytes.toString( rawEncoded );
    if ( tempString.equalsIgnoreCase( "Y" ) || tempString.equalsIgnoreCase( "N" )
        || tempString.equalsIgnoreCase( "YES" )
        || tempString.equalsIgnoreCase( "NO" )
        || tempString.equalsIgnoreCase( "TRUE" )
        || tempString.equalsIgnoreCase( "FALSE" )
        || tempString.equalsIgnoreCase( "T" ) || tempString.equalsIgnoreCase( "F" )
        || tempString.equalsIgnoreCase( "1" ) || tempString.equalsIgnoreCase( "0" ) ) {

      return Boolean.valueOf( tempString.equalsIgnoreCase( "Y" )
          || tempString.equalsIgnoreCase( "YES" )
          || tempString.equalsIgnoreCase( "TRUE" )
          || tempString.equalsIgnoreCase( "T" )
          || tempString.equalsIgnoreCase( "1" ) );
    }
    return null;
  }

  /**
   * Decodes a boolean value from an array of bytes that is assumed to hold a number.
   * Lifted from Shim to support AEL conversions
   *
   * @param rawEncoded an array of bytes holding the numerical representation of a boolean value
   * @return a Boolean object or null if it can't be decoded from the supplied array of bytes.
   */
  public static Boolean decodeBoolFromNumber( byte[] rawEncoded ) {

    if ( rawEncoded == null ) {
      return null;
    }

    if ( rawEncoded.length == Bytes.SIZEOF_BYTE ) {
      byte val = rawEncoded[ 0 ];
      if ( val == 0 || val == 1 ) {
        return new Boolean( val == 1 );
      }
    }

    if ( rawEncoded.length == Bytes.SIZEOF_SHORT ) {
      short tempShort = Bytes.toShort( rawEncoded );

      if ( tempShort == 0 || tempShort == 1 ) {
        return new Boolean( tempShort == 1 );
      }
    }

    if ( rawEncoded.length == Bytes.SIZEOF_INT
        || rawEncoded.length == Bytes.SIZEOF_FLOAT ) {
      int tempInt = Bytes.toInt( rawEncoded );
      if ( tempInt == 1 || tempInt == 0 ) {
        return new Boolean( tempInt == 1 );
      }

      float tempFloat = Bytes.toFloat( rawEncoded );
      if ( tempFloat == 0.0f || tempFloat == 1.0f ) {
        return new Boolean( tempFloat == 1.0f );
      }
    }

    if ( rawEncoded.length == Bytes.SIZEOF_LONG
        || rawEncoded.length == Bytes.SIZEOF_DOUBLE ) {
      long tempLong = Bytes.toLong( rawEncoded );
      if ( tempLong == 0L || tempLong == 1L ) {
        return new Boolean( tempLong == 1L );
      }

      double tempDouble = Bytes.toDouble( rawEncoded );
      if ( tempDouble == 0.0 || tempDouble == 1.0 ) {
        return new Boolean( tempDouble == 1.0 );
      }
    }

    // not identifiable from a number
    return null;
  }
}

