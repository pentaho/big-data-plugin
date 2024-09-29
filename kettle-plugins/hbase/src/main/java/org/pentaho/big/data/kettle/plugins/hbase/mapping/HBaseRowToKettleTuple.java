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

import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Class for decoding HBase rows to a <key, family, column, value, time stamp> Kettle row format.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class HBaseRowToKettleTuple {

  /**
   * Holds a set of tuples (Kettle rows) - one for each column from an HBase row
   */
  protected List<Object[]> mDecodedTuples;

  /**
   * Index in the Kettle row format of the key column
   */
  protected int mKeyIndex = -1;

  /**
   * Index in the Kettle row format of the family column
   */
  protected int mFamilyIndex = -1;

  /**
   * Index in the Kettle row format of the column name column
   */
  protected int mColNameIndex = -1;

  /**
   * Index in the Kettle row format of the column value column
   */
  protected int mValueIndex = -1;

  /**
   * Index in the Kettle row format of the time stamp column
   */
  protected int mTimestampIndex = -1;

  /**
   * List of (optional) byte array encoded user-specified column families to extract column values for
   */
  protected List<byte[]> mUserSpecifiedFamilies;

  /**
   * List of (optional) human-readable user-specified column families to extract column values for
   */
  protected List<String> mUserSpecifiedFamiliesHumanReadable;

  protected List<HBaseValueMetaInterface> mTupleColsFromAliasMap;

  protected ByteConversionUtil mBytesUtil;

  public HBaseRowToKettleTuple( ByteConversionUtil bytesUtil ) {
    if ( bytesUtil == null ) {
      throw new NullPointerException();
    }
    mBytesUtil = bytesUtil;
  }

  public void reset() {
    mDecodedTuples = null;

    mKeyIndex = -1;
    mFamilyIndex = -1;
    mColNameIndex = -1;
    mValueIndex = -1;
    mTimestampIndex = -1;
    mUserSpecifiedFamilies = null;
    mUserSpecifiedFamiliesHumanReadable = null;

    mTupleColsFromAliasMap = null;
  }

  /**
   * Convert an HBase row to (potentially) multiple Kettle rows in tuple format.
   *
   * @param mapping                the mapping information to use (must be a "tuple" mapping)
   * @param tupleColsMappedByAlias the meta data for each of the tuple columns the user has opted to have output
   * @param outputRowMeta          the outgoing Kettle row format
   * @return a list of Kettle rows in tuple format
   * @throws KettleException if a problem occurs
   */
  public List<Object[]> hbaseRowToKettleTupleMode( HBaseValueMetaInterfaceFactory hBaseValueMetaInterfaceFactory,
                                                   Object result, Mapping mapping,
                                                   Map<String, HBaseValueMetaInterface> tupleColsMappedByAlias,
                                                   RowMetaInterface outputRowMeta ) throws KettleException {

    if ( mDecodedTuples == null ) {
      mTupleColsFromAliasMap = new ArrayList<>();
      // add the key first - type (or name for that matter)
      // is not important as this is just a dummy placeholder
      // here so that indexes into m_tupleColsFromAliasMap align with the output
      // row meta
      // format
      HBaseValueMetaInterface keyMeta = hBaseValueMetaInterfaceFactory
        .createHBaseValueMetaInterface( null, mapping.getKeyName(), "dummy", ValueMetaInterface.TYPE_INTEGER, 0, 0 );
      mTupleColsFromAliasMap.add( keyMeta );

      for ( Map.Entry<String, HBaseValueMetaInterface> entry : tupleColsMappedByAlias.entrySet() ) {
        mTupleColsFromAliasMap.add( tupleColsMappedByAlias.get( entry.getValue() ) );
      }
    }

    return hbaseRowToKettleTupleMode( result, mapping, mTupleColsFromAliasMap, outputRowMeta );
  }

  /**
   * Convert an HBase row to (potentially) multiple Kettle rows in tuple format.
   *
   * @param mapping       the mapping information to use (must be a "tuple" mapping)
   * @param tupleCols     the meta data for each of the tuple columns the user has opted to have output
   * @param outputRowMeta the outgoing Kettle row format
   * @return a list of Kettle rows in tuple format
   * @throws KettleException if a problem occurs
   */
  public List<Object[]> hbaseRowToKettleTupleMode( Object result, Mapping mapping,
                                                   List<HBaseValueMetaInterface> tupleCols,
                                                   RowMetaInterface outputRowMeta ) throws KettleException {

    if ( mDecodedTuples == null ) {
      mDecodedTuples = new ArrayList<>();
      mKeyIndex = outputRowMeta.indexOfValue( mapping.getKeyName() );
      mFamilyIndex = outputRowMeta.indexOfValue( Mapping.TupleMapping.FAMILY.toString() );
      mColNameIndex = outputRowMeta.indexOfValue( Mapping.TupleMapping.COLUMN.toString() );
      mValueIndex = outputRowMeta.indexOfValue( Mapping.TupleMapping.VALUE.toString() );
      mTimestampIndex = outputRowMeta.indexOfValue( Mapping.TupleMapping.TIMESTAMP.toString() );

      if ( !Const.isEmpty( mapping.getTupleFamilies() ) ) {
        String[] familiesS = mapping.getTupleFamiliesSplit();
        mUserSpecifiedFamilies = new ArrayList<>();
        mUserSpecifiedFamiliesHumanReadable = new ArrayList<>();

        for ( String family : familiesS ) {
          mUserSpecifiedFamiliesHumanReadable.add( family );
          mUserSpecifiedFamilies.add( mBytesUtil.toBytes( family.trim() ) );
        }
      }
    } else {
      mDecodedTuples.clear();
    }

    byte[] rawKey = null;
    try {
      rawKey = (byte[]) result.getClass().getMethod( "getRow" ).invoke( result );
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }
    Object decodedKey = mapping.decodeKeyValue( rawKey );

    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowData = null;
    try {
      rowData =
        (NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>) result.getClass().getMethod( "getMap" )
          .invoke( result );
    } catch ( Exception ex ) {
      throw new KettleException( ex );
    }

    if ( !Const.isEmpty( mapping.getTupleFamilies() ) ) {
      int i = 0;
      for ( byte[] family : mUserSpecifiedFamilies ) {
        NavigableMap<byte[], NavigableMap<Long, byte[]>> colMap = rowData.get( family );
        for ( Map.Entry<byte[], NavigableMap<Long, byte[]>> colMapEntry : colMap.entrySet() ) {
          NavigableMap<Long, byte[]> valuesByTimestamp = colMapEntry.getValue();

          Object[] newTuple = RowDataUtil.allocateRowData( outputRowMeta.size() );

          // row key
          if ( mKeyIndex != -1 ) {
            newTuple[ mKeyIndex ] = decodedKey;
          }

          // get value of most recent column value
          Map.Entry<Long, byte[]> mostRecentColVal = valuesByTimestamp.lastEntry();

          // store the timestamp
          if ( mTimestampIndex != -1 ) {
            newTuple[ mTimestampIndex ] = mostRecentColVal.getKey();
          }

          // column name
          if ( mColNameIndex != -1 ) {
            HBaseValueMetaInterface colNameMeta = tupleCols.get( mColNameIndex );
            Object decodedColName = colNameMeta.decodeColumnValue( colMapEntry.getKey() );
            newTuple[ mColNameIndex ] = decodedColName;
          }

          // column value
          if ( mValueIndex != -1 ) {
            HBaseValueMetaInterface colValueMeta = tupleCols.get( mValueIndex );
            Object decodedValue =
              colValueMeta.decodeColumnValue( mostRecentColVal.getValue() );
            newTuple[ mValueIndex ] = decodedValue;
          }

          // column family
          if ( mFamilyIndex != -1 ) {
            newTuple[ mFamilyIndex ] = mUserSpecifiedFamiliesHumanReadable.get( i );
          }

          mDecodedTuples.add( newTuple );
        }
        i++;
      }
    } else {
      // process all column families
      for ( Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowDataEntry : rowData.entrySet() ) {

        // column family
        Object decodedFamily = null;
        if ( mFamilyIndex != -1 ) {
          HBaseValueMetaInterface colFamMeta = tupleCols.get( mFamilyIndex );
          decodedFamily = colFamMeta.decodeColumnValue( rowDataEntry.getKey() );
        }

        NavigableMap<byte[], NavigableMap<Long, byte[]>> colMap = rowDataEntry.getValue();
        for ( Map.Entry<byte[], NavigableMap<Long, byte[]>> colMapEntry : colMap.entrySet() ) {
          NavigableMap<Long, byte[]> valuesByTimestamp = colMapEntry.getValue();

          Object[] newTuple = RowDataUtil.allocateRowData( outputRowMeta.size() );

          // row key
          if ( mKeyIndex != -1 ) {
            newTuple[ mKeyIndex ] = decodedKey;
          }

          // get value of most recent column value
          Map.Entry<Long, byte[]> mostRecentColVal = valuesByTimestamp.lastEntry();

          // store the timestamp
          if ( mTimestampIndex != -1 ) {
            newTuple[ mTimestampIndex ] = mostRecentColVal.getKey();
          }

          // column name
          if ( mColNameIndex != -1 ) {
            HBaseValueMetaInterface colNameMeta = tupleCols.get( mColNameIndex );
            Object decodedColName = colNameMeta.decodeColumnValue( colMapEntry.getKey() );
            newTuple[ mColNameIndex ] = decodedColName;
          }

          // column value
          if ( mValueIndex != -1 ) {
            HBaseValueMetaInterface colValueMeta = tupleCols.get( mValueIndex );
            Object decodedValue = colValueMeta.decodeColumnValue( mostRecentColVal.getValue() );
            newTuple[ mValueIndex ] = decodedValue;
          }

          // column family
          if ( mFamilyIndex != -1 ) {
            newTuple[ mFamilyIndex ] = decodedFamily;
          }

          mDecodedTuples.add( newTuple );
        }
      }
    }

    return mDecodedTuples;
  }
}
