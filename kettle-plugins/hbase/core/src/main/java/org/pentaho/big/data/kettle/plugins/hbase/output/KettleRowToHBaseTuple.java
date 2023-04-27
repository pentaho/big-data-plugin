/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hbase.output;

import java.util.Map;

import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingUtils;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping.KeyType;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTableWriteOperationManager;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;

public class KettleRowToHBaseTuple {

  private int keyIndex = -1;
  private ValueMetaInterface keyInMeta;
  private KeyType keyType;

  private int familyIndex = -1;
  private ValueMetaInterface familyInMeta;

  private int columnIndex = -1;
  private ValueMetaInterface columnInMeta;

  private int valueIndex = -1;
  private ValueMetaInterface valueInMeta;
  private HBaseValueMetaInterface valueMeta;

  private int visibilityIndex = -1;
  private ValueMetaInterface visibilityInMeta;
  private HBaseValueMetaInterface visibilityMeta;

  /**
   * Creates a conversion class that converts an incoming row object with values for the various Tuple fields <KEY,
   * Family, Column, Value> into an HBasePut
   *
   * @param inputRowMeta
   *          The row meta of the incoming row structure
   * @param tupleMapping
   *          The mapping in use for the step
   * @param columnMapping
   *          The non-KEY columns in the mapping mapped by column alias
   * @throws KettleException
   */
  public KettleRowToHBaseTuple( RowMetaInterface inputRowMeta, Mapping tupleMapping,
      Map<String, HBaseValueMetaInterface> columnMapping ) throws KettleException {

    String keyName = tupleMapping.getKeyName();
    keyIndex = inputRowMeta.indexOfValue( keyName );
    if ( keyIndex < 0 ) {
      // No Key Column
      throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.NoKeyColumn" ) );
    }
    keyInMeta = inputRowMeta.getValueMeta( keyIndex );
    keyType = tupleMapping.getKeyType();

    familyIndex = inputRowMeta.indexOfValue( Mapping.TupleMapping.FAMILY.toString() );
    if ( familyIndex < 0 ) {
      throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.NoFamilyColumn" ) );
    }
    familyInMeta = inputRowMeta.getValueMeta( familyIndex );

    columnIndex = inputRowMeta.indexOfValue( Mapping.TupleMapping.COLUMN.toString() );
    if ( columnIndex < 0 ) {
      throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.NoColumnColumn" ) );
    }
    columnInMeta = inputRowMeta.getValueMeta( columnIndex );

    // NOTE: TIMESTAMPS cannot be written via HBase Put, so the column is useless for writing

    valueIndex = inputRowMeta.indexOfValue( Mapping.TupleMapping.VALUE.toString() );
    if ( valueIndex < 0 ) {
      throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG, "HBaseOutput.Error.NoValueColumn" ) );
    }
    valueInMeta = inputRowMeta.getValueMeta( valueIndex );
    valueMeta = columnMapping.get( valueInMeta.getName() );

    // NOTE: The Visibility Index is optional
    visibilityIndex = inputRowMeta.indexOfValue( MappingUtils.TUPLE_MAPPING_VISIBILITY );
    if ( visibilityIndex >= 0 ) {
      visibilityInMeta = inputRowMeta.getValueMeta( visibilityIndex );
      visibilityMeta = columnMapping.get( visibilityInMeta.getName() );
      if ( visibilityMeta == null ) {
        // There is no column mapping for Visibility, so disable it by removing the index in the RowMeta
        visibilityInMeta = null;
        visibilityIndex = -1;
      }
    }

  }

  /**
   * Creates an HBasePut representing the tuple by extracting data from a row
   *
   * @param hBaseTableWriteOperationManager
   *          HBase write manager
   * @param bu
   *          The Byte Conversion utility (Required for key conversion)
   * @param row
   *          Object containing row data
   * @param writeToWAL
   *          Should data be written to WAL?
   * @return An HBase Put for the tuple
   * @throws Exception
   */
  public HBasePut createTuplePut( HBaseTableWriteOperationManager hBaseTableWriteOperationManager,
      ByteConversionUtil bu, Object[] row, boolean writeToWAL ) throws Exception {

    if ( keyInMeta.isNull( row[keyIndex] ) ) {
      throw new FieldException( Mapping.TupleMapping.KEY );
    }
    if ( familyInMeta.isNull( row[familyIndex] ) ) {
      throw new FieldException( Mapping.TupleMapping.FAMILY );
    }
    if ( columnInMeta.isNull( row[columnIndex] ) ) {
      throw new FieldException( Mapping.TupleMapping.COLUMN );
    }
    if ( valueInMeta.isNull( row[valueIndex] ) ) {
      throw new FieldException( Mapping.TupleMapping.VALUE );
    }

    byte[] encodedKey = bu.encodeKeyValue( row[keyIndex], keyInMeta, keyType );

    HBasePut put = hBaseTableWriteOperationManager.createPut( encodedKey );

    // Note: Families must always be string with the implementation of HBasePut
    String columnFamily = familyInMeta.getString( row[familyIndex] );

    boolean binaryColName = false;
    String columnName = columnInMeta.getString( row[columnIndex] );
    if ( columnName.startsWith( "@@@binary@@@" ) ) {
      // assume hex encoded column name
      columnName = columnName.replace( "@@@binary@@@", "" );
      binaryColName = true;
    }

    byte[] encodedValue = valueMeta.encodeColumnValue( row[valueIndex], valueInMeta );
    put.addColumn( columnFamily, columnName, binaryColName, encodedValue );

    if ( visibilityIndex >= 0 && !visibilityInMeta.isNull( row[visibilityIndex] ) ) {
      byte[] encodedVisibility = visibilityMeta.encodeColumnValue( row[visibilityIndex], visibilityInMeta );
      put.addColumn( columnFamily, MappingUtils.TUPLE_MAPPING_VISIBILITY, false, encodedVisibility );
    }

    put.setWriteToWAL( writeToWAL );
    return put;
  }

  public static class FieldException extends Exception {

    public Mapping.TupleMapping field;

    public FieldException( Mapping.TupleMapping field ) {
      super();
      this.field = field;
    }

    public String getFieldString() {
      return field.toString();
    }

  }

}
