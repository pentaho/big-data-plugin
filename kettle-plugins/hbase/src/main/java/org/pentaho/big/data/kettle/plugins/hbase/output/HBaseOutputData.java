/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.table.HBasePut;
import org.pentaho.hadoop.shim.api.hbase.table.HBaseTableWriteOperationManager;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/**
 * Class providing an output step for writing data to an HBase table according to meta data column/type mapping info
 * stored in a separate HBase table called "pentaho_mappings". See org.pentaho.hbase.mapping.Mapping for details on the
 * meta data format.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class HBaseOutputData extends BaseStepData implements StepDataInterface {

  /** The output data format */
  protected RowMetaInterface m_outputRowMeta;

  public RowMetaInterface getOutputRowMeta() {
    return m_outputRowMeta;
  }

  public void setOutputRowMeta( RowMetaInterface rmi ) {
    m_outputRowMeta = rmi;
  }

  /**
   * Sets up a new target table put operation using the connection shim
   *
   * @param inRowMeta
   *          the incoming kettle row meta data
   * @param keyIndex
   *          the index of the key in the incoming row structure
   * @param kettleRow
   *          the current incoming kettle row
   * @param tableMapping
   *          the HBase table mapping to use
   * @param bu
   *          the byte util shim to use for conversion to and from byte arrays
   * @param hbAdmin
   *          the connection shim
   * @param writeToWAL
   *          true if the write ahead log should be written to
   * @return false if the key is null (missing) for the current incoming kettle row
   * @throws Exception
   *           if a problem occurs when initializing the new put operation
   */
  public static HBasePut initializeNewPut( RowMetaInterface inRowMeta, int keyIndex, Object[] kettleRow,
      Mapping tableMapping, ByteConversionUtil bu, HBaseTableWriteOperationManager hBaseTableWriteOperationManager,
      boolean writeToWAL ) throws Exception {
    ValueMetaInterface keyvm = inRowMeta.getValueMeta( keyIndex );

    if ( keyvm.isNull( kettleRow[keyIndex] ) ) {
      return null;
    }

    byte[] encodedKey = bu.encodeKeyValue( kettleRow[keyIndex], keyvm, tableMapping.getKeyType() );

    HBasePut hBaseTablePut = hBaseTableWriteOperationManager.createPut( encodedKey );
    hBaseTablePut.setWriteToWAL( writeToWAL );
    return hBaseTablePut;
  }

  /**
   * Adds those incoming kettle field values that are defined in the table mapping for the current row to the target
   * table put operation
   *
   * @param inRowMeta
   *          the incoming kettle row meta data
   * @param kettleRow
   *          the current incoming kettle row
   * @param keyIndex
   *          the index of the key in the incoming row structure
   * @param columnsMappedByAlias
   *          the columns in the table mapping
   * @param hbAdmin
   *          the connection shim
   * @param bu
   *          the byte util shim to use for conversion to and from byte arrays
   * @throws KettleException
   *           if a problem occurs when adding a column to the put operation
   */
  public static void addColumnsToPut( RowMetaInterface inRowMeta, Object[] kettleRow, int keyIndex,
      Map<String, HBaseValueMetaInterface> columnsMappedByAlias, HBasePut hBasePut, ByteConversionUtil bu )
    throws KettleException {

    for ( int i = 0; i < inRowMeta.size(); i++ ) {
      ValueMetaInterface current = inRowMeta.getValueMeta( i );
      if ( i != keyIndex && !current.isNull( kettleRow[i] ) ) {
        HBaseValueMetaInterface hbaseColMeta = columnsMappedByAlias.get( current.getName() );
        String columnFamily = hbaseColMeta.getColumnFamily();
        String columnName = hbaseColMeta.getColumnName();

        boolean binaryColName = false;
        if ( columnName.startsWith( "@@@binary@@@" ) ) {
          // assume hex encoded column name
          columnName = columnName.replace( "@@@binary@@@", "" );
          binaryColName = true;
        }
        byte[] encoded = hbaseColMeta.encodeColumnValue( kettleRow[i], current );

        try {
          hBasePut.addColumn( columnFamily, columnName, binaryColName, encoded );
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( HBaseOutputMeta.PKG,
              "HBaseOutput.Error.UnableToAddColumnToTargetTablePut" ), ex );
        }
      }
    }
  }

  public static URL stringToURL( String pathOrURL ) throws MalformedURLException {
    URL result = null;

    if ( !Const.isEmpty( pathOrURL ) ) {
      if ( pathOrURL.toLowerCase().startsWith( "http://" ) || pathOrURL.toLowerCase().startsWith( "file://" ) ) {
        result = new URL( pathOrURL );
      } else {
        String c = "file://" + pathOrURL;
        result = new URL( c );
      }
    }

    return result;
  }

}
