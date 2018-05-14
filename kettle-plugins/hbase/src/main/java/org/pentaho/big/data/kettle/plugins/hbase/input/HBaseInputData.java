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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import org.pentaho.big.data.kettle.plugins.hbase.mapping.HBaseRowToKettleTuple;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.Result;
import org.pentaho.hadoop.shim.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class providing an input step for reading data from an HBase table according to meta data mapping info stored in a
 * separate HBase table called "pentaho_mappings". See org.pentaho.hbase.mapping.Mapping for details on the meta data
 * format.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 * 
 */
public class HBaseInputData extends BaseStepData implements StepDataInterface {

  /** The output data format */
  protected RowMetaInterface m_outputRowMeta;

  /**
   * Get the output row format
   * 
   * @return the output row format
   */
  public RowMetaInterface getOutputRowMeta() {
    return m_outputRowMeta;
  }

  /**
   * Set the output row format
   * 
   * @param rmi
   *          the output row format
   */
  public void setOutputRowMeta( RowMetaInterface rmi ) {
    m_outputRowMeta = rmi;
  }

  /**
   * Utility method to covert a string to a URL object.
   * 
   * @param pathOrURL
   *          file or http URL as a string
   * @return a URL
   * @throws MalformedURLException
   *           if there is a problem with the URL.
   */
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

  /**
   * Set the specific columns to be returned by the scan.
   * 
   * @param resultScannerBuilder
   *          the resultScannerBuilder
   * @param limitCols
   *          the columns to limit the scan to
   * @param tableMapping
   *          the mapping information
   * @throws KettleException
   *           if a problem occurs
   */
  public static void setScanColumns( ResultScannerBuilder resultScannerBuilder, List<HBaseValueMetaInterface> limitCols, Mapping tableMapping )
    throws KettleException {
    for ( HBaseValueMetaInterface currentCol : limitCols ) {
      if ( !currentCol.isKey() ) {
        String colFamilyName = currentCol.getColumnFamily();
        String qualifier = currentCol.getColumnName();

        boolean binaryColName = false;
        if ( qualifier.startsWith( "@@@binary@@@" ) ) {
          qualifier = qualifier.replace( "@@@binary@@@", "" );
          binaryColName = true;
        }

        try {
          resultScannerBuilder.addColumnToScan( colFamilyName, qualifier, binaryColName );
        } catch ( Exception ex ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
              "HBaseInput.Error.UnableToAddColumnToScan" ), ex );
        }
      }
    }
  }

  /**
   * Set column filters to apply server-side to the scan results.
   * 
   * @param resultScannerBuilder
   *          the resultScannerBuilder
   * @param columnFilters
   *          the column filters to apply
   * @param matchAnyFilter
   *          if true then a row will be returned if any of the filters match (otherwise all have to match)
   * @param columnsMappedByAlias
   *          the columns defined in the mapping
   * @param vars
   *          variables to use
   * @throws KettleException
   *           if a problem occurs
   */
  public static void setScanFilters( ResultScannerBuilder resultScannerBuilder, Collection<ColumnFilter> columnFilters,
                                     boolean matchAnyFilter, Map<String, HBaseValueMetaInterface> columnsMappedByAlias, VariableSpace vars )
    throws KettleException {

    for ( ColumnFilter cf : columnFilters ) {
      String fieldAliasS = vars.environmentSubstitute( cf.getFieldAlias() );
      HBaseValueMetaInterface mappedCol = columnsMappedByAlias.get( fieldAliasS );
      if ( mappedCol == null ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.ColumnFilterIsNotInTheMapping", fieldAliasS ) );
      }

      // check the type (if set in the ColumnFilter) against the type
      // of this field in the mapping
      String fieldTypeS = vars.environmentSubstitute( cf.getFieldType() );
      if ( !Const.isEmpty( fieldTypeS ) ) {
        if ( !mappedCol.getHBaseTypeDesc().equalsIgnoreCase( fieldTypeS ) ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG, "HBaseInput.Error.FieldTypeMismatch",
              fieldTypeS, fieldAliasS, mappedCol.getHBaseTypeDesc() ) );
        }
      }

      try {
        resultScannerBuilder.addColumnFilterToScan( cf, mappedCol, vars, matchAnyFilter );
      } catch ( Exception ex ) {
        throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
            "HBaseInput.Error.UnableToAddColumnFilterToScan" ), ex );
      }
    }
  }

  /**
   * Convert/decode the current hbase row into a list of "tuple" kettle rows
   * 
   * @param hBaseService
   *          the hBaseService
   * @param result
   *          the result to use
   * @param userOutputColumns
   *          user-specified subset of columns (if any) from the mapping
   * @param columnsMappedByAlias
   *          columns in the mapping keyed by alias
   * @param tableMapping
   *          the mapping to use
   * @param tupleHandler
   *          the HBaseRowToKettleTuple to delegate to
   * @param outputRowMeta
   *          the outgoing row meta
   * @return a list of kettle rows
   * @throws KettleException
   *           if a problem occurs
   */
  public static List<Object[]> getTupleOutputRows( HBaseService hBaseService, Result result, List<HBaseValueMetaInterface> userOutputColumns,
                                                   Map<String, HBaseValueMetaInterface> columnsMappedByAlias, Mapping tableMapping, HBaseRowToKettleTuple tupleHandler,
                                                   RowMetaInterface outputRowMeta ) throws KettleException {

    if ( userOutputColumns != null && userOutputColumns.size() > 0 ) {
      return tupleHandler.hbaseRowToKettleTupleMode( result, tableMapping, userOutputColumns, outputRowMeta );
    } else {
      return tupleHandler.hbaseRowToKettleTupleMode( hBaseService.getHBaseValueMetaInterfaceFactory(), result, tableMapping, columnsMappedByAlias, outputRowMeta );
    }
  }

  /**
   * Convert/decode the current hbase row into a kettle row
   * 
   * @param result
   *          the result to use
   * @param userOutputColumns
   *          user-specified subset of columns (if any) from the mapping
   * @param columnsMappedByAlias
   *          columns in the mapping keyed by alias
   * @param tableMapping
   *          the mapping to use
   * @param outputRowMeta
   *          the outgoing row meta
   * @return a kettle row
   * @throws KettleException
   *           if a problem occurs
   */
  public static Object[] getOutputRow( Result result, List<HBaseValueMetaInterface> userOutputColumns,
      Map<String, HBaseValueMetaInterface> columnsMappedByAlias, Mapping tableMapping, RowMetaInterface outputRowMeta ) throws KettleException {

    int size = ( userOutputColumns != null && userOutputColumns.size() > 0 ) ? userOutputColumns.size()
      : tableMapping.numMappedColumns() + 1; // + 1 for the key

    Object[] outputRowData = RowDataUtil.allocateRowData( size );

    // User-selected output columns?
    if ( userOutputColumns != null && userOutputColumns.size() > 0 ) {
      for ( HBaseValueMetaInterface currentCol : userOutputColumns ) {
        if ( currentCol.isKey() ) {
          byte[] rawKey = null;
          try {
            rawKey = result.getRow();
          } catch ( Exception e ) {
            throw new KettleException( e );
          }
          Object decodedKey = tableMapping.decodeKeyValue( rawKey );
          int keyIndex = outputRowMeta.indexOfValue( currentCol.getAlias() );
          outputRowData[keyIndex] = decodedKey;
        } else {
          String colFamilyName = currentCol.getColumnFamily();
          String qualifier = currentCol.getColumnName();

          boolean binaryColName = false;
          if ( qualifier.startsWith( "@@@binary@@@" ) ) {
            qualifier = qualifier.replace( "@@@binary@@@", "" );
            // assume hex encoded
            binaryColName = true;
          }

          byte[] kv = null;
          try {
            kv = result.getValue( colFamilyName, qualifier, binaryColName );
          } catch ( Exception e ) {
            throw new KettleException( e );
          }

          int outputIndex = outputRowMeta.indexOfValue( currentCol.getAlias() );
          if ( outputIndex < 0 ) {
            throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
                "HBaseInput.Error.ColumnNotDefinedInOutput", currentCol.getAlias() ) );
          }

          Object decodedVal = currentCol.decodeColumnValue( ( kv == null ) ? null : kv );

          outputRowData[outputIndex] = decodedVal;
        }
      }
    } else {
      // do the key first
      byte[] rawKey = null;
      try {
        rawKey = result.getRow();
      } catch ( Exception e ) {
        throw new KettleException( e );
      }

      Object decodedKey = tableMapping.decodeKeyValue( rawKey );
      int keyIndex = outputRowMeta.indexOfValue( tableMapping.getKeyName() );
      outputRowData[keyIndex] = decodedKey;

      Set<String> aliasSet = columnsMappedByAlias.keySet();

      for ( String name : aliasSet ) {
        HBaseValueMetaInterface currentCol = columnsMappedByAlias.get( name );
        String colFamilyName = currentCol.getColumnFamily();
        String qualifier = currentCol.getColumnName();
        if ( currentCol.isKey() ) {
          // skip key as it has already been processed 
          // and is not in the scan's columns 
          continue;
        }

        boolean binaryColName = false;
        if ( qualifier.startsWith( "@@@binary@@@" ) ) {
          qualifier = qualifier.replace( "@@@binary@@@", "" );
          // assume hex encoded
          binaryColName = true;
        }

        byte[] kv = null;
        try {
          kv = result.getValue( colFamilyName, qualifier, binaryColName );
        } catch ( Exception e ) {
          throw new KettleException( e );
        }

        int outputIndex = outputRowMeta.indexOfValue( name );
        if ( outputIndex < 0 ) {
          throw new KettleException( BaseMessages.getString( HBaseInputMeta.PKG,
              "HBaseInput.Error.ColumnNotDefinedInOutput", name ) );
        }

        Object decodedVal = currentCol.decodeColumnValue( ( kv == null ) ? null : kv );

        outputRowData[outputIndex] = decodedVal;
      }
    }

    return outputRowData;
  }
}
