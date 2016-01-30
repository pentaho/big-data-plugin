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

package com.pentaho.big.data.bundles.impl.shim.hbase.table;

import com.pentaho.big.data.bundles.impl.shim.hbase.BatchHBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionOperation;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceImpl;
import org.pentaho.bigdata.api.hbase.mapping.ColumnFilter;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.bigdata.api.hbase.table.ResultScanner;
import org.pentaho.bigdata.api.hbase.table.ResultScannerBuilder;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerBuilderImpl implements ResultScannerBuilder {
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;
  private final BatchHBaseConnectionOperation batchHBaseConnectionOperation;
  private int caching = 0;
  private String tableName;

  public ResultScannerBuilderImpl( HBaseConnectionPool hBaseConnectionPool,
                                   HBaseValueMetaInterfaceFactoryImpl hBaseValueMetaInterfaceFactory,
                                   HBaseBytesUtilShim hBaseBytesUtilShim, String tableName,
                                   final byte[] keyLowerBound,
                                   final byte[] keyUpperBound ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseValueMetaInterfaceFactory = hBaseValueMetaInterfaceFactory;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.batchHBaseConnectionOperation = new BatchHBaseConnectionOperation();
    this.tableName = tableName;
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.newSourceTableScan( keyLowerBound, keyUpperBound, caching );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override
  public void addColumnToScan( final String colFamilyName, final String colName, final boolean colNameIsBinary )
    throws IOException {
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper.addColumnToScan( colFamilyName, colName, colNameIsBinary );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void addColumnFilterToScan( ColumnFilter cf, HBaseValueMetaInterface columnMeta,
                                               final VariableSpace vars,
                                               final boolean matchAny ) throws IOException {
    final org.pentaho.hbase.shim.api.ColumnFilter columnFilter =
      new org.pentaho.hbase.shim.api.ColumnFilter( cf.getFieldAlias() );
    columnFilter.setFormat( cf.getFormat() );
    columnFilter.setConstant( cf.getConstant() );
    columnFilter.setSignedComparison( cf.getSignedComparison() );
    columnFilter.setFieldType( cf.getFieldType() );
    columnFilter.setComparisonOperator(
      org.pentaho.hbase.shim.api.ColumnFilter.ComparisonType.valueOf( cf.getComparisonOperator().name() ) );
    final HBaseValueMetaInterfaceImpl hBaseValueMetaInterface = hBaseValueMetaInterfaceFactory.copy( columnMeta );
    batchHBaseConnectionOperation.addOperation( new HBaseConnectionOperation() {
      @Override public void perform( HBaseConnectionWrapper hBaseConnectionWrapper ) throws IOException {
        try {
          hBaseConnectionWrapper
            .addColumnFilterToScan( columnFilter, hBaseValueMetaInterface, vars, matchAny );
        } catch ( Exception e ) {
          throw new IOException( e );
        }
      }
    } );
  }

  @Override public void setCaching( int cacheSize ) {
    this.caching = cacheSize;
  }

  @Override public ResultScanner build() throws IOException {
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( tableName );
    batchHBaseConnectionOperation.perform( connectionHandle.getConnection() );
    try {
      connectionHandle.getConnection().executeSourceTableScan();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
    return new ResultScannerImpl( connectionHandle, hBaseBytesUtilShim );
  }
}
