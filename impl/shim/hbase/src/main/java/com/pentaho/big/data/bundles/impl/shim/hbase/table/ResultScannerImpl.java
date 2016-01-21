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

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import com.pentaho.big.data.bundles.impl.shim.hbase.ResultImpl;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.bigdata.api.hbase.Result;
import org.pentaho.bigdata.api.hbase.table.ResultScanner;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class ResultScannerImpl implements ResultScanner {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final HBaseConnectionWrapper hBaseConnectionWrapper;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public ResultScannerImpl( HBaseConnectionHandle hBaseConnectionHandle, HBaseBytesUtilShim hBaseBytesUtilShim ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    hBaseConnectionWrapper = hBaseConnectionHandle.getConnection();
  }

  @Override public Result next() throws IOException {
    try {
      if ( !hBaseConnectionWrapper.resultSetNextRow() ) {
        return null;
      }
      return new ResultImpl( hBaseConnectionWrapper.getCurrentResult(), hBaseBytesUtilShim );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionHandle.close();
  }
}
