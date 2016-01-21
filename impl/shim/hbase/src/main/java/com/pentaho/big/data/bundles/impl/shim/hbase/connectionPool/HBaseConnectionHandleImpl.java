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

package com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool;

import java.io.IOException;

/**
 * Created by bryan on 1/25/16.
 */
public class HBaseConnectionHandleImpl implements HBaseConnectionHandle {
  private final HBaseConnectionPool hBaseConnectionPool;
  private HBaseConnectionPoolConnection hBaseConnection;

  public HBaseConnectionHandleImpl( HBaseConnectionPool hBaseConnectionPool, HBaseConnectionPoolConnection hBaseConnection ) {
    this.hBaseConnectionPool = hBaseConnectionPool;
    this.hBaseConnection = hBaseConnection;
  }

  @Override public HBaseConnectionPoolConnection getConnection() {
    return hBaseConnection;
  }

  @Override public void close() throws IOException {
    HBaseConnectionPoolConnection hBaseConnection = this.hBaseConnection;
    this.hBaseConnection = null;
    hBaseConnectionPool.releaseConnection( hBaseConnection );
  }
}
