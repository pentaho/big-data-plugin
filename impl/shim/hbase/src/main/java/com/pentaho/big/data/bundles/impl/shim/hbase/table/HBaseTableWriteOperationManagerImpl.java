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

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.pentaho.bigdata.api.hbase.table.HBaseDelete;
import org.pentaho.bigdata.api.hbase.table.HBasePut;
import org.pentaho.bigdata.api.hbase.table.HBaseTableWriteOperationManager;

import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public class HBaseTableWriteOperationManagerImpl implements HBaseTableWriteOperationManager {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final boolean autoFlush;

  public HBaseTableWriteOperationManagerImpl( HBaseConnectionHandle hBaseConnectionHandle, boolean autoFlush ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.autoFlush = autoFlush;
  }

  @Override public boolean isAutoFlush() {
    return autoFlush;
  }

  @Override public HBasePut createPut( byte[] key ) {
    return new HBasePutImpl( key, hBaseConnectionHandle );
  }

  @Override public HBaseDelete createDelete( byte[] key ) {
    return new HBaseDeleteImpl( hBaseConnectionHandle, key );
  }

  @Override public void flushCommits() throws IOException {
    try {
      hBaseConnectionHandle.getConnection().flushCommitsTargetTable();
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionHandle.close();
  }
}
