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

import java.io.IOException;

/**
 * Created by bryan on 1/26/16.
 */
public class HBaseDeleteImpl implements HBaseDelete {
  private final HBaseConnectionHandle hBaseConnectionHandle;
  private final byte[] key;

  public HBaseDeleteImpl( HBaseConnectionHandle hBaseConnectionHandle, byte[] key ) {
    this.hBaseConnectionHandle = hBaseConnectionHandle;
    this.key = key;
  }

  @Override public void execute() throws IOException {
    try {
      hBaseConnectionHandle.getConnection().executeTargetTableDelete( key );
    } catch ( Exception e ) {
      throw new IOException( e );
    }
  }
}
