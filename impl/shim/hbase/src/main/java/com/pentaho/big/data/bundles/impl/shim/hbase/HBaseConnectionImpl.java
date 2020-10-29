/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.table.HBaseTableImpl;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bryan on 1/21/16.
 */
public class HBaseConnectionImpl implements HBaseConnection {
  private final HBaseServiceImpl hBaseService;
  private final HBaseConnectionPool hBaseConnectionPool;
  private final HBaseBytesUtilShim hBaseBytesUtilShim;

  public HBaseConnectionImpl( HBaseServiceImpl hBaseService, HBaseShim hBaseShim, HBaseBytesUtilShim hBaseBytesUtilShim,
                              Properties connectionProps, LogChannelInterface logChannelInterface ) throws IOException {
    this( hBaseService, hBaseBytesUtilShim, new HBaseConnectionPool( hBaseShim, connectionProps, logChannelInterface ) );
  }

  public HBaseConnectionImpl( HBaseServiceImpl hBaseService, HBaseBytesUtilShim hBaseBytesUtilShim,
                              HBaseConnectionPool hBaseConnectionPool ) {
    this.hBaseService = hBaseService;
    this.hBaseBytesUtilShim = hBaseBytesUtilShim;
    this.hBaseConnectionPool = hBaseConnectionPool;
  }

  @Override public HBaseService getService() {
    return hBaseService;
  }

  @Override public HBaseTableImpl getTable( String tableName ) throws IOException {
    return new HBaseTableImpl( hBaseConnectionPool, hBaseService.getHBaseValueMetaInterfaceFactory(),
      hBaseBytesUtilShim, tableName );
  }

  @Override public void checkHBaseAvailable() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      hBaseConnectionHandle.getConnection().checkHBaseAvailable();
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }

  @Override public List<String> listTableNames() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listTableNames();
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }

  @Override public void close() throws IOException {
    hBaseConnectionPool.close();
  }

  @Override public List<String> listNamespaces() throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listNamespaces();
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }

  @Override public List<String> listTableNamesByNamespace( String namespace ) throws IOException {
    try ( HBaseConnectionHandle hBaseConnectionHandle = hBaseConnectionPool.getConnectionHandle() ) {
      return hBaseConnectionHandle.getConnection().listTableNamesByNamespace( namespace );
    } catch ( Exception e ) {
      throw IOExceptionUtil.wrapIfNecessary( e );
    }
  }
}
