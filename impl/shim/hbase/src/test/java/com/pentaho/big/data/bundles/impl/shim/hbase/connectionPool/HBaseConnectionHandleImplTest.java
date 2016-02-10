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

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by bryan on 2/4/16.
 */
public class HBaseConnectionHandleImplTest {
  private HBaseConnectionPool hBaseConnectionPool;
  private HBaseConnectionPoolConnection hBaseConnection;
  private HBaseConnectionHandleImpl hBaseConnectionHandle;

  @Before
  public void setup() {
    hBaseConnectionPool = mock( HBaseConnectionPool.class );
    hBaseConnection = mock( HBaseConnectionPoolConnection.class );
    hBaseConnectionHandle = new HBaseConnectionHandleImpl( hBaseConnectionPool, hBaseConnection );
  }

  @Test
  public void testGetConnection() {
    assertEquals( hBaseConnection, hBaseConnectionHandle.getConnection() );
  }

  @Test
  public void testClose() throws IOException {
    hBaseConnectionHandle.close();
    assertNull( hBaseConnectionHandle.getConnection() );
    verify( hBaseConnectionPool ).releaseConnection( hBaseConnection );
  }
}
