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

package com.pentaho.big.data.bundles.impl.shim.hbase;

import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPoolConnection;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/3/16.
 */
public class HBaseConnectionImplTest {
  private HBaseServiceImpl hBaseService;
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private HBaseConnectionImpl hBaseConnection;
  private HBaseConnectionPool hBaseConnectionPool;
  private HBaseConnectionHandle hBaseConnectionHandle;
  private HBaseConnectionPoolConnection poolConnection;
  private IOException ioException;
  private Exception exception;

  @Before
  public void setup() throws IOException {
    exception = new Exception();
    ioException = new IOException();
    hBaseService = mock( HBaseServiceImpl.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    hBaseConnectionPool = mock( HBaseConnectionPool.class );
    hBaseConnectionHandle = mock( HBaseConnectionHandle.class );
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    poolConnection = mock( HBaseConnectionPoolConnection.class );
    when( hBaseConnectionHandle.getConnection() ).thenReturn( poolConnection );
    hBaseConnection =
      new HBaseConnectionImpl( hBaseService, hBaseBytesUtilShim, hBaseConnectionPool );
  }

  @Test
  public void testGetService() {
    assertEquals( hBaseService, hBaseConnection.getService() );
  }

  @Test
  public void testGetTable() throws IOException {
    assertNotNull( hBaseConnection.getTable( "tableName" ) );
  }

  @Test
  public void testCheckHBaseAvailable() throws Exception {
    hBaseConnection.checkHBaseAvailable();
    verify( poolConnection ).checkHBaseAvailable();
  }

  @Test( expected = IOException.class )
  public void testCheckHBaseAvailableSuppressedException() throws Exception {
    doThrow( exception ).when( poolConnection ).checkHBaseAvailable();
    doThrow( ioException ).when( hBaseConnectionHandle ).close();
    try {
      hBaseConnection.checkHBaseAvailable();
    } catch ( IOException e ) {
      checkSuppressedIOE( e );
    }
  }

  @Test( expected = IOException.class )
  public void testCheckHBaseAvailableCloseException() throws Exception {
    doThrow( ioException ).when( hBaseConnectionHandle ).close();
    try {
      hBaseConnection.checkHBaseAvailable();
    } catch ( IOException e ) {
      assertEquals( ioException, e );
      throw e;
    }
  }

  @Test
  public void testListTableNames() throws Exception {
    List<String> list = new ArrayList<>( Arrays.asList( "a", "b", "c" ) );
    when( poolConnection.listTableNames() ).thenReturn( list );
    assertEquals( list, hBaseConnection.listTableNames() );
  }

  @Test( expected = IOException.class )
  public void testListTableNamesSuppressedException() throws Exception {
    when( poolConnection.listTableNames() ).thenThrow( exception );
    doThrow( ioException ).when( hBaseConnectionHandle ).close();
    try {
      hBaseConnection.listTableNames();
    } catch ( IOException e ) {
      checkSuppressedIOE( e );
    }
  }

  @Test( expected = IOException.class )
  public void testListTableNamesCloseException() throws Exception {
    doThrow( ioException ).when( hBaseConnectionHandle ).close();
    try {
      hBaseConnection.listTableNames();
    } catch ( IOException e ) {
      assertEquals( ioException, e );
      throw e;
    }
  }

  private void checkSuppressedIOE( IOException e ) throws IOException {
    Throwable cause = e.getCause();
    assertEquals( exception, cause );
    Throwable[] suppressed = cause.getSuppressed();
    assertEquals( 1, suppressed.length );
    assertEquals( ioException, suppressed[0] );
    throw e;
  }

  @Test
  public void testClose() throws Exception {
    hBaseConnection.close();
    verify( hBaseConnectionPool ).close();
  }
}
