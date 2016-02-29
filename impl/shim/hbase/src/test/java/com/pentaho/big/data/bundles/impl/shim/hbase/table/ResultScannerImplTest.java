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
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionHandle;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/29/16.
 */
public class ResultScannerImplTest {
  private HBaseConnectionHandle hBaseConnectionHandle;
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private HBaseConnectionWrapper hBaseConnectionWrapper;
  private ResultScannerImpl resultScanner;

  @Before
  public void setup() {
    hBaseConnectionHandle = mock( HBaseConnectionHandle.class );
    hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );

    when( hBaseConnectionHandle.getConnection() ).thenReturn( hBaseConnectionWrapper );
    resultScanner = new ResultScannerImpl( hBaseConnectionHandle, hBaseBytesUtilShim );
  }

  @Test
  public void testNextSuccess() throws Exception {
    Result result = mock( Result.class );
    byte[] testRow = "testRow".getBytes( Charset.forName( "UTF-8" ) );
    when( result.getRow() ).thenReturn( testRow );
    when( hBaseConnectionWrapper.resultSetNextRow() ).thenReturn( true ).thenReturn( false );
    when( hBaseConnectionWrapper.getCurrentResult() ).thenReturn( result )
      .thenThrow( new IllegalStateException( "Only expected one call" ) );
    assertArrayEquals( testRow, resultScanner.next().getRow() );
    assertNull( resultScanner.next() );
  }

  @Test( expected = IOException.class )
  public void testNextException() throws Exception {
    Exception exception = new Exception();
    when( hBaseConnectionWrapper.resultSetNextRow() ).thenThrow( exception );
    try {
      resultScanner.next();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testClose() throws IOException {
    resultScanner.close();
    verify( hBaseConnectionHandle ).close();
  }
}
