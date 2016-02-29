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
import org.junit.Before;
import org.junit.Test;
import org.pentaho.hbase.shim.api.HBaseValueMeta;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/29/16.
 */
public class HBasePutImplTest {
  private byte[] testKey;
  private HBaseConnectionHandle hBaseConnectionHandle;
  private HBasePutImpl hBasePut;
  private HBaseConnectionWrapper hBaseConnectionWrapper;

  @Before
  public void setup() {
    testKey = "testKey".getBytes( Charset.forName( "UTF-8" ) );
    hBaseConnectionHandle = mock( HBaseConnectionHandle.class );
    hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    when( hBaseConnectionHandle.getConnection() ).thenReturn( hBaseConnectionWrapper );
    hBasePut = new HBasePutImpl( testKey, hBaseConnectionHandle );
  }

  @Test
  public void testSetWriteToWalFalse() throws Exception {
    hBasePut.execute();
    verify( hBaseConnectionWrapper ).newTargetTablePut( testKey, false );
    verify( hBaseConnectionWrapper ).executeTargetTablePut();
  }

  @Test
  public void testSetWriteToWalTrue() throws Exception {
    hBasePut.setWriteToWAL( true );
    hBasePut.execute();
    verify( hBaseConnectionWrapper ).newTargetTablePut( testKey, true );
    verify( hBaseConnectionWrapper ).executeTargetTablePut();
  }

  @Test
  public void testFullPut() throws Exception {
    String testFamily = "testFamily";
    String testName = "testName";
    boolean colNameIsBinary = true;
    byte[] colValue = "testColVal".getBytes( Charset.forName( "UTF-8" ) );
    hBasePut.addColumn( testFamily, testName, colNameIsBinary, colValue );
    hBasePut.execute();
    verify( hBaseConnectionWrapper ).newTargetTablePut( testKey, false );
    verify( hBaseConnectionWrapper ).addColumnToTargetPut( testFamily, testName, colNameIsBinary, colValue );
    verify( hBaseConnectionWrapper ).executeTargetTablePut();
  }

  @Test( expected = IOException.class )
  public void testNewTargetPutException() throws Exception {
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnectionWrapper ).newTargetTablePut( testKey, false );
    try {
      hBasePut.execute();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test( expected = IOException.class )
  public void testAddColumnException() throws Exception {
    String testFamily = "testFamily";
    String testName = "testName";
    boolean colNameIsBinary = true;
    byte[] colValue = "testColVal".getBytes( Charset.forName( "UTF-8" ) );
    hBasePut.addColumn( testFamily, testName, colNameIsBinary, colValue );

    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnectionWrapper )
      .addColumnToTargetPut( testFamily, testName, colNameIsBinary, colValue );

    try {
      hBasePut.execute();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test( expected = IOException.class )
  public void testExecuteTargetTablePutException() throws Exception {
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnectionWrapper ).executeTargetTablePut();
    try {
      hBasePut.execute();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testCreateColumnName() {
    String partA = "partA";
    String partB = "partB";
    assertEquals( partA + HBaseValueMeta.SEPARATOR + partB, hBasePut.createColumnName( partA, partB ) );
    assertEquals( "", hBasePut.createColumnName() );
  }
}
