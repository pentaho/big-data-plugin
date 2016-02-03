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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;
import org.pentaho.hbase.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/3/16.
 */
public class HBaseConnectionImplTest {
  private HBaseServiceImpl hBaseService;
  private HBaseConnection shimHBaseConnection;
  private HBaseShim hBaseShim;
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private Properties connectionProps;
  private LogChannelInterface logChannelInterface;
  private HBaseConnectionImpl hBaseConnection;

  @Before
  public void setup() throws IOException {
    hBaseService = mock( HBaseServiceImpl.class );
    hBaseShim = mock( HBaseShim.class );
    shimHBaseConnection = mock( HBaseConnectionWithResultField.class );
    when( hBaseShim.getHBaseConnection() ).thenReturn( shimHBaseConnection );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    connectionProps = mock( Properties.class );
    logChannelInterface = mock( LogChannelInterface.class );
    hBaseConnection =
      new HBaseConnectionImpl( hBaseService, hBaseShim, hBaseBytesUtilShim, connectionProps, logChannelInterface );
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
    verify( shimHBaseConnection ).checkHBaseAvailable();
  }

  @Test( expected = IOException.class )
  public void testCheckHBaseAvailableException() throws Exception {
    Exception thrown = new Exception();
    doThrow( thrown ).when( shimHBaseConnection ).checkHBaseAvailable();
    try {
      hBaseConnection.checkHBaseAvailable();
    } catch ( IOException e ) {
      assertEquals( thrown, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testListTableNames() throws Exception {
    List<String> list = new ArrayList<>( Arrays.asList( "a", "b", "c" ) );
    when( shimHBaseConnection.listTableNames() ).thenReturn( list );
    assertEquals( list, hBaseConnection.listTableNames() );
  }

  @Test( expected = IOException.class )
  public void testListTableNamesException() throws Exception {
    Exception exception = new Exception();
    when( shimHBaseConnection.listTableNames() ).thenThrow( exception );
    try {
      hBaseConnection.listTableNames();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testClose() throws Exception {
    hBaseConnection.listTableNames();
    hBaseConnection.close();
    verify( shimHBaseConnection ).closeSourceTable();
    verify( shimHBaseConnection ).closeTargetTable();
  }
}
