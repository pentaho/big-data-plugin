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

import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionTestImpls;
import com.pentaho.big.data.bundles.impl.shim.hbase.HBaseConnectionWrapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.hbase.shim.spi.HBaseConnection;
import org.pentaho.hbase.shim.spi.HBaseShim;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/4/16.
 */
public class HBaseConnectionPoolTest {
  private HBaseShim hBaseShim;
  private Properties props;
  private LogChannelInterface logChannelInterface;
  private HBaseConnectionPool hBaseConnectionPool;
  private List<HBaseConnectionTestImpls.HBaseConnectionWithResultField> mockConnections;

  @Before
  public void setup() {
    hBaseShim = mock( HBaseShim.class );
    mockConnections = new ArrayList<>();
    when( hBaseShim.getHBaseConnection() ).thenAnswer( new Answer<HBaseConnection>() {
      @Override public HBaseConnection answer( InvocationOnMock invocation ) throws Throwable {
        HBaseConnectionTestImpls.HBaseConnectionWithResultField connectionWithResultField =
          mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
        mockConnections.add( connectionWithResultField );
        return connectionWithResultField;
      }
    } );
    props = mock( Properties.class );
    logChannelInterface = mock( LogChannelInterface.class );
    hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, props, logChannelInterface );
  }

  @Test
  public void testGetConnectionHandleNoArgReuse() throws IOException {
    HBaseConnectionHandle handle1 = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionWrapper handle1Connection = handle1.getConnection();
    handle1.close();
    HBaseConnectionHandle handle2 = hBaseConnectionPool.getConnectionHandle();
    assertNotEquals( handle1, handle2 );
    assertEquals( handle1Connection, handle2.getConnection() );
  }

  @Test
  public void testGetConnectionHandleNoArgPrefersEmptySourceTarget() throws IOException {
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    List<Closeable> toClose = new ArrayList<>();
    toClose.add( connectionHandle );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle().getConnection() );
  }

  @Test
  public void testGetConnectionHandleNoArgPrefersEmptyTarget() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( "source" );
    toClose.add( connectionHandle );
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle().getConnection() );
  }

  @Test
  public void testGetConnectionHandleNoArgPrefersEmptySource() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    HBaseConnectionHandle connectionHandle =
      hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) );
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    toClose.clear();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    connectionHandle.close();
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle().getConnection() );
  }

  @Test
  public void testGetConnectionHandleNoArgPicksArbitraryIfNonePreferable() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    toClose.clear();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertNotNull( hBaseConnectionPool.getConnectionHandle() );
  }

  @Test
  public void testGetConnectionSourceTablePrefersNullSourceIfArgNull() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    toClose.add( connectionHandle );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle( null ).getConnection() );
  }

  @Test
  public void testGetConnectionSourceTablePrefersNullSourceIfNoneMatch() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    toClose.add( connectionHandle );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle( "source2" ).getConnection() );
  }

  @Test
  public void testGetConnectionSourceTablePrefersMatchingTable() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    String source2 = "source2";
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( source2 );
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    toClose.add( connectionHandle );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle( source2 ).getConnection() );
  }

  @Test
  public void testGetConnectionHandleSourceTablePicksArbitraryIfNonePreferable() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source" ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertNotNull( hBaseConnectionPool.getConnectionHandle( "source2" ) );
  }

  @Test
  public void testGetConnectionTargetPrefersNullSourceIfArgNull() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    toClose.add( connectionHandle );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle( null, null ).getConnection() );
  }

  @Test
  public void testGetConnectionTargetTablePrefersNullSourceIfNoneMatch() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    toClose.add( connectionHandle );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "target", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertEquals( connection,
      hBaseConnectionPool.getConnectionHandle( "target2", mock( Properties.class ) ).getConnection() );
  }

  @Test
  public void testGetConnectionTargetPrefersMatchingTable() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    String target = "target";
    String target2 = "target2";
    Properties targetTableProps = mock( Properties.class );
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( target2, targetTableProps );
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    HBaseConnectionHandle connectionHandle2 = hBaseConnectionPool.getConnectionHandle( target, null );
    HBaseConnectionWrapper connection2 = connectionHandle2.getConnection();
    toClose.add( connectionHandle );
    toClose.add( connectionHandle2 );
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( target, targetTableProps ) );
    }
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( target2, mock( Properties.class ) ) );
    }
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( target2, null ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    HBaseConnectionHandle connectionHandle3 = hBaseConnectionPool.getConnectionHandle( target2, targetTableProps );
    assertEquals( connection, connectionHandle3.getConnection() );
    connectionHandle3.close();
    assertEquals( connection2, hBaseConnectionPool.getConnectionHandle( target, null ).getConnection() );
  }

  @Test
  public void testGetConnectionHandleTargetTablePicksArbitraryIfNonePreferable() throws IOException {
    List<Closeable> toClose = new ArrayList<>();
    for ( int i = 0; i < 100; i++ ) {
      toClose.add( hBaseConnectionPool.getConnectionHandle( "source", mock( Properties.class ) ) );
    }
    for ( Closeable closeable : toClose ) {
      closeable.close();
    }
    assertNotNull( hBaseConnectionPool.getConnectionHandle( "source2", mock( Properties.class ) ) );
  }

  @Test
  public void testGetConnectionHandleTargetTableNonNullProps() throws IOException {
    String table = "table";
    Properties properties = mock( Properties.class );
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( table, properties );
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    connectionHandle.close();
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle( table, null ).getConnection() );
  }

  @Test
  public void testGetConnectionHandleTargetTableNullProps() throws IOException {
    String table = "table";
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle( table, null );
    HBaseConnectionWrapper connection = connectionHandle.getConnection();
    connectionHandle.close();
    assertEquals( connection, hBaseConnectionPool.getConnectionHandle( table, null ).getConnection() );
  }

  @Test
  public void testConfigMessage() throws Exception {
    hBaseShim = mock( HBaseShim.class );
    HBaseConnection hBaseConnection = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, props, logChannelInterface );
    final String message = "message";
    doAnswer( new Answer<Void>() {
      @Override public Void answer( InvocationOnMock invocation ) throws Throwable {
        ( (List<String>) invocation.getArguments()[ 1 ] ).add( message );
        return null;
      }
    } ).when( hBaseConnection ).configureConnection( eq( props ), anyList() );
    hBaseConnectionPool.getConnectionHandle();
    verify( logChannelInterface ).logBasic( message );
    hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, props, null );
    hBaseConnectionPool.getConnectionHandle();
  }

  @Test( expected = IOException.class )
  public void testConfigError() throws Exception {
    hBaseShim = mock( HBaseShim.class );
    HBaseConnection hBaseConnection = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, props, logChannelInterface );
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnection ).configureConnection( eq( props ), anyList() );
    try {
      hBaseConnectionPool.getConnectionHandle();
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test( expected = IOException.class )
  public void testCantSetSourceTable() throws Exception {
    String table = "table";
    hBaseShim = mock( HBaseShim.class );
    HBaseConnection hBaseConnection = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, props, logChannelInterface );
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnection ).newSourceTable( table );
    try {
      hBaseConnectionPool.getConnectionHandle( table );
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test( expected = IOException.class )
  public void testCantSetTargetTable() throws Exception {
    String table = "table";
    Properties properties = mock( Properties.class );
    hBaseShim = mock( HBaseShim.class );
    HBaseConnection hBaseConnection = mock( HBaseConnectionTestImpls.HBaseConnectionWithResultField.class );
    when( hBaseShim.getHBaseConnection() ).thenReturn( hBaseConnection );
    hBaseConnectionPool = new HBaseConnectionPool( hBaseShim, props, logChannelInterface );
    Exception exception = new Exception();
    doThrow( exception ).when( hBaseConnection ).newTargetTable( table, properties );
    try {
      hBaseConnectionPool.getConnectionHandle( table, properties );
    } catch ( IOException e ) {
      assertEquals( exception, e.getCause() );
      throw e;
    }
  }

  @Test
  public void testClose() throws Exception {
    HBaseConnectionHandle connectionHandle = hBaseConnectionPool.getConnectionHandle();
    HBaseConnectionHandle connectionHandle2 = hBaseConnectionPool.getConnectionHandle();
    hBaseConnectionPool.getConnectionHandle();
    hBaseConnectionPool.getConnectionHandle().close();
    connectionHandle2.close();
    assertEquals( 4, mockConnections.size() );
    for ( HBaseConnectionTestImpls.HBaseConnectionWithResultField mockConnection : mockConnections ) {
      verify( mockConnection, never() ).close();
    }
    String e1Msg = "e1Msg";
    IOException exception1 = new IOException( e1Msg );
    doThrow( exception1 ).when( mockConnections.get( 0 ) ).close();
    String e2Msg = "e2Msg";
    IOException exception2 = new IOException( e2Msg );
    doThrow( exception2 ).when( mockConnections.get( 1 ) ).close();
    hBaseConnectionPool.close();
    for ( HBaseConnectionTestImpls.HBaseConnectionWithResultField mockConnection : mockConnections ) {
      verify( mockConnection ).close();
    }
    verify( logChannelInterface ).logError( e1Msg, exception1 );
    verify( logChannelInterface ).logError( e2Msg, exception2 );
  }
}
