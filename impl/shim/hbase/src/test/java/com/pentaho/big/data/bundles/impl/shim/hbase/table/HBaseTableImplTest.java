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
import com.pentaho.big.data.bundles.impl.shim.hbase.connectionPool.HBaseConnectionPool;
import com.pentaho.big.data.bundles.impl.shim.hbase.meta.HBaseValueMetaInterfaceFactoryImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hbase.shim.spi.HBaseBytesUtilShim;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 2/26/16.
 */
public class HBaseTableImplTest {
  private final byte[] testBytes = "testbytes".getBytes( Charset.forName( "UTF-8" ) );
  private HBaseConnectionPool hBaseConnectionPool;
  private HBaseValueMetaInterfaceFactoryImpl
    hBaseValueMetaInterfaceFactory;
  private HBaseBytesUtilShim hBaseBytesUtilShim;
  private String testName;
  private HBaseTableImpl hBaseTable;
  private final IOERunnable existsRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.exists();
    }
  };
  private final IOERunnable disabledRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.disabled();
    }
  };
  private final IOERunnable availableRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.available();
    }
  };
  private final IOERunnable disableRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.disable();
    }
  };
  private final IOERunnable enableRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.enable();
    }
  };
  private final IOERunnable deleteRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.delete();
    }
  };
  private final IOERunnable createRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.create( new ArrayList<String>(), new Properties() );
    }
  };
  private final IOERunnable getColumnFamiliesRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.getColumnFamilies();
    }
  };
  private final IOERunnable keyExistsRunnable = new IOERunnable() {
    @Override public void run() throws IOException {
      hBaseTable.keyExists( testBytes );
    }
  };
  private HBaseConnectionHandle hBaseConnectionHandle;
  private HBaseConnectionHandle hBaseConnectionHandleNamed;
  private HBaseConnectionWrapper hBaseConnectionWrapper;
  private Mapping tableMapping;
  private LogChannelInterface logChannelInterface;
  private VariableSpace variableSpace;

  @Before
  public void setup() {
    hBaseConnectionPool = mock( HBaseConnectionPool.class );
    hBaseValueMetaInterfaceFactory = mock( HBaseValueMetaInterfaceFactoryImpl.class );
    hBaseBytesUtilShim = mock( HBaseBytesUtilShim.class );
    testName = "testName";
    hBaseTable =
      new HBaseTableImpl( hBaseConnectionPool, hBaseValueMetaInterfaceFactory, hBaseBytesUtilShim, testName );
    hBaseConnectionHandle = mock( HBaseConnectionHandle.class );
    hBaseConnectionHandleNamed = mock( HBaseConnectionHandle.class );
    hBaseConnectionWrapper = mock( HBaseConnectionWrapper.class );
    tableMapping = mock( Mapping.class );
    logChannelInterface = mock( LogChannelInterface.class );
    variableSpace = mock( VariableSpace.class );

    when( hBaseConnectionHandle.getConnection() ).thenReturn( hBaseConnectionWrapper );
    when( hBaseConnectionHandleNamed.getConnection() ).thenReturn( hBaseConnectionWrapper );
  }

  @Test
  public void testExistsSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    when( hBaseConnectionWrapper.tableExists( testName ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseTable.exists() );
    assertFalse( hBaseTable.exists() );
  }

  @Test( expected = IOException.class )
  public void testExistsErrorGettingHandle() throws IOException {
    testIOEGettingHandle( existsRunnable );
  }

  @Test( expected = IOException.class )
  public void testExistsErrorClosingHandle() throws IOException {
    testIOEClosingHandle( existsRunnable );
  }

  @Test( expected = IOException.class )
  public void testExistsHandleClosedWhenException() throws Exception {
    testEStillClosesHandle( when( hBaseConnectionWrapper.tableExists( testName ) ), existsRunnable );
  }

  @Test
  public void testDisabledSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    when( hBaseConnectionWrapper.isTableDisabled( testName ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseTable.disabled() );
    assertFalse( hBaseTable.disabled() );
  }

  @Test( expected = IOException.class )
  public void testDisabledErrorGettingHandle() throws IOException {
    testIOEGettingHandle( disabledRunnable );
  }

  @Test( expected = IOException.class )
  public void testDisabledErrorClosingHandle() throws IOException {
    testIOEClosingHandle( disabledRunnable );
  }

  @Test( expected = IOException.class )
  public void testDisabledHandleClosedWhenException() throws Exception {
    testEStillClosesHandle( when( hBaseConnectionWrapper.isTableDisabled( testName ) ), disabledRunnable );
  }

  @Test
  public void testAvailableSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    when( hBaseConnectionWrapper.isTableAvailable( testName ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseTable.available() );
    assertFalse( hBaseTable.available() );
  }

  @Test( expected = IOException.class )
  public void testAvailableErrorGettingHandle() throws IOException {
    testIOEGettingHandle( availableRunnable );
  }

  @Test( expected = IOException.class )
  public void testAvailableErrorClosingHandle() throws IOException {
    testIOEClosingHandle( availableRunnable );
  }

  @Test( expected = IOException.class )
  public void testAvailableHandleClosedWhenException() throws Exception {
    testEStillClosesHandle( when( hBaseConnectionWrapper.isTableAvailable( testName ) ), availableRunnable );
  }

  @Test
  public void testDisableSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    hBaseTable.disable();
    verify( hBaseConnectionWrapper ).disableTable( testName );
  }

  @Test( expected = IOException.class )
  public void testDisableErrorGettingHandle() throws IOException {
    testIOEGettingHandle( disableRunnable );
  }

  @Test( expected = IOException.class )
  public void testDisableErrorClosingHandle() throws IOException {
    testIOEClosingHandle( disableRunnable );
  }

  @Test( expected = IOException.class )
  public void testDisableHandleClosedWhenException() throws Exception {
    StubberReturn stubberReturn = testEStillClosesHandle( disableRunnable );
    doThrow( stubberReturn.exception ).when( hBaseConnectionWrapper ).disableTable( testName );
    stubberReturn.ioeRunnable.run();
  }

  @Test
  public void testEnableSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    hBaseTable.enable();
    verify( hBaseConnectionWrapper ).enableTable( testName );
  }

  @Test( expected = IOException.class )
  public void testEnableErrorGettingHandle() throws IOException {
    testIOEGettingHandle( enableRunnable );
  }

  @Test( expected = IOException.class )
  public void testEnableErrorClosingHandle() throws IOException {
    testIOEClosingHandle( enableRunnable );
  }

  @Test( expected = IOException.class )
  public void testEnableHandleClosedWhenException() throws Exception {
    StubberReturn stubberReturn = testEStillClosesHandle( enableRunnable );
    doThrow( stubberReturn.exception ).when( hBaseConnectionWrapper ).enableTable( testName );
    stubberReturn.ioeRunnable.run();
  }

  @Test
  public void testDeleteSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    hBaseTable.delete();
    verify( hBaseConnectionWrapper ).deleteTable( testName );
  }

  @Test( expected = IOException.class )
  public void testDeleteErrorGettingHandle() throws IOException {
    testIOEGettingHandle( deleteRunnable );
  }

  @Test( expected = IOException.class )
  public void testDeleteErrorClosingHandle() throws IOException {
    testIOEClosingHandle( deleteRunnable );
  }

  @Test( expected = IOException.class )
  public void testDeleteHandleClosedWhenException() throws Exception {
    StubberReturn stubberReturn = testEStillClosesHandle( deleteRunnable );
    doThrow( stubberReturn.exception ).when( hBaseConnectionWrapper ).deleteTable( testName );
    stubberReturn.ioeRunnable.run();
  }

  @Test
  public void testCreateSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    List list = mock( List.class );
    Properties props = mock( Properties.class );
    hBaseTable.create( list, props );
    verify( hBaseConnectionWrapper ).createTable( testName, list, props );
  }

  @Test( expected = IOException.class )
  public void testCreateErrorGettingHandle() throws IOException {
    testIOEGettingHandle( createRunnable );
  }

  @Test( expected = IOException.class )
  public void testCreateErrorClosingHandle() throws IOException {
    testIOEClosingHandle( createRunnable );
  }

  @Test( expected = IOException.class )
  public void testCreateHandleClosedWhenException() throws Exception {
    StubberReturn stubberReturn = testEStillClosesHandle( createRunnable );
    doThrow( stubberReturn.exception ).when( hBaseConnectionWrapper )
      .createTable( eq( testName ), eq( new ArrayList<String>() ), eq( new Properties() ) );
    stubberReturn.ioeRunnable.run();
  }

  @Test
  public void testGetColumnFamiliesSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    List families = mock( List.class );
    when( hBaseConnectionWrapper.getTableFamiles( testName ) ).thenReturn( families );
    assertEquals( families, hBaseTable.getColumnFamilies() );
  }

  @Test( expected = IOException.class )
  public void testGetColumnFamiliesErrorGettingHandle() throws IOException {
    testIOEGettingHandle( getColumnFamiliesRunnable );
  }

  @Test( expected = IOException.class )
  public void testGetColumnFamiliesErrorClosingHandle() throws IOException {
    testIOEClosingHandle( getColumnFamiliesRunnable );
  }

  @Test( expected = IOException.class )
  public void testGetColumnFamiliesHandleClosedWhenException() throws Exception {
    testEStillClosesHandle( when( hBaseConnectionWrapper.getTableFamiles( testName ) ), getColumnFamiliesRunnable );
  }

  @Test
  public void testKeyExistsSuccess() throws Exception {
    when( hBaseConnectionPool.getConnectionHandle( testName ) ).thenReturn( hBaseConnectionHandle );
    when( hBaseConnectionWrapper.sourceTableRowExists( testBytes ) ).thenReturn( true ).thenReturn( false );
    assertTrue( hBaseTable.keyExists( testBytes ) );
    assertFalse( hBaseTable.keyExists( testBytes ) );
  }

  @Test( expected = IOException.class )
  public void testKeyExistsErrorGettingHandle() throws IOException {
    testIOEGettingHandle( keyExistsRunnable, testName );
  }

  @Test( expected = IOException.class )
  public void testKeyExistsErrorClosingHandle() throws IOException {
    testIOEClosingHandle( keyExistsRunnable, testName );
  }

  @Test( expected = IOException.class )
  public void testKeyExistsHandleClosedWhenException() throws Exception {
    testEStillClosesHandle( testName, when( hBaseConnectionWrapper.sourceTableRowExists( testBytes ) ),
      keyExistsRunnable );
  }

  @Test
  public void testCreateWriteOperationManagerSuccessNullBufferSize() throws IOException {
    assertNotNull( hBaseTable.createWriteOperationManager( null ) );
    verify( hBaseConnectionPool ).getConnectionHandle( eq( testName ), eq( new Properties() ) );
  }

  @Test
  public void testCreateWriteOperationManagerSuccessNotNullBufferSize() throws IOException {
    Long writeBufferSize = 10L;
    assertNotNull( hBaseTable.createWriteOperationManager( writeBufferSize ) );
    Properties properties = new Properties();
    properties.setProperty( org.pentaho.hbase.shim.spi.HBaseConnection.HTABLE_WRITE_BUFFER_SIZE_KEY,
      writeBufferSize.toString() );
    verify( hBaseConnectionPool ).getConnectionHandle( eq( testName ), eq( properties ) );
  }

  @Test
  public void testClose() throws IOException {
    hBaseTable.close();
    verifyNoMoreInteractions( hBaseConnectionPool, hBaseValueMetaInterfaceFactory, hBaseBytesUtilShim );
  }

  @Test
  public void testCreateScannerBuilderKeyBounds() {
    assertTrue( hBaseTable.createScannerBuilder( new byte[] {}, new byte[] {} ) instanceof ResultScannerBuilderImpl );
  }

  @Test
  public void testCreateScannerBuilderFullArgsEmptyKeyStart() throws KettleException {
    when( tableMapping.getKeyType() ).thenReturn( Mapping.KeyType.LONG );
    String scannerCacheSizeVar = "scannerCacheSizeVar";
    String scannerCacheSize = "100";
    when( variableSpace.environmentSubstitute( scannerCacheSizeVar ) ).thenReturn( scannerCacheSize );
    assertNotNull( hBaseTable.createScannerBuilder( tableMapping, "testConvMask", null, null, scannerCacheSizeVar, logChannelInterface, variableSpace ) );
  }

  private void testIOEGettingHandle( IOERunnable runnable ) throws IOException {
    testIOEGettingHandle( runnable, when( hBaseConnectionPool.getConnectionHandle() ) );
  }

  private void testIOEGettingHandle( IOERunnable runnable, String name ) throws IOException {
    testIOEGettingHandle( runnable, when( hBaseConnectionPool.getConnectionHandle( name ) ) );
  }

  private void testIOEGettingHandle( IOERunnable runnable, OngoingStubbing<HBaseConnectionHandle> ongoingStubbing )
    throws IOException {
    IOException ioException = new IOException();
    ongoingStubbing.thenThrow( ioException );
    try {
      runnable.run();
    } catch ( IOException e ) {
      assertEquals( ioException, e.getCause() );
      throw e;
    }
  }

  private void testIOEClosingHandle( IOERunnable runnable ) throws IOException {
    testIOEClosingHandle( runnable, when( hBaseConnectionPool.getConnectionHandle() ) );
  }

  private void testIOEClosingHandle( IOERunnable runnable, String name ) throws IOException {
    testIOEClosingHandle( runnable, when( hBaseConnectionPool.getConnectionHandle( name ) ) );
  }

  private void testIOEClosingHandle( IOERunnable runnable, OngoingStubbing<HBaseConnectionHandle> ongoingStubbing )
    throws IOException {
    IOException ioException = new IOException();
    ongoingStubbing.thenReturn( hBaseConnectionHandle );
    doThrow( ioException ).when( hBaseConnectionHandle ).close();
    try {
      runnable.run();
    } catch ( IOException e ) {
      assertEquals( ioException, e.getCause() );
      throw e;
    }
  }

  private void testEStillClosesHandle( OngoingStubbing<?> ongoingStubbing, IOERunnable runnable ) throws IOException {
    Exception exception = new Exception();
    ongoingStubbing.thenThrow( exception );
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    new EStillClosesRunnable( runnable, exception ).run();
  }

  private void testEStillClosesHandle( String name, OngoingStubbing<?> ongoingStubbing, IOERunnable runnable )
    throws IOException {
    Exception exception = new Exception();
    ongoingStubbing.thenThrow( exception );
    when( hBaseConnectionPool.getConnectionHandle( name ) ).thenReturn( hBaseConnectionHandle );
    new EStillClosesRunnable( runnable, exception ).run();
  }

  private StubberReturn testEStillClosesHandle( final IOERunnable runnable ) throws IOException {
    final Exception exception = new Exception();
    when( hBaseConnectionPool.getConnectionHandle() ).thenReturn( hBaseConnectionHandle );
    return new StubberReturn( exception, new EStillClosesRunnable( runnable, exception ) );
  }

  private interface IOERunnable {
    void run() throws IOException;
  }

  private class StubberReturn {
    private final Exception exception;
    private final IOERunnable ioeRunnable;

    private StubberReturn( Exception exception, IOERunnable ioeRunnable ) {
      this.exception = exception;
      this.ioeRunnable = ioeRunnable;
    }
  }

  private class EStillClosesRunnable implements IOERunnable {
    private final IOERunnable runnable;
    private final Exception exception;

    private EStillClosesRunnable( IOERunnable runnable, Exception exception ) {
      this.runnable = runnable;
      this.exception = exception;
    }

    @Override public void run() throws IOException {
      try {
        runnable.run();
      } catch ( IOException e ) {
        assertEquals( exception, e.getCause() );
        verify( hBaseConnectionHandle ).close();
        throw e;
      }
    }
  }
}
