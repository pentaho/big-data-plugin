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

package com.pentaho.big.data.bundles.impl.shim.hive;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.jdbc.JdbcUrl;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/18/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class HiveDriverTest {
  @Mock Driver delegate;
  @Mock JdbcUrlParser jdbcUrlParser;
  @Mock JdbcUrl jdbcUrl;
  @Mock Properties properties;
  @Mock Connection connection;
  HiveDriver hiveDriver;
  String testUrl;

  @Before
  public void setup() throws URISyntaxException {
    hiveDriver = new HiveDriver( delegate, null, true, jdbcUrlParser );
    testUrl = "testUrl";
    when( jdbcUrlParser.parse( testUrl ) ).thenReturn( jdbcUrl );
    when( jdbcUrl.toString() ).thenReturn( testUrl );
  }

  @Test
  public void testConnectSimba() throws SQLException {
    assertNull( hiveDriver.connect( "jdbc:hive2//test;AuthMech=0", null ) );
  }

  @Test( expected = SQLException.class )
  public void testConnectFailParse() throws SQLException, URISyntaxException {
    String url = "fake-url";
    when( jdbcUrlParser.parse( url ) ).thenThrow( new URISyntaxException( "", "" ) );
    hiveDriver.connect( url, null );
  }

  @Test
  public void testConnectNotAccepts() throws SQLException, MetaStoreException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( false );
    assertNull( hiveDriver.connect( testUrl, null ) );
  }

  @Test
  public void testConnectNamedClusterException() throws SQLException, MetaStoreException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( false );
    when( jdbcUrl.getNamedCluster() ).thenThrow( new RuntimeException() );
    assertNull( hiveDriver.connect( testUrl, null ) );
  }

  @Test
  public void testSuccess() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    when( delegate.connect( testUrl, properties ) ).thenReturn( connection );
    assertEquals( connection, hiveDriver.connect( testUrl, properties ) );
  }

  @Test( expected = RuntimeException.class )
  public void testException() throws SQLException {
    RuntimeException runtimeException = new RuntimeException();
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    when( delegate.connect( testUrl, properties ) ).thenThrow( runtimeException );
    try {
      hiveDriver.connect( testUrl, properties );
    } catch ( Exception e ) {
      assertEquals( runtimeException, e );
      throw e;
    }
  }

  @Test
  public void testStateNotSupportedException() throws SQLException {
    RuntimeException runtimeException =
      new RuntimeException( new SQLException( "test", HiveDriver.SQL_STATE_NOT_SUPPORTED, null ) );
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    when( delegate.connect( testUrl, properties ) ).thenThrow( runtimeException );
    assertNull( hiveDriver.connect( testUrl, properties ) );
    verify( delegate ).connect( testUrl, properties );
  }

  @Test
  public void testAcceptsUrlException() throws URISyntaxException, SQLException {
    String url = "fake-url";
    when( jdbcUrlParser.parse( url ) ).thenThrow( new URISyntaxException( "", "" ) );
    assertFalse( hiveDriver.acceptsURL( url ) );
  }

  @Test
  public void testAcceptsUrlTrue() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    assertTrue( hiveDriver.acceptsURL( testUrl ) );
  }

  @Test
  public void testAcceptsUrlDelegateException() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenThrow( new SQLException() );
    assertFalse( hiveDriver.acceptsURL( testUrl ) );
  }

  @Test
  public void testAcceptsUrlExceptionGettingId() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    assertFalse( new HiveDriver( delegate, null, true, jdbcUrlParser ){
      @Override
      public Driver checkBeforeCallActiveDriver(String url) throws SQLException {
        throw new SQLException("Mock Exception");
      }
    }.acceptsURL( testUrl ) );
  }

  @Test
  public void testAcceptsUrlNullDelegate() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    assertFalse( new HiveDriver( null, null, true, jdbcUrlParser ).acceptsURL( testUrl ) );
  }

  @Test
  public void testAcceptsUrlNotDefault() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    assertFalse( new HiveDriver( delegate, null, false, jdbcUrlParser ).acceptsURL( testUrl ) );
    assertFalse( new HiveDriver( delegate, "testId", false, jdbcUrlParser ).acceptsURL( testUrl ) );
  }

  @Test
  public void testAcceptsUrlNotDefaultWithConfigId() throws SQLException {
    when( delegate.acceptsURL( testUrl ) ).thenReturn( true );
    String testId = "testId";
    assertFalse( new HiveDriver( delegate, testId, false, jdbcUrlParser ).acceptsURL( testUrl ) );
  }

  @Test
  public void testGetPropertyInfoNullDelegate() throws SQLException {
    assertNull( new HiveDriver( null, null, true, jdbcUrlParser ).getPropertyInfo( testUrl, properties ) );
  }

  @Test
  public void testGetPropertyInfo() throws SQLException {
    DriverPropertyInfo[] driverPropertyInfos = new DriverPropertyInfo[] { mock( DriverPropertyInfo.class ) };
    when( delegate.getPropertyInfo( testUrl, properties ) ).thenReturn( driverPropertyInfos );
    assertArrayEquals( driverPropertyInfos, hiveDriver.getPropertyInfo( testUrl, properties ) );
  }

  @Test
  public void testGetMajorVersionNullDelegate() {
    assertEquals( -1, new HiveDriver( null, null, true, jdbcUrlParser ).getMajorVersion() );
  }

  @Test
  public void testGetMajorVersion() {
    int expected = 111;
    when( delegate.getMajorVersion() ).thenReturn( expected );
    assertEquals( expected, hiveDriver.getMajorVersion() );
  }

  @Test
  public void testGetMinorVersionNullDelegate() {
    assertEquals( -1, new HiveDriver( null, null, true, jdbcUrlParser ).getMinorVersion() );
  }

  @Test
  public void testGetMinorVersion() {
    int expected = 111;
    when( delegate.getMinorVersion() ).thenReturn( expected );
    assertEquals( expected, hiveDriver.getMinorVersion() );
  }

  @Test
  public void testJdbcCompliantNullDelegate() {
    assertFalse( new HiveDriver( null, null, true, jdbcUrlParser ).jdbcCompliant() );
  }

  @Test
  public void testJdbcCompliantException() {
    when( delegate.jdbcCompliant() ).thenThrow( new RuntimeException() );
    assertFalse( hiveDriver.jdbcCompliant() );
  }

  @Test
  public void testJdbcCompliant() {
    when( delegate.jdbcCompliant() ).thenReturn( true ).thenReturn( false );
    assertTrue( hiveDriver.jdbcCompliant() );
    assertFalse( hiveDriver.jdbcCompliant() );
  }

  @Test
  public void testGetParentLoggerNullDelegate() throws SQLFeatureNotSupportedException {
    assertNull( new HiveDriver( null, null, true, jdbcUrlParser ).getParentLogger() );
  }

  @Test
  public void testGetParentLogger() throws SQLFeatureNotSupportedException {
    Logger logger = mock( Logger.class );
    when( delegate.getParentLogger() ).thenReturn( logger );
    assertEquals( logger, hiveDriver.getParentLogger() );
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testGetParentLoggerRuntimeException() throws SQLFeatureNotSupportedException {
    RuntimeException runtimeException = new RuntimeException();
    when( delegate.getParentLogger() ).thenThrow( runtimeException );
    try {
      hiveDriver.getParentLogger();
    } catch ( Exception e ) {
      assertEquals( runtimeException, e.getCause() );
      throw e;
    }
  }

  @Test( expected = SQLFeatureNotSupportedException.class )
  public void testGetParentLoggerSQLFeatureNotSupportedException() throws SQLFeatureNotSupportedException {
    SQLFeatureNotSupportedException exception = new SQLFeatureNotSupportedException();
    when( delegate.getParentLogger() ).thenThrow( exception );
    try {
      hiveDriver.getParentLogger();
    } catch ( SQLFeatureNotSupportedException e ) {
      assertEquals( exception, e );
      throw e;
    }
  }
}
