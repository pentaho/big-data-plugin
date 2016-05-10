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

package org.pentaho.big.data.api.jdbc.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.osgi.framework.ServiceReference;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/29/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class LazyDelegatingDriverTest {
  @Mock DriverLocatorImpl driverRegistry;
  @Mock HasRegisterDriver hasRegisterDriver;
  @Mock ServiceReference<Driver> badDriverServiceReference;
  @Mock Driver badDriver;
  @Mock ServiceReference<Driver> goodDriverServiceReference;
  @Mock Driver driver;
  @Mock Connection connection;
  @Mock DriverPropertyInfo driverPropertyInfo;
  @Mock Logger logger;
  List<Map.Entry<ServiceReference<Driver>, Driver>> drivers;
  LazyDelegatingDriver lazyDelegatingDriver;
  String testUrl;
  int majorVersion;
  int minorVersion;

  private Map.Entry<ServiceReference<Driver>, Driver> makeEntry( ServiceReference<Driver> serviceReference, Driver driver ) {
    Map.Entry<ServiceReference<Driver>, Driver> entry = mock( Map.Entry.class );
    when( entry.getKey() ).thenReturn( serviceReference );
    when( entry.getValue() ).thenReturn( driver );
    return entry;
  }

  @Before
  public void setup() throws SQLException {
    testUrl = "testUrl";
    majorVersion = 10;
    minorVersion = 11;
    lazyDelegatingDriver = new LazyDelegatingDriver( driverRegistry, hasRegisterDriver );
    drivers = new ArrayList<>();
    drivers.add( makeEntry( badDriverServiceReference, badDriver ) );
    drivers.add( makeEntry( goodDriverServiceReference, driver ) );
    when( driver.connect( testUrl, null ) ).thenReturn( connection );
    when( driver.acceptsURL( testUrl ) ).thenReturn( true );
    when( driver.getPropertyInfo( testUrl, null ) ).thenReturn( new DriverPropertyInfo[] { driverPropertyInfo } );
    when( driver.getMajorVersion() ).thenReturn( majorVersion );
    when( driver.getMinorVersion() ).thenReturn( minorVersion );
    when( driver.getParentLogger() ).thenReturn( logger );
    when( driverRegistry.getDrivers() ).thenReturn( drivers.iterator() );
  }

  @Test
  public void testSimpleConstructor() throws SQLException {
    assertNotNull( new LazyDelegatingDriver( driverRegistry ) );
  }

  @Test
  public void testConnectMatch() throws SQLException {
    assertEquals( connection, lazyDelegatingDriver.connect( testUrl, null ) );
    drivers.clear();
    assertEquals( connection, lazyDelegatingDriver.connect( testUrl, null ) );
  }

  @Test
  public void testConnectNoMatch() throws SQLException {
    assertNull( lazyDelegatingDriver.connect( "badurl", null ) );
  }

  @Test
  public void testAcceptsMatch() throws SQLException {
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
    drivers.clear();
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
  }

  @Test
  public void testGetPropertyInfoEmpty() throws SQLException {
    assertEquals( 0, lazyDelegatingDriver.getPropertyInfo( testUrl, null ).length );
  }

  @Test
  public void testGetPropertyInfoNotEmpty() throws SQLException {
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
    assertEquals( 1, lazyDelegatingDriver.getPropertyInfo( testUrl, null ).length );
    assertEquals( driverPropertyInfo, lazyDelegatingDriver.getPropertyInfo( testUrl, null )[ 0 ] );
  }

  @Test
  public void testGetMajorVersionEmpty() {
    assertEquals( 0, lazyDelegatingDriver.getMajorVersion() );
  }

  @Test
  public void testGetMajorVersionNotEmpty() throws SQLException {
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
    assertEquals( majorVersion, lazyDelegatingDriver.getMajorVersion() );
  }

  @Test
  public void testGetMinorVersion() throws SQLException {
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
    assertEquals( minorVersion, lazyDelegatingDriver.getMinorVersion() );
  }

  @Test
  public void testGetJdbcCompliant() throws SQLException {
    when( driver.jdbcCompliant() ).thenReturn( true ).thenReturn( false );
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
    assertTrue( lazyDelegatingDriver.jdbcCompliant() );
    assertFalse( lazyDelegatingDriver.jdbcCompliant() );
  }

  @Test
  public void testGetParentLoggerEmpty() throws SQLFeatureNotSupportedException {
    assertNull( lazyDelegatingDriver.getParentLogger() );
  }

  @Test
  public void testGetParentLoggerNotEmpty() throws SQLException {
    assertTrue( lazyDelegatingDriver.acceptsURL( testUrl ) );
    assertEquals( logger, lazyDelegatingDriver.getParentLogger() );
  }
}
