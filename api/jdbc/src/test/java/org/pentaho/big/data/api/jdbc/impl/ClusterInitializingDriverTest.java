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
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.api.initializer.ClusterInitializer;
import org.pentaho.big.data.api.jdbc.JdbcUrl;
import org.pentaho.big.data.api.jdbc.JdbcUrlParser;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 4/29/16.
 */
@RunWith( MockitoJUnitRunner.class )
public class ClusterInitializingDriverTest {
  @Mock ClusterInitializer clusterInitializer;
  @Mock JdbcUrlParser jdbcUrlParser;
  @Mock DriverLocatorImpl driverRegistry;
  @Mock HasRegisterDriver hasRegisterDriver;
  @Mock NamedCluster namedCluster;
  @Mock JdbcUrl jdbcUrl;
  Integer numLazyProxies;
  ClusterInitializingDriver clusterInitializingDriver;
  String testUrl;

  @Before
  public void setup() throws SQLException, URISyntaxException, MetaStoreException {
    numLazyProxies = 6;
    clusterInitializingDriver =
      new ClusterInitializingDriver( clusterInitializer, jdbcUrlParser, driverRegistry, numLazyProxies,
        hasRegisterDriver );
    verify( hasRegisterDriver, times( 7 ) ).registerDriver( any() );
    testUrl = "testUrl";
    when( jdbcUrlParser.parse( testUrl ) ).thenReturn( jdbcUrl );
    when( jdbcUrl.getNamedCluster() ).thenReturn( namedCluster );
  }

  @Test
  public void testMinimalConstructor() {
    assertNotNull( new ClusterInitializingDriver( clusterInitializer, jdbcUrlParser, driverRegistry ) );
  }

  @Test
  public void testRegisterDriverFailures() throws SQLException {
    doThrow( new SQLException() ).when( hasRegisterDriver ).registerDriver( any() );
    assertNotNull(
      new ClusterInitializingDriver( clusterInitializer, jdbcUrlParser, driverRegistry, null, hasRegisterDriver ) );
  }

  @Test
  public void testConnect() throws SQLException, ClusterInitializationException {
    assertNull( clusterInitializingDriver.connect( testUrl, null ) );
    verify( clusterInitializer ).initialize( null );
  }

  @Test
  public void testConnectException() throws ClusterInitializationException, SQLException {
    doThrow( new ClusterInitializationException( null ) ).when( clusterInitializer ).initialize( namedCluster );
    assertFalse( clusterInitializingDriver.acceptsURL( testUrl ) );
  }

  @Test
  public void testAccept() throws SQLException, ClusterInitializationException {
    assertFalse( clusterInitializingDriver.acceptsURL( testUrl ) );
    verify( clusterInitializer ).initialize( null );
  }

  @Test
  public void testGetPropertyInfo() throws SQLException {
    assertEquals( 0, clusterInitializingDriver.getPropertyInfo( null, null ).length );
  }

  @Test
  public void testGetMajorVersion() {
    assertEquals( 0, clusterInitializingDriver.getMajorVersion() );
  }

  @Test
  public void testGetMinorVersion() {
    assertEquals( 0, clusterInitializingDriver.getMinorVersion() );
  }

  @Test
  public void testJdbcCompliant() {
    assertFalse( clusterInitializingDriver.jdbcCompliant() );
  }

  @Test
  public void testGetParentLogger() throws SQLFeatureNotSupportedException {
    assertNull( clusterInitializingDriver.getParentLogger() );
  }
}
