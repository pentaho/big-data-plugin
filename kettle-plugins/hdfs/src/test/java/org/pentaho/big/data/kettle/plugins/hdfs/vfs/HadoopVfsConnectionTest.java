/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hdfs.vfs;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.variables.Variables;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 11/23/15.
 */
public class HadoopVfsConnectionTest {

  /**
   *
   */
  private static final String DEFAULT_VALUE = "default";

  /**
   *
   */
  private static final String EXPECTED_URL = "hdfs://testUser:testPassword@testHost:testPort";

  private static final String TEST_PASSWORD = "testPassword";

  private static final String TEST_USER = "testUser";

  private static final String TEST_PORT = "testPort";

  private static final String TEST_HOST = "testHost";

  private static final String EMPTY = "";

  @Test
  public void testDefaultConstructor() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection();
    assertEquals( EMPTY, hdfsConnection.getHostname() );
    assertEquals( EMPTY, hdfsConnection.getPassword() );
    assertEquals( EMPTY, hdfsConnection.getPort() );
    assertEquals( EMPTY, hdfsConnection.getUsername() );
  }

  @Test
  public void testConstructorWithParameters() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( TEST_HOST, TEST_PORT, TEST_USER, TEST_PASSWORD );
    assertEquals( TEST_HOST, hdfsConnection.getHostname() );
    assertEquals( TEST_PORT, hdfsConnection.getPort() );
    assertEquals( TEST_USER, hdfsConnection.getUsername() );
    assertEquals( TEST_PASSWORD, hdfsConnection.getPassword() );
  }

  @Test
  public void testConstructorWithNamedClusterAsParameter() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( getTestNamedCluster(), new Variables() );
    assertEquals( TEST_HOST, hdfsConnection.getHostname() );
    assertEquals( TEST_PORT, hdfsConnection.getPort() );
    assertEquals( TEST_USER, hdfsConnection.getUsername() );
    assertEquals( TEST_PASSWORD, hdfsConnection.getPassword() );
  }

  @Test
  public void testConstructorWithNamedClusterAsParameter_HostNameNull() {
    NamedCluster testNamedCluster = mock( NamedCluster.class );
    when( testNamedCluster.getHdfsHost() ).thenReturn( null );
    when( testNamedCluster.getHdfsPort() ).thenReturn( TEST_PORT );
    when( testNamedCluster.getHdfsUsername() ).thenReturn( TEST_USER );
    when( testNamedCluster.getHdfsPassword() ).thenReturn( TEST_PASSWORD );

    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( testNamedCluster, new Variables() );
    assertEquals( EMPTY, hdfsConnection.getHostname() );
    assertEquals( TEST_PORT, hdfsConnection.getPort() );
    assertEquals( TEST_USER, hdfsConnection.getUsername() );
    assertEquals( TEST_PASSWORD, hdfsConnection.getPassword() );
  }

  @Test
  public void testConstructorWithNamedClusterAsParameter_PortNull() {
    NamedCluster testNamedCluster = mock( NamedCluster.class );
    when( testNamedCluster.getHdfsHost() ).thenReturn( TEST_HOST );
    when( testNamedCluster.getHdfsPort() ).thenReturn( null );
    when( testNamedCluster.getHdfsUsername() ).thenReturn( TEST_USER );
    when( testNamedCluster.getHdfsPassword() ).thenReturn( TEST_PASSWORD );

    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( testNamedCluster, new Variables() );
    assertEquals( TEST_HOST, hdfsConnection.getHostname() );
    assertEquals( EMPTY, hdfsConnection.getPort() );
    assertEquals( TEST_USER, hdfsConnection.getUsername() );
    assertEquals( TEST_PASSWORD, hdfsConnection.getPassword() );
  }

  @Test
  public void testConstructorWithNamedClusterAsParameter_UserNull() {
    NamedCluster testNamedCluster = mock( NamedCluster.class );
    when( testNamedCluster.getHdfsHost() ).thenReturn( TEST_HOST );
    when( testNamedCluster.getHdfsPort() ).thenReturn( TEST_PORT );
    when( testNamedCluster.getHdfsUsername() ).thenReturn( null );
    when( testNamedCluster.getHdfsPassword() ).thenReturn( TEST_PASSWORD );

    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( testNamedCluster, new Variables() );
    assertEquals( TEST_HOST, hdfsConnection.getHostname() );
    assertEquals( TEST_PORT, hdfsConnection.getPort() );
    assertEquals( EMPTY, hdfsConnection.getUsername() );
    assertEquals( TEST_PASSWORD, hdfsConnection.getPassword() );
  }

  @Test
  public void testConstructorWithNamedClusterAsParameter_PasswordNull() {
    NamedCluster testNamedCluster = mock( NamedCluster.class );
    when( testNamedCluster.getHdfsHost() ).thenReturn( TEST_HOST );
    when( testNamedCluster.getHdfsPort() ).thenReturn( TEST_PORT );
    when( testNamedCluster.getHdfsUsername() ).thenReturn( TEST_USER );
    when( testNamedCluster.getHdfsPassword() ).thenReturn( null );

    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( testNamedCluster, new Variables() );
    assertEquals( TEST_HOST, hdfsConnection.getHostname() );
    assertEquals( TEST_PORT, hdfsConnection.getPort() );
    assertEquals( TEST_USER, hdfsConnection.getUsername() );
    assertEquals( EMPTY, hdfsConnection.getPassword() );
  }

  @Test
  public void testConstructorWithNamedClusterNullAsParameter() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( null, new Variables() );
    assertEquals( EMPTY, hdfsConnection.getHostname() );
    assertEquals( EMPTY, hdfsConnection.getPort() );
    assertEquals( EMPTY, hdfsConnection.getUsername() );
    assertEquals( EMPTY, hdfsConnection.getPassword() );
  }

  @Test
  public void testGetConnectionStringForHDFSScheme() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( getTestNamedCluster(), new Variables() );
    assertEquals( EXPECTED_URL, hdfsConnection.getConnectionString( "hdfs" ) );
  }

  @Test
  public void testGetConnectionStringForNullInputScheme() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( getTestNamedCluster(), new Variables() );
    assertEquals( EXPECTED_URL, hdfsConnection.getConnectionString( null ) );
  }

  @Test
  public void testGetConnectionStringForEmptyInputScheme() {
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( getTestNamedCluster(), new Variables() );
    assertEquals( EXPECTED_URL, hdfsConnection.getConnectionString( EMPTY ) );
  }

  private NamedCluster getTestNamedCluster() {
    NamedCluster nCluster = mock( NamedCluster.class );
    when( nCluster.getHdfsHost() ).thenReturn( TEST_HOST );
    when( nCluster.getHdfsPort() ).thenReturn( TEST_PORT );
    when( nCluster.getHdfsUsername() ).thenReturn( TEST_USER );
    when( nCluster.getHdfsPassword() ).thenReturn( TEST_PASSWORD );
    return nCluster;
  }

  @Test
  public void tesSetSustomParameters() throws KettleFileException {
    Props.init( 0 );
    HadoopVfsConnection hdfsConnection = new HadoopVfsConnection( getTestNamedCluster(), new Variables() );
    hdfsConnection.setCustomParameters( Props.getInstance() );
    assertEquals( TEST_HOST, Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.host",
      DEFAULT_VALUE ) );
    assertEquals( TEST_PORT, Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.port",
      DEFAULT_VALUE ) );
    assertEquals( TEST_USER, Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.user",
      DEFAULT_VALUE ) );
    assertEquals( TEST_PASSWORD, Props.getInstance().getCustomParameter( "HadoopVfsFileChooserDialog.password",
      DEFAULT_VALUE ) );
  }

}
