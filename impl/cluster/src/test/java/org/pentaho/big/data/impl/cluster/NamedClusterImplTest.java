/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.cluster;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.util.Map;

import static com.google.code.beanmatchers.BeanMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 7/14/15.
 */
public class NamedClusterImplTest {
  private VariableSpace variableSpace;
  private NamedClusterImpl namedCluster;

  private String jobEntryName;
  private String namedClusterName;
  private String namedClusterHdfsHost;
  private String namedClusterHdfsPort;
  private String namedClusterHdfsUsername;
  private String namedClusterHdfsPassword;
  private String namedClusterJobTrackerPort;
  private String namedClusterJobTrackerHost;
  private String namedClusterZookeeperHost;
  private String namedClusterZookeeperPort;
  private String namedClusterOozieUrl;
  private boolean isMapr;
  private IMetaStore metaStore;

  @Before
  public void setup() {
    metaStore = mock( IMetaStore.class );
    variableSpace = mock( VariableSpace.class );
    variableSpace = mock( VariableSpace.class );
    namedCluster = new NamedClusterImpl();
    namedCluster.shareVariablesWith( variableSpace );
    namedClusterName = "namedClusterName";
    namedClusterHdfsHost = "namedClusterHdfsHost";
    namedClusterHdfsPort = "12345";
    namedClusterHdfsUsername = "namedClusterHdfsUsername";
    namedClusterHdfsPassword = "namedClusterHdfsPassword";
    namedClusterJobTrackerHost = "namedClusterJobTrackerHost";
    namedClusterJobTrackerPort = "namedClusterJobTrackerPort";
    namedClusterZookeeperHost = "namedClusterZookeeperHost";
    namedClusterZookeeperPort = "namedClusterZookeeperPort";
    namedClusterOozieUrl = "namedClusterOozieUrl";
    isMapr = true;

    namedCluster.setName( namedClusterName );
    namedCluster.setHdfsHost( namedClusterHdfsHost );
    namedCluster.setHdfsPort( namedClusterHdfsPort );
    namedCluster.setHdfsUsername( namedClusterHdfsUsername );
    namedCluster.setHdfsPassword( namedClusterHdfsPassword );
    namedCluster.setJobTrackerHost( namedClusterJobTrackerHost );
    namedCluster.setJobTrackerPort( namedClusterJobTrackerPort );
    namedCluster.setZooKeeperHost( namedClusterZookeeperHost );
    namedCluster.setZooKeeperPort( namedClusterZookeeperPort );
    namedCluster.setOozieUrl( namedClusterOozieUrl );
    namedCluster.setMapr( isMapr );
  }

  @Test
  public void testBean() {
    assertThat( NamedClusterImpl.class, hasValidBeanConstructor() );
    assertThat( NamedClusterImpl.class, hasValidGettersAndSetters() );
    assertThat( NamedClusterImpl.class, hasValidBeanEqualsFor( "name" ) );
  }

  @Test
  public void testClone() {
    long before = System.currentTimeMillis();
    NamedClusterImpl newNamedCluster = namedCluster.clone();
    assertEquals( namedClusterName, newNamedCluster.getName() );
    assertEquals( namedClusterHdfsHost, newNamedCluster.getHdfsHost() );
    assertEquals( namedClusterHdfsPort, newNamedCluster.getHdfsPort() );
    assertEquals( namedClusterHdfsUsername, newNamedCluster.getHdfsUsername() );
    assertEquals( namedClusterHdfsPassword, newNamedCluster.getHdfsPassword() );
    assertEquals( namedClusterJobTrackerHost, newNamedCluster.getJobTrackerHost() );
    assertEquals( namedClusterJobTrackerPort, newNamedCluster.getJobTrackerPort() );
    assertEquals( namedClusterZookeeperHost, newNamedCluster.getZooKeeperHost() );
    assertEquals( namedClusterZookeeperPort, newNamedCluster.getZooKeeperPort() );
    assertEquals( namedClusterOozieUrl, newNamedCluster.getOozieUrl() );
    assertTrue( before <= newNamedCluster.getLastModifiedDate() );
    assertTrue( newNamedCluster.getLastModifiedDate() <= System.currentTimeMillis() );
  }

  @Test
  public void testCopyVariablesFrom() {
    VariableSpace from = mock( VariableSpace.class );
    namedCluster.copyVariablesFrom( from );
    verify( variableSpace ).copyVariablesFrom( from );
  }

  @Test
  public void testEnvironmentSubstitute() {
    String testVar = "testVar";
    String testVal = "testVal";
    when( variableSpace.environmentSubstitute( testVar ) ).thenReturn( testVal );
    assertEquals( testVal, namedCluster.environmentSubstitute( testVar ) );
  }

  @Test
  public void testArrayEnvironmentSubstitute() {
    String[] testVars = { "testVar" };
    String[] testVals = { "testVal" };
    when( variableSpace.environmentSubstitute( testVars ) ).thenReturn( testVals );
    assertArrayEquals( testVals, namedCluster.environmentSubstitute( testVars ) );
  }

  @Test
  public void testFieldSubstitute() throws KettleValueException {
    String testString = "testString";
    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    Object[] rowData = new Object[] {};
    String testVal = "testVal";
    when( variableSpace.fieldSubstitute( testString, rowMetaInterface, rowData ) ).thenReturn( testVal );
    assertEquals( testVal, namedCluster.fieldSubstitute( testString, rowMetaInterface, rowData ) );
  }

  @Test
  public void testGetVariableDefault() {
    String name = "name";
    String defaultValue = "default";
    String val = "val";
    when( variableSpace.getVariable( name, defaultValue ) ).thenReturn( val );
    assertEquals( val, namedCluster.getVariable( name, defaultValue ) );
  }

  @Test
  public void testGetVariable() {
    String name = "name";
    String val = "val";
    when( variableSpace.getVariable( name ) ).thenReturn( val );
    assertEquals( val, namedCluster.getVariable( name ) );
  }

  @Test
  public void testGetBooleanValueOfVariable() {
    String var = "var";
    String val1 = "Y";
    String val2 = "N";

    assertTrue( namedCluster.getBooleanValueOfVariable( null, true ) );
    assertFalse( namedCluster.getBooleanValueOfVariable( null, false ) );

    when( variableSpace.environmentSubstitute( var ) ).thenReturn( val1 ).thenReturn( val2 ).thenReturn( null );
    assertTrue( namedCluster.getBooleanValueOfVariable( var, false ) );
    assertFalse( namedCluster.getBooleanValueOfVariable( var, true ) );
    assertTrue( namedCluster.getBooleanValueOfVariable( var, true ) );
    assertFalse( namedCluster.getBooleanValueOfVariable( var, false ) );
  }

  @Test
  public void testListVariables() {
    String[] vars = new String[] { "vars" };
    when( variableSpace.listVariables() ).thenReturn( vars );
    assertArrayEquals( vars, namedCluster.listVariables() );
  }

  @Test
  public void testSetVariable() {
    String var = "var";
    String val = "val";
    namedCluster.setVariable( var, val );
    verify( variableSpace ).setVariable( var, val );
  }

  @Test
  @SuppressWarnings( "unchecked" )
  public void testInjectVariables() {
    Map<String, String> prop = mock( Map.class );
    namedCluster.injectVariables( prop );
    verify( variableSpace ).injectVariables( prop );
  }

  @Test
  public void testComparator() {
    NamedClusterImpl other = new NamedClusterImpl();
    other.setName( "a" );
    assertTrue( NamedClusterImpl.comparator.compare( namedCluster, other ) > 0 );
    other.setName( "z" );
    assertTrue( NamedClusterImpl.comparator.compare( namedCluster, other ) < 0 );
    other.setName( namedClusterName );
    assertTrue( NamedClusterImpl.comparator.compare( namedCluster, other ) == 0 );
  }

  @Test
  public void testToString() {
    NamedClusterImpl other = new NamedClusterImpl();
    assertEquals( "Named cluster: null", other.toString() );
    other.setName( "a" );
    assertEquals( "Named cluster: a", other.toString() );
  }

  @Test
  public void testGenerateURLNullParameters() {
    namedCluster.setName( null );
    assertNull( namedCluster.generateURL( "testScheme", metaStore, null ) );
    namedCluster.setName( "testName" );
    assertNull( namedCluster.generateURL( null, metaStore, null ) );
    assertNull( namedCluster.generateURL( "testScheme", null, null ) );
    assertNull( namedCluster.generateURL( "testScheme", metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFS() throws MetaStoreException {
    String scheme = "hdfs";
    String testHost = "testHost";
    String testPort = "9333";
    String testUsername = "testUsername";
    String testPassword = "testPassword";
    namedCluster.setHdfsHost( " " + testHost + " " );
    namedCluster.setHdfsPort( " " + testPort + " " );
    namedCluster.setHdfsUsername( " " + testUsername + " " );
    namedCluster.setHdfsPassword( " " + testPassword + " " );
    assertEquals( scheme + "://" + testUsername + ":" + testPassword + "@" + testHost + ":" + testPort,
      namedCluster.generateURL( scheme, metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFSPort() throws MetaStoreException {
    String scheme = "hdfs";
    String testHost = "testHost";
    String testPort = "9333";
    namedCluster.setHdfsHost( " " + testHost + " " );
    namedCluster.setHdfsPort( " " + testPort + " " );
    namedCluster.setHdfsUsername( null );
    namedCluster.setHdfsPassword( null );
    assertEquals( scheme + "://" + testHost + ":" + testPort,
      namedCluster.generateURL( scheme, metaStore, null ) );
  }

  @Test
  public void testGenerateURLMaprFSPort() throws MetaStoreException {
    String scheme = "maprfs";
    String testHost = "testHost";
    String testPort = "9333";
    namedCluster.setHdfsHost( " " + testHost + " " );
    namedCluster.setHdfsPort( " " + testPort + " " );
    assertNull( namedCluster.generateURL( scheme, metaStore, null ) );
  }

  @Test
  public void testCheckHdfsNameEmpty() throws MetaStoreException {
    String testHost = "";
    namedCluster.setHdfsHost( " " + testHost + " " );
    assertEquals( true, namedCluster.isHdfsHostEmpty( null ) );
  }

  @Test
  public void testGetHdfsNameParsed() throws MetaStoreException {
    String testHost = "test";
    namedCluster.setHdfsHost( " " + testHost + " " );
    assertEquals( "test", namedCluster.getHostNameParsed( null ) );
  }

  @Test
  public void testGetHdfsNameParsedFromVariable() throws MetaStoreException {
    String testHost = "${hdfsHost}";
    namedCluster.setHdfsHost( " " + testHost + " " );
    when( variableSpace.getVariable( "hdfsHost" ) ).thenReturn( "test" );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( "test" );
    assertEquals( "test", namedCluster.getHostNameParsed( variableSpace ) );
  }

  @Test
  public void testGetHdfsNameParsedFromVariableNoVariableInSpace() throws MetaStoreException {
    String testHost = "${hdfsHost}";
    namedCluster.setHdfsHost( " " + testHost + " " );
    assertEquals( null, namedCluster.getHostNameParsed( variableSpace ) );
  }

  @Test
  public void testCheckHdfsNameNotEmpty() throws MetaStoreException {
    String testHost = "test";
    namedCluster.setHdfsHost( " " + testHost + " " );
    assertEquals( false, namedCluster.isHdfsHostEmpty( null ) );
  }

  @Test
  public void testCheckHdfsNameNull() throws MetaStoreException {
    namedCluster.setHdfsHost( null );
    assertEquals( true, namedCluster.isHdfsHostEmpty( null ) );
  }

  @Test
  public void testCheckHdfsNameVariableNull() throws MetaStoreException {
    namedCluster.setHdfsHost( "${hdfsHost}" );
    assertEquals( true, namedCluster.isHdfsHostEmpty( null ) );
  }

  @Test
  public void testCheckHdfsNameVariableNotNull() throws MetaStoreException {
    namedCluster.setHdfsHost( "${hdfsHost}" );
    when( variableSpace.getVariable( "hdfsHost" ) ).thenReturn( "test" );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( "test" );
    assertEquals( false, namedCluster.isHdfsHostEmpty( variableSpace ) );
  }

  @Test
  public void testProcessURLHostEmpty() throws MetaStoreException {
    //namedCluster.setHdfsHost( "hostname" );
    //namedCluster.setHdfsPort( "12340");
    namedCluster.setHdfsHost( null );
    namedCluster.setStorageScheme( "hdfs");
    String incomingURL = "${hdfsUrl}/test";
    //incomingURL = "/tmp/hdsfDemo.txt";
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }
  
  @Test
  public void testProcessURLhdfsFullSubstitution() throws MetaStoreException {
    namedCluster.setHdfsHost( "hostname" );
    namedCluster.setHdfsPort( "12340");
    namedCluster.setStorageScheme( "hdfs");
    String incomingURL = "hdfs://namedClusterHdfsUsername:namedClusterHdfsPassword@hostname:12340/tmp/hdsfDemo.txt";
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }
  
  @Test
  public void testProcessURLWASBFullSubstitution() throws MetaStoreException {
    namedCluster.setHdfsHost( "hostname" );
    namedCluster.setHdfsPort( "12340");
    namedCluster.setStorageScheme( "wasb");
    String incomingURL = "wasb://namedClusterHdfsUsername:namedClusterHdfsPassword@hostname:12340/tmp/hdsfDemo.txt";
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }
  
  @Test
  public void testProcessURLHostVariableNull() throws MetaStoreException {
    namedCluster.setHdfsHost( "${hostUrl}" );
    namedCluster.setStorageScheme( "hdfs");
    String incomingURL = "${hdfsUrl}/test";
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLHostVariableNotNull() throws MetaStoreException {
    namedCluster.setHdfsHost( "${hostUrl}" );
    namedCluster.setStorageScheme( "hdfs");
    String hostPort = "1000";
    namedCluster.setHdfsPort( hostPort );
    namedCluster.setHdfsUsername( "" );
    namedCluster.setHdfsPassword( "" );
    String incomingURL = "${hdfsUrl}/test";
    String hostName = "test";
    when( variableSpace.getVariable( "hostUrl" ) ).thenReturn( hostName );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( hostName );
    assertEquals( "hdfs://" + hostName + ":" + hostPort + incomingURL,
      namedCluster.processURLsubstitution( incomingURL, metaStore, variableSpace ) );
  }

  @Test
  public void testGenerateURLMaprFS() throws MetaStoreException {
    String scheme = "maprfs";
    namedCluster.setMapr( true );
    assertNull( namedCluster.generateURL( scheme, metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFSNoPort() throws MetaStoreException {
    String scheme = "hdfs";
    String testHost = "testHost";
    namedCluster.setHdfsHost( " " + testHost + " " );
    namedCluster.setHdfsPort( null );
    namedCluster.setHdfsUsername( null );
    namedCluster.setHdfsPassword( null );
    assertEquals( scheme + "://" + testHost, namedCluster.generateURL( scheme, metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFSVariableSpace() throws MetaStoreException {
    String scheme = "hdfs";
    String hostVar = "hostVar";
    String testHost = "testHost";
    String portVar = "portVar";
    String testPort = "9333";
    String usernameVar = "usernameVar";
    String testUsername = "testUsername";
    String passwordVar = "passwordVar";
    String testPassword = "testPassword";
    namedCluster.setHdfsHost( "${" + hostVar + "}" );
    namedCluster.setHdfsPort( "${" + portVar + "}" );
    namedCluster.setHdfsUsername( "${" + usernameVar + "}" );
    namedCluster.setHdfsPassword( "${" + passwordVar + "}" );
    when( variableSpace.getVariable( hostVar ) ).thenReturn( testHost );
    when( variableSpace.getVariable( portVar ) ).thenReturn( testPort );
    when( variableSpace.getVariable( usernameVar ) ).thenReturn( testUsername );
    when( variableSpace.getVariable( passwordVar ) ).thenReturn( testPassword );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( testHost );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ) ).thenReturn( testPort );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsUsername() ) ).thenReturn( testUsername );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsPassword() ) ).thenReturn( testPassword );
    assertEquals( scheme + "://" + testUsername + ":" + testPassword + "@" + testHost + ":" + testPort,
      namedCluster.generateURL( scheme, metaStore, variableSpace ) );
  }
}
