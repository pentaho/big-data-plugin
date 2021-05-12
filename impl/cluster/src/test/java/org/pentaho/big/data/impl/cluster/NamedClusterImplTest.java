/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.osgi.api.NamedClusterSiteFile;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.api.security.Base64TwoWayPasswordEncoder;
import org.pentaho.metastore.api.security.ITwoWayPasswordEncoder;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import static com.google.code.beanmatchers.BeanMatchers.hasValidBeanConstructor;
import static com.google.code.beanmatchers.BeanMatchers.hasValidBeanEqualsFor;
import static com.google.code.beanmatchers.BeanMatchers.hasValidGettersAndSetters;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Created by bryan on 7/14/15.
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { UriParser.class, VFS.class } )
public class NamedClusterImplTest {
  private static final String HDFS_PREFIX = "hdfs";

  private VariableSpace variableSpace;
  private NamedClusterImpl namedCluster;

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
  private String namedClusterStorageScheme;
  private String namedClusterKafkaBootstrapServers;
  private boolean isMapr;
  private IMetaStore metaStore;
  private StandardFileSystemManager fsm;
  private String fileContents1;
  private String fileContents2;

  @Before
  public void setup() throws Exception {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
    mockStatic( VFS.class );
    mockStatic( UriParser.class );
    spy( UriParser.class );

    metaStore = mock( IMetaStore.class );
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
    namedClusterStorageScheme = "hdfs";
    namedClusterKafkaBootstrapServers = "kafkaBootstrapServers";
    isMapr = true;
    fileContents1 =
      FileUtils.readFileToString( new File( getClass().getResource( "/core-site.xml" ).getFile() ), "UTF-8" );
    fileContents2 = "some printable contents";

    namedCluster.setName( namedClusterName );
    namedCluster.setHdfsHost( namedClusterHdfsHost );
    namedCluster.setHdfsPort( namedClusterHdfsPort );
    namedCluster.setHdfsUsername( namedClusterHdfsUsername );
    namedCluster.setHdfsPassword( namedCluster.encodePassword( namedClusterHdfsPassword ) );
    namedCluster.setJobTrackerHost( namedClusterJobTrackerHost );
    namedCluster.setJobTrackerPort( namedClusterJobTrackerPort );
    namedCluster.setZooKeeperHost( namedClusterZookeeperHost );
    namedCluster.setZooKeeperPort( namedClusterZookeeperPort );
    namedCluster.setOozieUrl( namedClusterOozieUrl );
    namedCluster.setMapr( isMapr );
    namedCluster.setStorageScheme( namedClusterStorageScheme );
    namedCluster.setKafkaBootstrapServers( namedClusterKafkaBootstrapServers );
    namedCluster.addSiteFile( "core-site.xml", fileContents1 );
    namedCluster.addSiteFile( "hbase-site.xml", fileContents2 );

    fsm = mock( StandardFileSystemManager.class );
    when( VFS.getManager() ).thenReturn( fsm );
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
    assertEquals( namedClusterStorageScheme, newNamedCluster.getStorageScheme() );
    assertEquals( namedClusterName, newNamedCluster.getName() );
    assertEquals( namedClusterHdfsHost, newNamedCluster.getHdfsHost() );
    assertEquals( namedClusterHdfsPort, newNamedCluster.getHdfsPort() );
    assertEquals( namedClusterHdfsUsername, newNamedCluster.getHdfsUsername() );
    assertEquals( namedClusterHdfsPassword, newNamedCluster.decodePassword( newNamedCluster.getHdfsPassword() ) );
    assertEquals( namedClusterJobTrackerHost, newNamedCluster.getJobTrackerHost() );
    assertEquals( namedClusterJobTrackerPort, newNamedCluster.getJobTrackerPort() );
    assertEquals( namedClusterZookeeperHost, newNamedCluster.getZooKeeperHost() );
    assertEquals( namedClusterZookeeperPort, newNamedCluster.getZooKeeperPort() );
    assertEquals( namedClusterOozieUrl, newNamedCluster.getOozieUrl() );
    assertEquals( namedClusterKafkaBootstrapServers, newNamedCluster.getKafkaBootstrapServers() );
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
  public void testGenerateURLNullParameters() throws Exception {
    namedCluster.setName( null );
    String scheme = "testScheme";
    buildAppendEncodedUserPassMocks( namedClusterHdfsUsername, namedClusterHdfsPassword );
    assertEquals(
      scheme + "://" + namedClusterHdfsUsername + ":" + namedClusterHdfsPassword + "@" + namedClusterHdfsHost + ":"
        + namedClusterHdfsPort,
      namedCluster.generateURL( "testScheme", metaStore, null ) );
    assertNull( namedCluster.generateURL( null, metaStore, null ) );
    assertEquals(
      scheme + "://" + namedClusterHdfsUsername + ":" + namedClusterHdfsPassword + "@" + namedClusterHdfsHost + ":"
        + namedClusterHdfsPort,
      namedCluster.generateURL( "testScheme", null, null ) );
  }

  @Test
  public void testGenerateURLHDFS() throws Exception {
    String scheme = "hdfs";
    String testHost = "testHost";
    String testPort = "9333";
    String testUsername = "testUsername";
    String testPassword = "testPassword";
    namedCluster.setHdfsHost( " " + testHost + " " );
    namedCluster.setHdfsPort( " " + testPort + " " );
    namedCluster.setHdfsUsername( " " + testUsername + " " );
    namedCluster.setHdfsPassword( namedCluster.encodePassword( testPassword ) );
    buildAppendEncodedUserPassMocks( testUsername, namedCluster.encodePassword( testPassword ) );
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
    namedCluster.setHdfsHost( null );
    namedCluster.setStorageScheme( "hdfs" );
    String incomingURL = "${hdfsUrl}/test";
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLhdfsFullSubstitution() throws Exception {
    String pathBase = "//namedClusterHdfsUsername:namedClusterHdfsPassword@hostname:12340";
    String filePathInFileSystem = "/tmp/hdsfDemo.txt";
    namedCluster.setHdfsHost( "hostname" );
    namedCluster.setHdfsPort( "12340" );
    namedCluster.setStorageScheme( HDFS_PREFIX );
    String incomingURL = HDFS_PREFIX + ":" + pathBase + filePathInFileSystem;
    buildExtractSchemeMocks( HDFS_PREFIX, incomingURL, pathBase + filePathInFileSystem );
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLSubstitution_Gateway() throws MetaStoreException {
    namedCluster.setUseGateway( true );
    String incomingURL = "/path";
    String expected = "hc://" + namedCluster.getName() + incomingURL;
    String actual = namedCluster.processURLsubstitution( incomingURL, metaStore, null );
    assertTrue( "Expected " + expected + " actual " + actual, expected.equalsIgnoreCase( actual ) );
  }

  @Test
  public void testProcessURLWASBFullSubstitution() throws Exception {
    String prefix = "wasb";
    String pathBase = "//namedClusterHdfsUsername:namedClusterHdfsPassword@hostname:12340";
    String filePathInFileSystem = "/tmp/hdsfDemo.txt";
    namedCluster.setHdfsHost( "hostname" );
    namedCluster.setHdfsPort( "12340" );
    namedCluster.setStorageScheme( prefix );
    String incomingURL = prefix + ":" + pathBase + filePathInFileSystem;
    buildAppendEncodedUserPassMocks( namedClusterHdfsUsername, namedClusterHdfsPassword );
    buildExtractSchemeMocks( prefix, incomingURL, pathBase + filePathInFileSystem );
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLHostVariableNull() throws MetaStoreException {
    namedCluster.setHdfsHost( "${hostUrl}" );
    namedCluster.setStorageScheme( "hdfs" );
    String incomingURL = "${hdfsUrl}/test";
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLHostVariableNotNull() throws Exception {
    namedCluster.setHdfsHost( "${hostUrl}" );
    namedCluster.setStorageScheme( HDFS_PREFIX );
    String hostPort = "1000";
    namedCluster.setHdfsPort( hostPort );
    namedCluster.setHdfsUsername( "" );
    namedCluster.setHdfsPassword( "" );
    String incomingURL = "${hdfsUrl}/test";
    String hostName = "test";
    when( variableSpace.getVariable( "hostUrl" ) ).thenReturn( hostName );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( hostName );
    when( variableSpace.environmentSubstitute( incomingURL ) ).thenReturn( hostName + "/test" );
    String pathWithoutPrefix = "//" + hostName + ":" + hostPort + "//hdfsUrl//test";
    String pathWithPrefix = HDFS_PREFIX + ":" + pathWithoutPrefix;
    buildExtractSchemeMocks( HDFS_PREFIX, pathWithPrefix, pathWithoutPrefix );
    assertEquals( "hdfs://" + hostName + ":" + hostPort + incomingURL,
      namedCluster.processURLsubstitution( incomingURL, metaStore, variableSpace ) );
  }

  @Test
  public void testProcessCompleteClusterVariableReplacement() throws Exception {
    String hostname = "hostname";
    String hostPort = "1000";
    String variableName = "hdfsUrl";
    // special case to allow legacy fully qualified urls to work
    namedCluster.setHdfsHost( hostname );
    namedCluster.setStorageScheme( HDFS_PREFIX );
    namedCluster.setHdfsPort( hostPort );
    namedCluster.setHdfsUsername( "" );
    namedCluster.setHdfsPassword( "" );
    String incomingURL = "${" + variableName + "}/test";
    String pathWithoutPrefix = "//" + hostname + ":" + hostPort + "//" + variableName + "//test";
    String pathWithPrefix = HDFS_PREFIX + ":" + pathWithoutPrefix;
    when( variableSpace.environmentSubstitute( incomingURL ) ).thenReturn( "hdfs://FullyQualifiedPath/test" );
    buildExtractSchemeMocks( HDFS_PREFIX, pathWithPrefix, pathWithoutPrefix );
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, variableSpace ) );
  }

  @Test
  public void testProcessURLsubstitutionMaprFS_startsWithMaprfs() throws MetaStoreException {
    String incomingURL = "maprfs";
    namedCluster.setMapr( true );
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLsubstitutionMaprFS_startsWithNoMaprfs() throws MetaStoreException {
    String incomingURL = "path";
    namedCluster.setMapr( true );
    assertEquals( "maprfs://" + incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLsubstitutionNC() throws Exception {
    String prefix = "hc";
    String pathWithoutPrefix = "//cluster/input/file.txt";
    String pathWithPrefix = prefix + ":" + pathWithoutPrefix;
    buildAppendEncodedUserPassMocks( namedClusterHdfsUsername, namedClusterHdfsPassword );
    buildExtractSchemeMocks( prefix, pathWithPrefix, pathWithoutPrefix );
    assertEquals( "hdfs://namedClusterHdfsUsername:namedClusterHdfsPassword@namedClusterHdfsHost:12345/input/file.txt",
      namedCluster.processURLsubstitution( "hc://cluster/input/file.txt", metaStore, null ) );
  }

  @Test
  public void testProcessURLSubstitutionNC_variable() throws Exception {
    String pathWithoutPrefix = "//" + namedClusterHdfsUsername + ":" + namedClusterHdfsPassword + "@"
      + namedClusterHdfsHost + ":" + namedClusterHdfsPort + "//ncUrl//test";
    String pathWithPrefix = HDFS_PREFIX + ":" + pathWithoutPrefix;
    String incomingURL = "${ncUrl}/test";
    when( variableSpace.environmentSubstitute( incomingURL ) ).thenReturn( "hc://cluster/test" );
    buildAppendEncodedUserPassMocks( namedClusterHdfsUsername, namedClusterHdfsPassword );
    buildExtractSchemeMocks( HDFS_PREFIX, pathWithPrefix, pathWithoutPrefix );
    assertEquals( incomingURL, namedCluster.processURLsubstitution( incomingURL, metaStore, variableSpace ) );
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
  public void testGenerateURLHDFSVariableSpace() throws Exception {
    String schemeVar = "schemeVar";
    String testScheme = "hdfs";
    String hostVar = "hostVar";
    String testHost = "testHost";
    String portVar = "portVar";
    String testPort = "9333";
    String usernameVar = "usernameVar";
    String testUsername = "testUsername";
    String passwordVar = "passwordVar";
    String testPassword = "testPassword";
    namedCluster.setStorageScheme( "${" + schemeVar + "}" );
    namedCluster.setHdfsHost( "${" + hostVar + "}" );
    namedCluster.setHdfsPort( "${" + portVar + "}" );
    namedCluster.setHdfsUsername( "${" + usernameVar + "}" );
    namedCluster.setHdfsPassword( "${" + passwordVar + "}" );
    when( variableSpace.getVariable( schemeVar ) ).thenReturn( testScheme );
    when( variableSpace.getVariable( hostVar ) ).thenReturn( testHost );
    when( variableSpace.getVariable( portVar ) ).thenReturn( testPort );
    when( variableSpace.getVariable( usernameVar ) ).thenReturn( testUsername );
    when( variableSpace.getVariable( passwordVar ) ).thenReturn( testPassword );
    when( variableSpace.environmentSubstitute( namedCluster.getStorageScheme() ) ).thenReturn( testScheme );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( testHost );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ) ).thenReturn( testPort );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsUsername() ) ).thenReturn( testUsername );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsPassword() ) ).thenReturn( testPassword );
    buildAppendEncodedUserPassMocks( testUsername, testPassword );
    assertEquals( testScheme + "://" + testUsername + ":" + testPassword + "@" + testHost + ":" + testPort,
      namedCluster.generateURL( "${" + schemeVar + "}", metaStore, variableSpace ) );
  }

  @Test
  public void testGenerateURLHDFSVariableSpace_noVariable() throws MetaStoreException {
    String scheme = "hdfs";
    String hostVar = "hostVar";
    String portVar = "portVar";
    String usernameVar = "usernameVar";
    String passwordVar = "passwordVar";
    namedCluster.setStorageScheme( "${" + scheme + "}" );
    namedCluster.setHdfsHost( "${" + hostVar + "}" );
    namedCluster.setHdfsPort( "${" + portVar + "}" );
    namedCluster.setHdfsUsername( "${" + usernameVar + "}" );
    namedCluster.setHdfsPassword( "${" + passwordVar + "}" );
    assertEquals( scheme + ":", namedCluster.generateURL( scheme, metaStore, variableSpace ) );
  }

  @Test
  public void testXMLEmbedding() throws Exception {
    Element node = createNodeFromNamedCluster();

    NamedCluster nc = new NamedClusterImpl();
    nc = nc.fromXmlForEmbed( node );

    assertNamedClusterEquality( nc );
  }

  @Test
  public void testLegacyXMLEmbedding() throws Exception {
    Element node = createNodeFromNamedCluster();

    XPath xPath = XPathFactory.newInstance().newXPath();
    //Find the node containing the hdfsPassword
    Node n = ( (Node) xPath.evaluate( "/NamedCluster/child/id[text()='hdfsPassword']", node, XPathConstants.NODE ) )
      .getNextSibling();
    //Set the password value to what it would be if we were still encoding the legacy way
    ITwoWayPasswordEncoder passwordEncoder = new Base64TwoWayPasswordEncoder();
    n.setTextContent( passwordEncoder.encode( namedCluster.getHdfsPassword() ) );

    //Now check that we can still decode it
    NamedCluster nc = new NamedClusterImpl();
    nc = nc.fromXmlForEmbed( node );

    assertNamedClusterEquality( nc );
  }

  private Element createNodeFromNamedCluster() throws Exception {
    String clusterXml = namedCluster.toXmlForEmbed( "NamedCluster" );
    System.out.println( clusterXml );

    return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
        new ByteArrayInputStream( clusterXml.getBytes() ) )
        .getDocumentElement();
  }

  private void assertNamedClusterEquality( NamedCluster nc ) {

    assertEquals( namedCluster.getHdfsHost(), nc.getHdfsHost() );
    assertEquals( namedCluster.getHdfsPort(), nc.getHdfsPort() );
    assertEquals( namedCluster.getHdfsUsername(), nc.getHdfsUsername() );
    assertEquals( namedCluster.getHdfsPassword(), nc.getHdfsPassword() );
    assertEquals( namedCluster.getName(), nc.getName() );
    assertEquals( namedCluster.getShimIdentifier(), nc.getShimIdentifier() );
    assertEquals( namedCluster.getStorageScheme(), nc.getStorageScheme() );
    assertEquals( namedCluster.getJobTrackerHost(), nc.getJobTrackerHost() );
    assertEquals( namedCluster.getJobTrackerPort(), nc.getJobTrackerPort() );
    assertEquals( namedCluster.getZooKeeperHost(), nc.getZooKeeperHost() );
    assertEquals( namedCluster.getZooKeeperPort(), nc.getZooKeeperPort() );
    assertEquals( namedCluster.getOozieUrl(), nc.getOozieUrl() );
    assertEquals( namedCluster.getKafkaBootstrapServers(), nc.getKafkaBootstrapServers() );
    assertEquals( namedCluster.getLastModifiedDate(), nc.getLastModifiedDate() );
    assertEquals( namedCluster.getSiteFiles().size(), nc.getSiteFiles().size() );
    for ( NamedClusterSiteFile siteFile : namedCluster.getSiteFiles() ) {
      String contents = getSiteFileContents( nc, siteFile.getSiteFileName() );
      assertEquals( siteFile.getSiteFileContents(), contents );
    }
  }

  private Answer buildSchemeAnswer( String prefix, String buildPath ) {
    Answer extractSchemeAnswer = invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[2] ).append( buildPath );
      return prefix;
    };
    return extractSchemeAnswer;
  }

  private Answer buildUrlEncodeAnswer( String value ) {
    Answer urlEncodeAnswer = invocation -> {
      Object[] args = invocation.getArguments();
      ( (StringBuilder) args[0] ).append( (String) args[1] );
      return null;
    };
    return urlEncodeAnswer;
  }

  private void buildExtractSchemeMocks( String prefix, String fullPath, String pathWithoutPrefix ) throws Exception {
    String[] schemes = { "hc", "hdfs", "maprfs", "wasb" };
    when( fsm.getSchemes() ).thenReturn( schemes );
    doAnswer( buildSchemeAnswer( prefix, pathWithoutPrefix ) ).when( UriParser.class, "extractScheme",
      eq( schemes ), eq( fullPath ), any( StringBuilder.class ) );
  }

  private void buildAppendEncodedUserPassMocks( String username, String password ) throws Exception {
    doAnswer( buildUrlEncodeAnswer( username ) ).when( UriParser.class, "appendEncoded",
      any( StringBuilder.class ), eq( username ), any( char[].class ) );
    doAnswer( buildUrlEncodeAnswer( password ) ).when( UriParser.class, "appendEncoded",
      any( StringBuilder.class ), eq( password ), any( char[].class ) );
  }

  private String getSiteFileContents( NamedCluster nc, String siteFileName ) {
    NamedClusterSiteFile n = nc.getSiteFiles().stream().filter( sf -> sf.getSiteFileName().equals( siteFileName ) )
      .findFirst().orElse( null );
    return n == null ? null : n.getSiteFileContents();
  }
}
