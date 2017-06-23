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

import java.io.ByteArrayInputStream;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.w3c.dom.Element;

import static com.google.code.beanmatchers.BeanMatchers.hasValidBeanConstructor;
import static com.google.code.beanmatchers.BeanMatchers.hasValidBeanEqualsFor;
import static com.google.code.beanmatchers.BeanMatchers.hasValidGettersAndSetters;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 7/14/15.
 */
public class NamedClusterImplTest {
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
    namedClusterStorageScheme = "hdfs";
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
    namedCluster.setStorageScheme( namedClusterStorageScheme );
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
  public void testXMLEmbedding() throws Exception {
    String clusterXml = namedCluster.toXmlForEmbed( "NamedCluster" );
    System.out.println( clusterXml );

    Element node =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
            new ByteArrayInputStream( clusterXml.getBytes() ) )
            .getDocumentElement();

    NamedCluster nc = new NamedClusterImpl();
    nc = nc.fromXmlForEmbed( node );
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
    assertEquals( namedCluster.getLastModifiedDate(), nc.getLastModifiedDate() );
  }
}
