/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.di.core.namedcluster;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.metastore.persist.MetaStoreFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 9/25/15.
 */
public class NamedClusterManagerTest {
  private IMetaStore metaStore;
  private NamedClusterManager namedClusterManager;
  private VariableSpace variableSpace;
  private NamedClusterManager.MetaStoreFactoryFactory metaStoreFactoryFactory;
  private MetaStoreFactory<NamedCluster> metaStoreFactory;

  @Before
  @SuppressWarnings( "unchecked" )
  public void setup() {
    metaStore = mock( IMetaStore.class );
    variableSpace = mock( VariableSpace.class );
    metaStoreFactoryFactory = mock( NamedClusterManager.MetaStoreFactoryFactory.class );
    metaStoreFactory = mock( MetaStoreFactory.class );
    when( metaStoreFactoryFactory.createFactory( metaStore ) ).thenReturn( metaStoreFactory );
    namedClusterManager = new NamedClusterManager( metaStoreFactoryFactory );
  }

  @Test
  public void testGenerateURLNullParameters() {
    assertNull( namedClusterManager.generateURL( null, "testName", metaStore, null ) );
    assertNull( namedClusterManager.generateURL( "testScheme", null, metaStore, null ) );
    assertNull( namedClusterManager.generateURL( "testScheme", "testName", null, null ) );
    assertNull( namedClusterManager.generateURL( "testScheme", "testName", metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFS() throws MetaStoreException {
    String scheme = "hdfs";
    String testName = "testName";
    String testHost = "testHost";
    String testPort = "9333";
    String testUsername = "testUsername";
    String testPassword = "testPassword";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getHdfsHost() ).thenReturn( " " + testHost + " " );
    when( namedCluster.getHdfsPort() ).thenReturn( " " + testPort + " " );
    when( namedCluster.getHdfsUsername() ).thenReturn( " " + testUsername + " " );
    when( namedCluster.getHdfsPassword() ).thenReturn( " " + testPassword + " " );
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertEquals( scheme + "://" + testUsername + ":" + testPassword + "@" + testHost + ":" + testPort,
      namedClusterManager.generateURL( scheme, testName, metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFSPort() throws MetaStoreException {
    String scheme = "hdfs";
    String testName = "testName";
    String testHost = "testHost";
    String testPort = "9333";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getHdfsHost() ).thenReturn( " " + testHost + " " );
    when( namedCluster.getHdfsPort() ).thenReturn( " " + testPort + " " );
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertEquals( scheme + "://" + testHost + ":" + testPort,
      namedClusterManager.generateURL( scheme, testName, metaStore, null ) );
  }

  @Test
  public void testProcessURLsubstitutionMaprFS_startsWithMaprfs() throws MetaStoreException {
    String incomingURL = "maprfs";
    String testName = "testName";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.isMapr() ).thenReturn( true );
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertEquals( incomingURL, namedClusterManager.processURLsubstitution( testName, incomingURL, incomingURL, metaStore, null ) );
  }

  @Test
  public void testProcessURLsubstitutionMaprFS_startsWithNoMaprfs() throws MetaStoreException {
    String incomingURL = "path";
    String testName = "testName";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.isMapr() ).thenReturn( true );
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertNull(  namedClusterManager.processURLsubstitution( testName, incomingURL, incomingURL, metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFSNoPort() throws MetaStoreException {
    String scheme = "hdfs";
    String testName = "testName";
    String testHost = "testHost";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getHdfsHost() ).thenReturn( " " + testHost + " " );
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertEquals( scheme + "://" + testHost, namedClusterManager.generateURL( scheme, testName, metaStore, null ) );
  }

  @Test
  public void testGenerateURLHDFSVariableSpace() throws MetaStoreException {
    String scheme = "hdfs";
    String testName = "testName";
    String hostVar = "hostVar";
    String testHost = "testHost";
    String portVar = "portVar";
    String testPort = "9333";
    String usernameVar = "usernameVar";
    String testUsername = "testUsername";
    String passwordVar = "passwordVar";
    String testPassword = "testPassword";
    NamedCluster namedCluster = mock( NamedCluster.class );
    when( namedCluster.getHdfsHost() ).thenReturn( "${" + hostVar + "}" );
    when( namedCluster.getHdfsPort() ).thenReturn( "${" + portVar + "}" );
    when( namedCluster.getHdfsUsername() ).thenReturn( "${" + usernameVar + "}" );
    when( namedCluster.getHdfsPassword() ).thenReturn( "${" + passwordVar + "}" );
    when( variableSpace.getVariable( hostVar ) ).thenReturn( testHost );
    when( variableSpace.getVariable( portVar ) ).thenReturn( testPort );
    when( variableSpace.getVariable( usernameVar ) ).thenReturn( testUsername );
    when( variableSpace.getVariable( passwordVar ) ).thenReturn( testPassword );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsHost() ) ).thenReturn( testHost );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsPort() ) ).thenReturn( testPort );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsUsername() ) ).thenReturn( testUsername );
    when( variableSpace.environmentSubstitute( namedCluster.getHdfsPassword() ) ).thenReturn( testPassword );
    when( metaStoreFactory.loadElement( testName ) ).thenReturn( namedCluster );
    assertEquals( scheme + "://" + testUsername + ":" + testPassword + "@" + testHost + ":" + testPort,
      namedClusterManager.generateURL( scheme, testName, metaStore, variableSpace ) );
  }
}
