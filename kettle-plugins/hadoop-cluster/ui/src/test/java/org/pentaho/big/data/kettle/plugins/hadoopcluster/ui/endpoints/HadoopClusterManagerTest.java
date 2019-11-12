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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import com.google.common.collect.ImmutableList;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.runtime.test.RuntimeTestStatus;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class HadoopClusterManagerTest {
  @Mock private Spoon spoon;
  @Mock private NamedClusterService namedClusterService;
  @Mock private DelegatingMetaStore metaStore;
  @Mock private NamedCluster namedCluster;
  @Mock private ShimIdentifierInterface cdhShim;
  @Mock private ShimIdentifierInterface internalShim;
  @Mock private ShimIdentifierInterface maprShim;
  private String ncTestName = "ncTest";
  private HadoopClusterManager hadoopClusterManager;

  @Before public void setup() throws Exception {
    if ( getShimTestDir().exists() ) {
      FileUtils.deleteDirectory( getShimTestDir() );
    }
    when( cdhShim.getId() ).thenReturn( "cdh514" );
    when( cdhShim.getVendor() ).thenReturn( "Cloudera" );
    when( cdhShim.getVersion() ).thenReturn( "5.14" );
    when( internalShim.getId() ).thenReturn( "apache" );
    when( internalShim.getVendor() ).thenReturn( "apache" );
    when( internalShim.getVersion() ).thenReturn( "3.1" );
    when( maprShim.getVendor() ).thenReturn( "MapR" );
    when( maprShim.getVersion() ).thenReturn( "4.6" );
    when( maprShim.getId() ).thenReturn( "mapr46" );

    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    when( spoon.getMetaStore() ).thenReturn( metaStore );
    when( namedCluster.getName() ).thenReturn( ncTestName );
    when( namedClusterService.getNamedClusterByName( ncTestName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.getShimIdentifier() ).thenReturn( "cdh514" );
    when( namedClusterService.contains( ncTestName, metaStore ) ).thenReturn( false );
    when( namedClusterService.contains( "existingName", metaStore ) ).thenReturn( true );
    hadoopClusterManager = new HadoopClusterManager( spoon, namedClusterService, metaStore, "apache" );

    hadoopClusterManager.shimIdentifiersSupplier = () -> ImmutableList.of( cdhShim, internalShim, maprShim );
  }

  @Test public void testSecuredImportNamedCluster() {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    model.setImportPath( "src/test/resources/secured" );
    model.setShimVendor( "Cloudera" );
    model.setShimVersion( "5.14" );
    JSONObject result = hadoopClusterManager.importNamedCluster( model );
    assertEquals( ncTestName, result.get( "namedCluster" ) );
    assertTrue( new File( getShimTestDir(), "core-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "yarn-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "hive-site.xml" ).exists() );
  }

  @Test public void testUnsecuredImportNamedCluster() {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    model.setImportPath( "src/test/resources/unsecured" );
    model.setShimVendor( "Cloudera" );
    model.setShimVersion( "5.14" );
    JSONObject result = hadoopClusterManager.importNamedCluster( model );
    assertEquals( ncTestName, result.get( "namedCluster" ) );
    assertTrue( new File( getShimTestDir(), "core-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "yarn-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "hive-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "oozie-default.xml" ).exists() );
  }

  @Test public void testMissingInfoImportNamedCluster() {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    model.setImportPath( "src/test/resources/missing-info" );
    model.setShimVendor( "Cloudera" );
    model.setShimVersion( "5.14" );
    JSONObject result = hadoopClusterManager.importNamedCluster( model );
    assertEquals( ncTestName, result.get( "namedCluster" ) );
    assertTrue( new File( getShimTestDir(), "core-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "yarn-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "hive-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "oozie-default.xml" ).exists() );
    ThinNameClusterModel thinNameClusterModel = hadoopClusterManager.getNamedCluster( ncTestName );
    assertTrue( StringUtil.isEmpty( thinNameClusterModel.getHdfsHost() ) );
    assertTrue( StringUtil.isEmpty( thinNameClusterModel.getHdfsPort() ) );
    assertTrue( StringUtil.isEmpty( thinNameClusterModel.getJobTrackerPort() ) );
  }

  @Test public void testCreateNamedCluster() {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    JSONObject result = hadoopClusterManager.createNamedCluster( model );
    assertEquals( ncTestName, result.get( "namedCluster" ) );
  }

  @Test public void testEditNamedCluster() {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    model.setOldName( ncTestName );
    JSONObject result = hadoopClusterManager.editNamedCluster( model, true );
    assertEquals( ncTestName, result.get( "namedCluster" ) );
  }

  @Test public void testFailNamedCluster() {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    model.setImportPath( "src/test/resources/bad" );
    model.setShimVendor( "Claudera" );
    model.setShimVersion( "5.14" );
    JSONObject result = hadoopClusterManager.importNamedCluster( model );
    assertEquals( "", result.get( "namedCluster" ) );
  }

  @Test public void testGetShimIdentifiers() {
    List<ShimIdentifierInterface> shimIdentifiers = hadoopClusterManager.getShimIdentifiers();
    assertNotNull( shimIdentifiers );
    assertThat( shimIdentifiers.size(), equalTo( 2 ) );
    assertFalse( shimIdentifiers.contains( internalShim ) );
  }

  @Test public void testInstallDriver() {
    System.getProperties()
      .setProperty( Const.SHIM_DRIVER_DEPLOYMENT_LOCATION, "src/test/resources/driver-destination" );
    JSONObject response = hadoopClusterManager.installDriver( "src/test/resources/driver-source/driver.kar" );
    boolean isSuccess = (boolean) response.get( "installed" );
    if ( isSuccess ) {
      File driver = new File( "src/test/resources/driver-destination/driver.kar" );
      assertTrue( driver.exists() );
    }
  }

  @Test public void testRunTests() {
    RuntimeTestStatus runtimeTestStatus = mock( RuntimeTestStatus.class );
    when( namedClusterService.getNamedClusterByName( ncTestName, this.metaStore ) ).thenReturn( namedCluster );
    when( runtimeTestStatus.isDone() ).thenReturn( true );
    when( namedCluster.getOozieUrl() ).thenReturn( "" );
    when( namedCluster.getKafkaBootstrapServers() ).thenReturn( "" );
    when( namedCluster.getZooKeeperHost() ).thenReturn( "" );
    when( namedCluster.getJobTrackerHost() ).thenReturn( "" );

    hadoopClusterManager.onProgress( runtimeTestStatus );
    Object[] categories = (Object[]) hadoopClusterManager.runTests( null, ncTestName );

    for ( Object category : categories ) {
      TestCategory testCategory = (TestCategory) category;
      String categoryName = testCategory.getCategoryName();
      boolean isCategoryNameValid = false;
      if ( categoryName.equals( "Hadoop file system" ) || categoryName.equals( "Oozie host connection" ) || categoryName
        .equals( "Kafka connection" ) || categoryName.equals( "Zookeeper connection" ) || categoryName
        .equals( "Job tracker / resource manager" ) ) {
        isCategoryNameValid = true;
      }
      assertTrue( isCategoryNameValid );
      assertFalse( testCategory.isCategoryActive() );
      List<org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.Test> tests = testCategory.getTests();
      for ( org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.Test test : tests ) {
        assertEquals( "Warning", test.getTestStatus() );
      }
    }
  }

  @Test public void testNamedClusterKerberosPasswordSecurity() throws ConfigurationException {
    ThinNameClusterModel model = new ThinNameClusterModel();
    model.setName( ncTestName );
    model.setSecurityType( "Kerberos" );
    model.setKerberosSubType( "Password" );
    model.setKerberosAuthenticationUsername( "username" );
    model.setKerberosAuthenticationPassword( "password" );
    model.setKerberosImpersonationUsername( "impersonationusername" );
    model.setKerberosImpersonationPassword( "impersonationpassword" );

    hadoopClusterManager.createNamedCluster( model );

    String configFile = System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore"
      + File.separator + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs" + File.separator
      + "ncTest" + File.separator + "config.properties";

    PropertiesConfiguration config = new PropertiesConfiguration( new File( configFile ) );
    assertEquals( "username", config.getProperty( "pentaho.authentication.default.kerberos.principal" ) );
    assertEquals( "password", config.getProperty( "pentaho.authentication.default.kerberos.password" ) );
    assertEquals( "impersonationusername",
      config.getProperty( "pentaho.authentication.default.mapping.server.credentials.kerberos.principal" ) );
    assertEquals( "impersonationpassword",
      config.getProperty( "pentaho.authentication.default.mapping.server.credentials.kerberos.password" ) );
    assertEquals( "simple", config.getProperty( "pentaho.authentication.default.mapping.impersonation.type" ) );

    ThinNameClusterModel retrievingModel = hadoopClusterManager.getNamedCluster( ncTestName );
    assertEquals( "Kerberos", retrievingModel.getSecurityType() );
    assertEquals( "Password", retrievingModel.getKerberosSubType() );
    assertEquals( "username", retrievingModel.getKerberosAuthenticationUsername() );
    assertEquals( "password", retrievingModel.getKerberosAuthenticationPassword() );
    assertEquals( "impersonationusername", retrievingModel.getKerberosImpersonationUsername() );
    assertEquals( "impersonationpassword", retrievingModel.getKerberosImpersonationPassword() );
  }

  @After public void tearDown() throws IOException {
    FileUtils.deleteDirectory( getShimTestDir() );
    FileUtils.deleteDirectory( new File( "src/test/resources/driver-destination" ) );
  }

  private File getShimTestDir() {
    String
      shimTestDir =
      System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore" + File.separator
        + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs" + File.separator + ncTestName;
    return new File( shimTestDir );
  }
}
