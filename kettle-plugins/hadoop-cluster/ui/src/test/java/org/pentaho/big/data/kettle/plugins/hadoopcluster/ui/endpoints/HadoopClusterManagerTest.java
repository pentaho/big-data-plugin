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

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.ShimIdentifierInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.stores.delegate.DelegatingMetaStore;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.RuntimeTester;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HadoopClusterManagerTest {
  private Spoon spoon;
  private NamedClusterService namedClusterService;
  private DelegatingMetaStore metaStore;
  private NamedCluster namedCluster;
  private List<ShimIdentifierInterface> shimIdentifiers;
  private String ncTestName = "ncTest";

  @Before public void setup() throws IOException {
    if ( getShimTestDir().exists() ) {
      FileUtils.deleteDirectory( getShimTestDir() );
    }

    spoon = mock( Spoon.class );
    namedClusterService = mock( NamedClusterService.class );
    metaStore = mock( DelegatingMetaStore.class );
    namedCluster = mock( NamedCluster.class );
    shimIdentifiers = new ArrayList<>();
    ShimIdentifierInterface shimIdentifier = mock( ShimIdentifierInterface.class );
    shimIdentifier.setId( "chd514" );
    shimIdentifiers.add( shimIdentifier );
    when( namedClusterService.getClusterTemplate() ).thenReturn( namedCluster );
    when( spoon.getMetaStore() ).thenReturn( metaStore );
    when( namedCluster.getName() ).thenReturn( ncTestName );
  }

  @Test public void testCreateNamedCluster() {
    HadoopClusterManager hadoopClusterManager = new HadoopClusterManager( spoon, namedClusterService );
    JSONObject
        result =
        hadoopClusterManager.createNamedCluster( ncTestName, "site", "src/test/resources", "Claudera", "5.14" );
    assertEquals( result.get( "namedCluster" ), ncTestName );
    assertTrue( new File( getShimTestDir(), "core-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "yarn-site.xml" ).exists() );
    assertTrue( new File( getShimTestDir(), "hive-site.xml" ).exists() );
  }

  @Test public void testFailNamedCluster() {
    HadoopClusterManager hadoopClusterManager = new HadoopClusterManager( spoon, namedClusterService );
    JSONObject
        result =
        hadoopClusterManager.createNamedCluster( ncTestName, "site", "src/test/resources/bad", "Claudera", "5.14" );
    assertEquals( "", result.get( "namedCluster" ) );
  }

  @Test public void testGetShimIdentifiers() {
    HadoopClusterManager hadoopClusterManager = new HadoopClusterManager( spoon, namedClusterService );
    List<ShimIdentifierInterface> shimIdentifiers = hadoopClusterManager.getShimIdentifiers();
    assertTrue( shimIdentifiers != null );
  }

  @Test public void testRunTests() {
    RuntimeTestStatus runtimeTestStatus = mock( RuntimeTestStatus.class );
    when( namedClusterService.getNamedClusterByName( ncTestName, this.metaStore ) ).thenReturn( namedCluster );
    when( runtimeTestStatus.isDone() ).thenReturn( true );

    HadoopClusterManager hadoopClusterManager = new HadoopClusterManager( spoon, namedClusterService );
    hadoopClusterManager.onProgress( runtimeTestStatus );
    Object[] categories = (Object[]) hadoopClusterManager.runTests( null, ncTestName );

    for ( Object category : categories ) {
      TestCategory testCategory = (TestCategory) category;
      String categoryName = testCategory.getCategoryName();
      boolean isCategoryNameValid = false;
      if ( categoryName.equals( "Hadoop file system" ) || categoryName.equals( "Oozie host connection" ) || categoryName
          .equals( "Kafka connection" ) || categoryName.equals( "Zookeeper connection" ) || categoryName
          .equals( "Job tracker / resource manager" ) || categoryName.equals( "Pentaho big data shim" ) ) {
        isCategoryNameValid = true;
      }
      assertTrue( isCategoryNameValid );
    }
  }

  @After public void tearDown() throws IOException {
    FileUtils.deleteDirectory( getShimTestDir() );
  }

  private File getShimTestDir() {
    String
        shimTestDir =
        System.getProperty( "user.home" ) + File.separator + ".pentaho" + File.separator + "metastore" + File.separator
            + "pentaho" + File.separator + "NamedCluster" + File.separator + "Configs" + File.separator + ncTestName;
    return new File( shimTestDir );
  }
}
