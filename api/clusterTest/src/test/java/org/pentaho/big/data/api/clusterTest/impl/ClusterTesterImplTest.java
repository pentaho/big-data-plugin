/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.api.clusterTest.impl;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTesterImplTest {

  private ClusterTesterImpl clusterTester;
  private List<ClusterTest> clusterTests;
  private ExecutorService executorService;
  private String orderedModulesString;
  private ClusterTestRunner.Factory clusterTestRunnerFactory;

  @Before
  public void setup() {
    clusterTests = new ArrayList<>( Arrays.asList( mock( ClusterTest.class ) ) );
    executorService = mock( ExecutorService.class );
    orderedModulesString = "test-modules";
    clusterTestRunnerFactory = mock( ClusterTestRunner.Factory.class );
    clusterTester =
      new ClusterTesterImpl( clusterTests, executorService, orderedModulesString, clusterTestRunnerFactory );
  }

  @Test
  public void testRunTests() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    ClusterTestProgressCallback clusterTestProgressCallback = mock( ClusterTestProgressCallback.class );
    clusterTester.testCluster( namedCluster, clusterTestProgressCallback );
    ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass( Runnable.class );
    verify( executorService ).submit( runnableArgumentCaptor.capture() );
    ClusterTestRunner clusterTestRunner = mock( ClusterTestRunner.class );
    when( clusterTestRunnerFactory.create( clusterTests, namedCluster, clusterTestProgressCallback, executorService ) )
      .thenReturn(
        clusterTestRunner );
    runnableArgumentCaptor.getValue().run();
    verify( clusterTestRunner ).runTests();
  }
}
