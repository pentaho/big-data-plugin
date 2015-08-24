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

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTesterImpl implements ClusterTester {
  private final List<ClusterTest> clusterTests;
  private final ExecutorService executorService;
  private final ClusterTestRunner.Factory clusterTestRunnerFactory;
  private ClusterTestComparator clusterTestComparator;

  public ClusterTesterImpl( List<ClusterTest> clusterTests, ExecutorService executorService,
                            String orderedModulesString ) {
    this( clusterTests, executorService, orderedModulesString, new ClusterTestRunner.Factory() );
  }

  public ClusterTesterImpl( List<ClusterTest> clusterTests, ExecutorService executorService,
                            String orderedModulesString, ClusterTestRunner.Factory clusterTestRunnerFactory ) {
    this.clusterTests = clusterTests;
    this.executorService = executorService;
    this.clusterTestRunnerFactory = clusterTestRunnerFactory;
    HashMap<String, Integer> orderedModules = new HashMap<>();
    String[] split = orderedModulesString.split( "," );
    for ( int module = 0; module < split.length; module++ ) {
      orderedModules.put( split[ module ].trim(), module );
    }
    clusterTestComparator = new ClusterTestComparator( orderedModules );
  }

  @Override
  public void testCluster( final NamedCluster namedCluster,
                           final ClusterTestProgressCallback clusterTestProgressCallback ) {
    final List<ClusterTest> clusterTests = new ArrayList<>( this.clusterTests );
    Collections.sort( clusterTests, clusterTestComparator );
    executorService.submit( new Runnable() {
      @Override public void run() {
        clusterTestRunnerFactory.create( clusterTests, namedCluster, clusterTestProgressCallback, executorService )
          .runTests();
      }
    } );
  }
}
