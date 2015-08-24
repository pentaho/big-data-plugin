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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.ClusterTestProgressCallback;
import org.pentaho.big.data.api.clusterTest.ClusterTestStatus;
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResult;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;
import org.pentaho.big.data.api.clusterTest.test.impl.BaseClusterTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/12/15.
 */
public class ClusterTestRunnerTest {
  private ExecutorService executorService;
  private TestClusterTest moduleATestA;
  private TestClusterTest moduleATestB;
  private TestClusterTest moduleATestC;
  private TestClusterTest moduleBTestA;
  private TestClusterTest moduleBTestB;
  private TestClusterTest moduleBTestC;
  private NamedCluster namedCluster;
  private TestClusterTest unsatisfiableDependencyA;
  private TestClusterTest moduleCTestA;
  private TestClusterTest moduleATestD;

  private static Set<String> dependenciesToIds( Set<TestClusterTest> testClusterTests ) {
    Set<String> result = new HashSet<>();
    for ( TestClusterTest testClusterTest : testClusterTests ) {
      result.add( testClusterTest.getId() );
    }
    return result;
  }

  @Before
  public void setup() {
    executorService = Executors.newCachedThreadPool();
    unsatisfiableDependencyA = new TestClusterTest( "unsatisfiableDependency", "unsatisfiableDependencyTestA", "Test A",
      new HashSet<>( Arrays.asList(
        new TestClusterTest( "fake-module", "fake-test-id", "fake-getName", new HashSet<TestClusterTest>(), 5,
          new ArrayList<ClusterTestResultEntry>(), false ) ) ), 5, new ArrayList<ClusterTestResultEntry>(),
      false );
    moduleATestA =
      new TestClusterTest( "moduleA", "moduleATestA", "Test A", new HashSet<>( Arrays.<TestClusterTest>asList() ), 5,
        new ArrayList<ClusterTestResultEntry>(), true );
    moduleATestB =
      new TestClusterTest( "moduleA", "moduleATestB", "Test B", new HashSet<>( Arrays.asList( moduleATestA ) ), 5,
        new ArrayList<ClusterTestResultEntry>(), true );
    moduleATestC =
      new TestClusterTest( "moduleA", "moduleATestC", "Test C", new HashSet<>( Arrays.asList( moduleATestB ) ), 5,
        new ArrayList<ClusterTestResultEntry>(), true );
    moduleATestD =
      new TestClusterTest( "moduleA", "moduleATestD", "Test D", new HashSet<>( Arrays.asList( moduleATestB ) ), 5,
        new ArrayList<ClusterTestResultEntry>(), true );
    moduleBTestA =
      new TestClusterTest( "moduleB", "moduleBTestA", "Test A", new HashSet<>( Arrays.asList( moduleATestA ) ), 5,
        new ArrayList<ClusterTestResultEntry>(), true );
    moduleBTestB =
      new TestClusterTest( "moduleB", "moduleBTestB", "Test B", new HashSet<>( Arrays.asList( moduleATestC ) ), 5,
        new ArrayList<ClusterTestResultEntry>(), true );
    moduleBTestC =
      new TestClusterTest( "moduleB", "moduleBTestC", "Test C",
        new HashSet<>( Arrays.asList( moduleBTestB, moduleATestC ) ),
        5, new ArrayList<ClusterTestResultEntry>(), true );
    moduleCTestA = new TestClusterTest( "moduleC", "moduleCTestA", "Test A",
      new HashSet<>( Arrays.asList( moduleBTestC, moduleATestC ) ),
      5, new ArrayList<ClusterTestResultEntry>(), true );
    namedCluster = mock( NamedCluster.class );
  }

  @After
  public void tearDown() {
    executorService.shutdown();
  }

  @Test
  public void testSingleTestNoDependencies() {
    testScenario( Arrays.asList( moduleATestA ) );
  }

  @Test
  public void testSingleTestWithDependencies() {
    testScenario( Arrays.asList( unsatisfiableDependencyA ) );
  }

  @Test
  public void testModuleA() {
    testScenario( Arrays.asList( moduleATestA, moduleATestB, moduleATestC, moduleATestD ) );
  }

  @Test
  public void testModuleAAndB() {
    testScenario( Arrays
      .asList( moduleATestA, moduleATestB, moduleATestC, moduleATestD, moduleBTestA, moduleBTestB, moduleBTestC ) );
  }

  @Test
  public void testModuleAthruC() {
    testScenario( Arrays
      .asList( moduleATestA, moduleATestB, moduleATestC, moduleATestD, moduleBTestA, moduleBTestB, moduleBTestC,
        moduleCTestA ) );
  }

  @Test
  public void testModuleAthruCUnsat() {
    testScenario( Arrays
      .asList( moduleATestA, moduleATestB, moduleATestC, moduleATestD, moduleBTestA, moduleBTestB, moduleBTestC,
        moduleCTestA,
        unsatisfiableDependencyA ) );
  }

  private void testScenario( List<TestClusterTest> clusterTests ) {
    final List<ClusterTestStatus> clusterTestStatuses = Collections.synchronizedList( new ArrayList
      <ClusterTestStatus>() );
    final ClusterTestProgressCallback clusterTestProgressCallback = new ClusterTestProgressCallback() {
      @Override public void onProgress( ClusterTestStatus clusterTestStatus ) {
        clusterTestStatuses.add( clusterTestStatus );
        if ( clusterTestStatus.isDone() ) {
          synchronized ( this ) {
            notifyAll();
          }
        }
      }
    };
    long before = System.currentTimeMillis();
    new ClusterTestRunner( clusterTests, namedCluster, clusterTestProgressCallback, executorService ).runTests();
    synchronized ( clusterTestProgressCallback ) {
      while ( clusterTestStatuses.size() == 0 || !clusterTestStatuses.get( clusterTestStatuses.size() - 1 ).isDone() ) {
        try {
          clusterTestProgressCallback.wait();
        } catch ( InterruptedException e ) {
          // Ignore
        }
      }
    }
    long after = System.currentTimeMillis();
    Set<String> doneIds = new HashSet<>();
    for ( int i = 0; i < clusterTestStatuses.size(); i++ ) {
      ClusterTestStatus clusterTestStatus = clusterTestStatuses.get( i );
      if ( i < clusterTestStatuses.size() - 1 ) {
        assertFalse( clusterTestStatus.isDone() );
      } else {
        assertTrue( clusterTestStatus.isDone() );
      }
      Set<String> justDoneIds = new HashSet<>();
      for ( ClusterTestModuleResults clusterTestModuleResults : clusterTestStatus.getModuleResults() ) {
        Set<String> outstandingIds = new HashSet<>();
        Set<String> runningIds = new HashSet<>();
        for ( ClusterTest clusterTest : clusterTestModuleResults.getOutstandingTests() ) {
          outstandingIds.add( clusterTest.getId() );
        }
        for ( ClusterTest clusterTest : clusterTestModuleResults.getRunningTests() ) {
          runningIds.add( clusterTest.getId() );
        }

        Set<String> resultIds = new HashSet<>();
        for ( ClusterTestResult clusterTestResult : clusterTestModuleResults.getClusterTestResults() ) {
          resultIds.add( clusterTestResult.getClusterTest().getId() );
        }
        // We should have results for all ids in module
        assertTrue( resultIds.containsAll( outstandingIds ) );
        assertTrue( resultIds.containsAll( runningIds ) );

        // No done ides should be in outstanding or running
        assertTrue( Collections.disjoint( doneIds, outstandingIds ) );
        assertTrue( Collections.disjoint( doneIds, runningIds ) );

        resultIds.removeAll( outstandingIds );
        resultIds.removeAll( runningIds );
        justDoneIds.addAll( resultIds );
      }
      // All previously done ids should still be done
      assertTrue( justDoneIds.containsAll( doneIds ) );
      // We should get called back for each one that finishes
      assertTrue( justDoneIds.size() == doneIds.size() || justDoneIds.size() == doneIds.size() + 1 );

      doneIds.addAll( justDoneIds );
    }
    for ( TestClusterTest clusterTest : clusterTests ) {
      assertTrue( doneIds.contains( clusterTest.getId() ) );
      clusterTest.validateRunState();
    }
    System.out.println( "Ran in " + ( after - before ) + " ms" );
    System.out.flush();
  }

  public class TestClusterTest extends BaseClusterTest {
    private final long delay;
    private final Set<TestClusterTest> dependencies;
    private final AtomicBoolean hasRun;
    private final List<ClusterTestResultEntry> clusterTestResultEntries;
    private final boolean shouldRun;

    public TestClusterTest( String module, String id, String name, Set<TestClusterTest> dependencies,
                            long delay, List<ClusterTestResultEntry> clusterTestResultEntries, boolean shouldRun ) {
      super( module, id, name, dependenciesToIds( dependencies ) );
      this.delay = delay;
      this.dependencies = dependencies;
      this.clusterTestResultEntries = clusterTestResultEntries;
      this.shouldRun = shouldRun;
      hasRun = new AtomicBoolean( false );
    }

    public String getLogName() {
      return getModule() + ":" + getId();
    }

    @Override public List<ClusterTestResultEntry> runTest( NamedCluster namedCluster ) {
      assertTrue( shouldRun );
      assertEquals( ClusterTestRunnerTest.this.namedCluster, namedCluster );
      String logName = getLogName();
      System.out.println( "Running: " + logName );
      for ( TestClusterTest dependency : dependencies ) {
        assertTrue( logName + " expected dependency " + dependency.getLogName() + " to have already run",
          dependency.hasRun.get() );
      }
      try {
        Thread.sleep( delay );
      } catch ( InterruptedException e ) {
        // Ignore
      }
      hasRun.set( true );
      System.out.println( "Done running: " + logName );
      return clusterTestResultEntries;
    }

    public void validateRunState() {
      String moduleString = getLogName();
      assertEquals( "Expected " + moduleString + " hasRun value of " + shouldRun + " but was " + hasRun.get(),
        shouldRun, hasRun.get() );
      System.out.println( "Got correct shouldRun value of " + shouldRun + " from " + moduleString );
    }
  }
}
