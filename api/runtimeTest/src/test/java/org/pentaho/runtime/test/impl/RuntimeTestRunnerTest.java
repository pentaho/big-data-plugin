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

package org.pentaho.runtime.test.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestProgressCallback;
import org.pentaho.runtime.test.RuntimeTestStatus;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.BaseRuntimeTest;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

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

/**
 * Created by bryan on 8/12/15.
 */
public class RuntimeTestRunnerTest {
  private ExecutorService executorService;
  private TestRuntimeTest moduleATestA;
  private TestRuntimeTest moduleATestB;
  private TestRuntimeTest moduleATestC;
  private TestRuntimeTest moduleBTestA;
  private TestRuntimeTest moduleBTestB;
  private TestRuntimeTest moduleBTestC;
  private Object objectUnderTest;
  private TestRuntimeTest unsatisfiableDependencyA;
  private TestRuntimeTest moduleCTestA;
  private TestRuntimeTest moduleATestD;

  private static Set<String> dependenciesToIds( Set<TestRuntimeTest> testRuntimeTests ) {
    Set<String> result = new HashSet<>();
    for ( TestRuntimeTest testRuntimeTest : testRuntimeTests ) {
      result.add( testRuntimeTest.getId() );
    }
    return result;
  }

  @Before
  public void setup() {
    executorService = Executors.newCachedThreadPool();
    RuntimeTestResultEntryImpl overallEntry =
      new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.INFO, "testDesc", "testMessage" );
    unsatisfiableDependencyA = new TestRuntimeTest( "unsatisfiableDependency", "unsatisfiableDependencyTestA", "Test A",
      new HashSet<>( Arrays.asList(
        new TestRuntimeTest( "fake-module", "fake-test-id", "fake-getName", new HashSet<TestRuntimeTest>(), 5,
          overallEntry, new ArrayList<RuntimeTestResultEntry>(), false ) ) ), 5, overallEntry,
      new ArrayList<RuntimeTestResultEntry>(),
      false );
    moduleATestA =
      new TestRuntimeTest( "moduleA", "moduleATestA", "Test A", new HashSet<>( Arrays.<TestRuntimeTest>asList() ), 5,
        overallEntry,
        new ArrayList<RuntimeTestResultEntry>(), true );
    moduleATestB =
      new TestRuntimeTest( "moduleA", "moduleATestB", "Test B", new HashSet<>( Arrays.asList( moduleATestA ) ), 5,
        overallEntry,
        new ArrayList<RuntimeTestResultEntry>(), true );
    moduleATestC =
      new TestRuntimeTest( "moduleA", "moduleATestC", "Test C", new HashSet<>( Arrays.asList( moduleATestB ) ), 5,
        overallEntry,
        new ArrayList<RuntimeTestResultEntry>(), true );
    moduleATestD =
      new TestRuntimeTest( "moduleA", "moduleATestD", "Test D", new HashSet<>( Arrays.asList( moduleATestB ) ), 5,
        overallEntry,
        new ArrayList<RuntimeTestResultEntry>(), true );
    moduleBTestA =
      new TestRuntimeTest( "moduleB", "moduleBTestA", "Test A", new HashSet<>( Arrays.asList( moduleATestA ) ), 5,
        overallEntry,
        new ArrayList<RuntimeTestResultEntry>(), true );
    moduleBTestB =
      new TestRuntimeTest( "moduleB", "moduleBTestB", "Test B", new HashSet<>( Arrays.asList( moduleATestC ) ), 5,
        overallEntry,
        new ArrayList<RuntimeTestResultEntry>(), true );
    moduleBTestC =
      new TestRuntimeTest( "moduleB", "moduleBTestC", "Test C",
        new HashSet<>( Arrays.asList( moduleBTestB, moduleATestC ) ),
        5, overallEntry, new ArrayList<RuntimeTestResultEntry>(), true );
    moduleCTestA = new TestRuntimeTest( "moduleC", "moduleCTestA", "Test A",
      new HashSet<>( Arrays.asList( moduleBTestC, moduleATestC ) ),
      5, overallEntry, new ArrayList<RuntimeTestResultEntry>(), true );
    objectUnderTest = new Object();
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

  private void testScenario( List<TestRuntimeTest> runtimeTests ) {
    final List<RuntimeTestStatus> runtimeTestStatuses = Collections.synchronizedList( new ArrayList
      <RuntimeTestStatus>() );
    final RuntimeTestProgressCallback runtimeTestProgressCallback = new RuntimeTestProgressCallback() {
      @Override public void onProgress( RuntimeTestStatus runtimeTestStatus ) {
        runtimeTestStatuses.add( runtimeTestStatus );
        if ( runtimeTestStatus.isDone() ) {
          synchronized ( this ) {
            notifyAll();
          }
        }
      }
    };
    long before = System.currentTimeMillis();
    new RuntimeTestRunner( runtimeTests, objectUnderTest, runtimeTestProgressCallback, executorService ).runTests();
    synchronized ( runtimeTestProgressCallback ) {
      while ( runtimeTestStatuses.size() == 0 || !runtimeTestStatuses.get( runtimeTestStatuses.size() - 1 ).isDone() ) {
        try {
          runtimeTestProgressCallback.wait();
        } catch ( InterruptedException e ) {
          // Ignore
        }
      }
    }
    long after = System.currentTimeMillis();
    Set<String> doneIds = new HashSet<>();
    for ( int i = 0; i < runtimeTestStatuses.size(); i++ ) {
      RuntimeTestStatus runtimeTestStatus = runtimeTestStatuses.get( i );
      if ( i < runtimeTestStatuses.size() - 1 ) {
        assertFalse( runtimeTestStatus.isDone() );
      } else {
        assertTrue( runtimeTestStatus.isDone() );
      }
      Set<String> justDoneIds = new HashSet<>();
      for ( RuntimeTestModuleResults runtimeTestModuleResults : runtimeTestStatus.getModuleResults() ) {
        Set<String> outstandingIds = new HashSet<>();
        Set<String> runningIds = new HashSet<>();
        for ( RuntimeTest runtimeTest : runtimeTestModuleResults.getOutstandingTests() ) {
          outstandingIds.add( runtimeTest.getId() );
        }
        for ( RuntimeTest runtimeTest : runtimeTestModuleResults.getRunningTests() ) {
          runningIds.add( runtimeTest.getId() );
        }

        Set<String> resultIds = new HashSet<>();
        for ( RuntimeTestResult runtimeTestResult : runtimeTestModuleResults.getRuntimeTestResults() ) {
          resultIds.add( runtimeTestResult.getRuntimeTest().getId() );
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
    for ( TestRuntimeTest runtimeTest : runtimeTests ) {
      assertTrue( doneIds.contains( runtimeTest.getId() ) );
      runtimeTest.validateRunState();
    }
    System.out.println( "Ran in " + ( after - before ) + " ms" );
    System.out.flush();
  }

  public class TestRuntimeTest extends BaseRuntimeTest {
    private final long delay;
    private final Set<TestRuntimeTest> dependencies;
    private final AtomicBoolean hasRun;
    private final RuntimeTestResultEntry overallEntry;
    private final List<RuntimeTestResultEntry> runtimeTestResultEntries;
    private final boolean shouldRun;

    public TestRuntimeTest( String module, String id, String name, Set<TestRuntimeTest> dependencies,
                            long delay, RuntimeTestResultEntry overallEntry,
                            List<RuntimeTestResultEntry> runtimeTestResultEntries, boolean shouldRun ) {
      super( Object.class, module, id, name, dependenciesToIds( dependencies ) );
      this.delay = delay;
      this.dependencies = dependencies;
      this.overallEntry = overallEntry;
      this.runtimeTestResultEntries = runtimeTestResultEntries;
      this.shouldRun = shouldRun;
      hasRun = new AtomicBoolean( false );
    }

    public String getLogName() {
      return getModule() + ":" + getId();
    }

    @Override public RuntimeTestResultSummary runTest( Object objectUnderTest ) {
      assertTrue( shouldRun );
      assertEquals( RuntimeTestRunnerTest.this.objectUnderTest, objectUnderTest );
      String logName = getLogName();
      System.out.println( "Running: " + logName );
      for ( TestRuntimeTest dependency : dependencies ) {
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
      return new RuntimeTestResultSummaryImpl( overallEntry, runtimeTestResultEntries );
    }

    public void validateRunState() {
      String moduleString = getLogName();
      assertEquals( "Expected " + moduleString + " hasRun value of " + shouldRun + " but was " + hasRun.get(),
        shouldRun, hasRun.get() );
      System.out.println( "Got correct shouldRun value of " + shouldRun + " from " + moduleString );
    }
  }
}
