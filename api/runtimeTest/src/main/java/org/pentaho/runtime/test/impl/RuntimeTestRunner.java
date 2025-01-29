/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.runtime.test.impl;

import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.RuntimeTestProgressCallback;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;
import org.pentaho.runtime.test.module.impl.RuntimeTestModuleResultsImpl;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.RuntimeTestDelegateWithMoreDependencies;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 8/11/15.
 */
public class RuntimeTestRunner {
  private static final Class<?> PKG = RuntimeTestRunner.class;
  private final Set<RuntimeTest> remainingTests;
  private final Object objectUnderTest;
  private final RuntimeTestProgressCallback runtimeTestProgressCallback;
  private final ExecutorService executorService;
  private final Set<String> satisfiedDependencies;
  private final Set<String> failedDependencies;
  private final List<String> runtimeModuleList;
  private final Map<String, List<String>> stringRuntimeTestModuleToTestIdMap;
  private final Map<String, RuntimeTestResult> runtimeTestResultMap;
  private final Set<String> outstandingTestIds;
  private final Set<String> runningTestIds;
  private final int numberOfTests;

  @SuppressWarnings( "unchecked" )
  public RuntimeTestRunner( Collection<? extends RuntimeTest> runtimeTests, Object objectUnderTest,
                            RuntimeTestProgressCallback runtimeTestProgressCallback, ExecutorService executorService ) {
    this.objectUnderTest = objectUnderTest;
    runtimeModuleList = new ArrayList<>();
    stringRuntimeTestModuleToTestIdMap = new HashMap<>();
    runtimeTestResultMap = new HashMap<>();
    outstandingTestIds = new HashSet<>();
    runningTestIds = new HashSet<>();

    Set<RuntimeTest> initTests = new HashSet<>();
    Set<String> initTestIds = new HashSet<>();
    Set<RuntimeTest> nonInitTests = new HashSet<>();
    int numberOfTests = 0;
    for ( RuntimeTest runtimeTest : runtimeTests ) {
      if ( runtimeTest.accepts( objectUnderTest ) ) {
        numberOfTests++;
        String runtimeTestModule = runtimeTest.getModule();
        List<String> runtimeIdsForModule = stringRuntimeTestModuleToTestIdMap.get( runtimeTestModule );
        if ( runtimeIdsForModule == null ) {
          runtimeModuleList.add( runtimeTestModule );
          runtimeIdsForModule = new ArrayList<>();
          stringRuntimeTestModuleToTestIdMap.put( runtimeTestModule, runtimeIdsForModule );
        }
        String runtimeTestId = runtimeTest.getId();
        runtimeIdsForModule.add( runtimeTestId );
        if ( runtimeTest.isConfigInitTest() ) {
          initTests.add( runtimeTest );
          initTestIds.add( runtimeTestId );
        } else {
          nonInitTests.add( runtimeTest );
        }
      }
    }
    this.numberOfTests = numberOfTests;
    this.remainingTests = new HashSet<>( initTests );
    for ( RuntimeTest nonInitTest : nonInitTests ) {
      remainingTests.add( new RuntimeTestDelegateWithMoreDependencies( nonInitTest, initTestIds ) );
    }
    for ( RuntimeTest remainingTest : remainingTests ) {
      String remainingTestId = remainingTest.getId();
      runtimeTestResultMap
        .put( remainingTestId, new RuntimeTestResultImpl( remainingTest, false, new RuntimeTestResultSummaryImpl(),
          0L ) );
      outstandingTestIds.add( remainingTestId );
    }
    this.satisfiedDependencies = new HashSet<>();
    this.failedDependencies = new HashSet<>();
    this.runtimeTestProgressCallback = runtimeTestProgressCallback;
    this.executorService = executorService;
  }

  private void markSkipped( RuntimeTest runtimeTest ) {
    Set<String> relevantFailed = new HashSet<>( failedDependencies );
    relevantFailed.retainAll( runtimeTest.getDependencies() );

    // Get one of the dependencies' names for display
    String failedDependencyName = "a prerequisite";
    if ( !relevantFailed.isEmpty() ) {
      String failedDependencyId = relevantFailed.iterator().next();
      RuntimeTestResult runtimeTestResult = runtimeTestResultMap.get( failedDependencyId );
      if ( runtimeTestResult != null ) {
        failedDependencyName = runtimeTestResult.getRuntimeTest().getName();
      }

    }

    // We had a dependency fail so we need to skip
    String runtimeTestId = runtimeTest.getId();
    failedDependencies.add( runtimeTestId );
    outstandingTestIds.remove( runtimeTestId );
    runningTestIds.remove( runtimeTestId );
    runtimeTestResultMap.put( runtimeTestId, new RuntimeTestResultImpl( runtimeTest, true,
      new RuntimeTestResultSummaryImpl( new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.SKIPPED,
        BaseMessages.getString( PKG, "RuntimeTestRunner.Skipped.Desc", failedDependencyName ),
        BaseMessages.getString( PKG, "RuntimeTestRunner.Skipped.Message", runtimeTest.getName(), relevantFailed ), (Throwable) null ) ), 0L ) );
  }

  private void callbackState() {
    callbackState( false );
  }

  private void callbackState( boolean done ) {
    if ( runtimeTestProgressCallback != null ) {
      List<RuntimeTestModuleResults> moduleResults = new ArrayList<>( runtimeModuleList.size() );
      for ( String runtimeModule : runtimeModuleList ) {
        List<RuntimeTestResult> runtimeTestResults = new ArrayList<>();
        Set<RuntimeTest> runningTests = new HashSet<>();
        HashSet<RuntimeTest> outstandingTests = new HashSet<>();
        for ( String testId : stringRuntimeTestModuleToTestIdMap.get( runtimeModule ) ) {
          RuntimeTestResult runtimeTestResult = runtimeTestResultMap.get( testId );
          runtimeTestResults.add( runtimeTestResult );
          if ( runningTestIds.contains( testId ) ) {
            runningTests.add( runtimeTestResult.getRuntimeTest() );
          } else if ( outstandingTestIds.contains( testId ) ) {
            outstandingTests.add( runtimeTestResult.getRuntimeTest() );
          }
        }
        moduleResults
          .add( new RuntimeTestModuleResultsImpl( runtimeModule, runtimeTestResults, runningTests, outstandingTests ) );
      }
      int testsRunning = runningTestIds.size();
      int testsOutstanding = outstandingTestIds.size();
      int testsDone = numberOfTests - testsOutstanding - testsRunning;
      runtimeTestProgressCallback.onProgress(
        new RuntimeTestStatusImpl( Collections.unmodifiableList( moduleResults ), testsDone, testsRunning,
          testsOutstanding, done ) );
    }
  }

  private void runTest( RuntimeTest runtimeTest ) {
    String eligibleTestId = runtimeTest.getId();
    RuntimeTestResultSummary runtimeTestResultSummary;
    long before = System.currentTimeMillis();
    RuntimeTestEntrySeverity overallSeverity;
    try {
      runtimeTestResultSummary = runtimeTest.runTest( objectUnderTest );
      overallSeverity = runtimeTestResultSummary.getOverallStatusEntry().getSeverity();
    } catch ( Throwable e ) {
      overallSeverity = RuntimeTestEntrySeverity.FATAL;
      runtimeTestResultSummary = new RuntimeTestResultSummaryImpl(
        new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          BaseMessages.getString( PKG, "RuntimeTestRunner.Error.Desc", runtimeTest.getName() ), e.getMessage(), e ) );
    }
    long after = System.currentTimeMillis();
    RuntimeTestResult runtimeTestResult =
      new RuntimeTestResultImpl( runtimeTest, true, runtimeTestResultSummary, after - before );
    synchronized ( this ) {
      if ( overallSeverity == RuntimeTestEntrySeverity.ERROR || overallSeverity == RuntimeTestEntrySeverity.FATAL ) {
        failedDependencies.add( eligibleTestId );
      } else {
        satisfiedDependencies.add( eligibleTestId );
      }
      runtimeTestResultMap.put( eligibleTestId, runtimeTestResult );
      runningTestIds.remove( eligibleTestId );
      callbackState();
      notifyAll();
    }
  }

  public synchronized void runTests() {
    callbackState();
    while ( remainingTests.size() > 0 || runningTestIds.size() > 0 ) {
      Set<RuntimeTest> eligibleTests = new HashSet<>();
      Set<RuntimeTest> skippingTests = new HashSet<>();
      Set<String> possibleToSatisfyIds = new HashSet<>( satisfiedDependencies );
      for ( RuntimeTest remainingTest : remainingTests ) {
        possibleToSatisfyIds.add( remainingTest.getId() );
      }
      possibleToSatisfyIds.addAll( outstandingTestIds );
      possibleToSatisfyIds.addAll( runningTestIds );
      for ( RuntimeTest remainingTest : remainingTests ) {
        Set<String> remainingTestDependencies = remainingTest.getDependencies();
        if ( satisfiedDependencies.containsAll( remainingTestDependencies ) ) {
          eligibleTests.add( remainingTest );
        } else if ( !Collections.disjoint( remainingTestDependencies, failedDependencies ) || !possibleToSatisfyIds
          .containsAll( remainingTestDependencies ) ) {
          skippingTests.add( remainingTest );
          markSkipped( remainingTest );
        }
      }
      remainingTests.removeAll( eligibleTests );
      remainingTests.removeAll( skippingTests );
      for ( RuntimeTest eligibleTest : eligibleTests ) {
        String eligibleTestId = eligibleTest.getId();
        outstandingTestIds.remove( eligibleTestId );
        runningTestIds.add( eligibleTestId );
      }
      final int wasRunning = runningTestIds.size();
      for ( final RuntimeTest eligibleTest : eligibleTests ) {
        executorService.submit( new Runnable() {
          @Override
          public void run() {
            runTest( eligibleTest );
          }
        } );
      }
      // If we skipped test(s) state has changed and we should rerun immediately, otherwise we can wait until one
      // finishes
      if ( skippingTests.size() == 0 ) {
        if ( wasRunning > 0 ) {
          while ( wasRunning == runningTestIds.size() ) {
            try {
              // Wait until a test finishes
              wait();
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }
        }
      } else {
        callbackState();
      }
    }
    callbackState( true );
  }

  public static class Factory {
    public RuntimeTestRunner create( Collection<? extends RuntimeTest> runtimeTests, Object objectUnderTest,
                                     RuntimeTestProgressCallback runtimeTestProgressCallback,
                                     ExecutorService executorService ) {
      return new RuntimeTestRunner( runtimeTests, objectUnderTest, runtimeTestProgressCallback, executorService );
    }
  }
}
