/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test.module.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResult;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class RuntimeTestModuleResultsImplTest {
  private String name;
  private List<RuntimeTestResult> runtimeTestResults;
  private Set<RuntimeTest> runningTests;
  private Set<RuntimeTest> outstandingTests;
  private RuntimeTestModuleResultsImpl runtimeTestModuleResults;
  private RuntimeTestResult runtimeTestResult;
  private RuntimeTest runningTest;
  private RuntimeTest outstandingTest;
  private RuntimeTestEntrySeverity maxSeverity;

  @Before
  public void setup() {
    name = "testName";
    runtimeTestResult = mock( RuntimeTestResult.class );
    when( runtimeTestResult.isDone() ).thenReturn( true );
    maxSeverity = RuntimeTestEntrySeverity.INFO;
    RuntimeTestResultEntry runtimeTestResultEntry = mock( RuntimeTestResultEntry.class );
    when( runtimeTestResult.getOverallStatusEntry() ).thenReturn( runtimeTestResultEntry );
    when( runtimeTestResultEntry.getSeverity() ).thenReturn( maxSeverity );
    runtimeTestResults = new ArrayList<>( Arrays.asList( runtimeTestResult ) );
    runningTest = mock( RuntimeTest.class );
    runningTests = new HashSet<>( Arrays.asList( runningTest ) );
    outstandingTest = mock( RuntimeTest.class );
    outstandingTests = new HashSet<>( Arrays.asList( outstandingTest ) );
    runtimeTestModuleResults =
      new RuntimeTestModuleResultsImpl( name, runtimeTestResults, runningTests, outstandingTests );
  }

  @Test
  public void testName() {
    assertEquals( name, runtimeTestModuleResults.getName() );
  }

  @Test
  public void testGetRuntimeTestResults() {
    assertEquals( runtimeTestResults, runtimeTestModuleResults.getRuntimeTestResults() );
  }

  @Test
  public void testGetRunningTests() {
    assertEquals( runningTests, runtimeTestModuleResults.getRunningTests() );
  }

  @Test
  public void testGetOutstandingTests() {
    assertEquals( outstandingTests, runtimeTestModuleResults.getOutstandingTests() );
  }

  @Test
  public void testGetMaxSeverity() {
    assertEquals( maxSeverity, runtimeTestModuleResults.getMaxSeverity() );
  }

  @Test
  public void testToString() {
    String string = runtimeTestModuleResults.toString();
    assertTrue( string.contains( name ) );
    assertTrue( string.contains( runtimeTestResult.toString() ) );
    assertTrue( string.contains( runningTest.toString() ) );
    assertTrue( string.contains( outstandingTest.toString() ) );
    assertTrue( string.contains( maxSeverity.toString() ) );
  }
}
