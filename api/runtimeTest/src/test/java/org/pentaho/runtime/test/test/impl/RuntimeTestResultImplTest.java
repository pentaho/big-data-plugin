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

package org.pentaho.runtime.test.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.RuntimeTest;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/20/15.
 */
public class RuntimeTestResultImplTest {
  private RuntimeTest runtimeTest;
  private List<RuntimeTestResultEntry> runtimeTestResultEntries;
  private long timeTaken;
  private RuntimeTestResultImpl runtimeTestResult;
  private RuntimeTestResultEntry runtimeTestResultEntry;
  private RuntimeTestEntrySeverity info;

  @Before
  public void setup() {
    runtimeTest = mock( RuntimeTest.class );
    info = RuntimeTestEntrySeverity.INFO;
    runtimeTestResultEntry = new RuntimeTestResultEntryImpl( info, "testDesc", "testMessage" );
    runtimeTestResultEntries = new ArrayList<>( Arrays.asList( runtimeTestResultEntry ) );
    timeTaken = 10L;
    runtimeTestResult = new RuntimeTestResultImpl( runtimeTest, true,
      new RuntimeTestResultSummaryImpl( runtimeTestResultEntry, runtimeTestResultEntries ), timeTaken );
  }

  @Test
  public void testGetMaxSeverity() {
    assertEquals( info, runtimeTestResult.getOverallStatusEntry().getSeverity() );
  }

  @Test
  public void testGetRuntimeTestResultEntries() {
    assertEquals( runtimeTestResultEntries, runtimeTestResult.getRuntimeTestResultEntries() );
  }

  @Test
  public void testGetRuntimeTest() {
    assertEquals( runtimeTest, runtimeTestResult.getRuntimeTest() );
  }

  @Test
  public void testGetTimeTaken() {
    assertEquals( timeTaken, runtimeTestResult.getTimeTaken() );
  }

  @Test
  public void testToString() {
    String string = runtimeTestResult.toString();
    assertTrue( string.contains( info.toString() ) );
    assertTrue( string.contains( runtimeTestResultEntry.toString() ) );
    assertTrue( string.contains( runtimeTest.toString() ) );
    assertTrue( string.contains( String.valueOf( timeTaken ) ) );
  }
}
