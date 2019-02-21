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

package org.pentaho.runtime.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.module.RuntimeTestModuleResults;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/20/15.
 */
public class RuntimeTestStatusImplTest {
  private List<RuntimeTestModuleResults> runtimeTestModuleResults;
  private RuntimeTestStatusImpl runtimeTestStatus;
  private int testsDone;
  private int testsRunning;
  private int testsOutstanding;
  private boolean done;

  @Before
  public void setup() {
    runtimeTestModuleResults = mock( List.class );
    testsDone = 1011;
    testsRunning = 11213;
    testsOutstanding = 12213;
    done = true;
    initStatus();
  }

  private void initStatus() {
    runtimeTestStatus =
      new RuntimeTestStatusImpl( runtimeTestModuleResults, testsDone, testsRunning, testsOutstanding, done );
  }

  @Test
  public void testConstructor() {
    assertEquals( runtimeTestModuleResults, runtimeTestStatus.getModuleResults() );
    assertTrue( runtimeTestStatus.isDone() );
    assertEquals( testsDone, runtimeTestStatus.getTestsDone() );
    assertEquals( testsRunning, runtimeTestStatus.getTestsRunning() );
    assertEquals( testsOutstanding, runtimeTestStatus.getTestsOutstanding() );
    done = false;
    initStatus();
    assertEquals( runtimeTestModuleResults, runtimeTestStatus.getModuleResults() );
    assertFalse( runtimeTestStatus.isDone() );
    assertEquals( testsDone, runtimeTestStatus.getTestsDone() );
    assertEquals( testsRunning, runtimeTestStatus.getTestsRunning() );
    assertEquals( testsOutstanding, runtimeTestStatus.getTestsOutstanding() );
  }

  @Test
  public void testToString() {
    assertTrue( runtimeTestStatus.toString().contains( runtimeTestModuleResults.toString() ) );
    assertTrue( runtimeTestStatus.toString().contains( Integer.toString( testsDone ) ) );
    assertTrue( runtimeTestStatus.toString().contains( Integer.toString( testsRunning ) ) );
    assertTrue( runtimeTestStatus.toString().contains( Integer.toString( testsOutstanding ) ) );
    assertTrue( runtimeTestStatus.toString().contains( "done=" + done ) );
    done = false;
    initStatus();
    assertTrue( runtimeTestStatus.toString().contains( runtimeTestModuleResults.toString() ) );
    assertTrue( runtimeTestStatus.toString().contains( Integer.toString( testsDone ) ) );
    assertTrue( runtimeTestStatus.toString().contains( Integer.toString( testsRunning ) ) );
    assertTrue( runtimeTestStatus.toString().contains( Integer.toString( testsOutstanding ) ) );
    assertTrue( runtimeTestStatus.toString().contains( "done=" + done ) );
  }
}
