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
