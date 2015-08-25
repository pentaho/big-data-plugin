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
import org.pentaho.big.data.api.clusterTest.module.ClusterTestModuleResults;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestStatusImplTest {
  private List<ClusterTestModuleResults> clusterTestModuleResults;
  private ClusterTestStatusImpl clusterTestStatus;
  private int testsDone;
  private int testsRunning;
  private int testsOutstanding;
  private boolean done;

  @Before
  public void setup() {
    clusterTestModuleResults = mock( List.class );
    testsDone = 1011;
    testsRunning = 11213;
    testsOutstanding = 12213;
    done = true;
    initStatus();
  }

  private void initStatus() {
    clusterTestStatus =
      new ClusterTestStatusImpl( clusterTestModuleResults, testsDone, testsRunning, testsOutstanding, done );
  }

  @Test
  public void testConstructor() {
    assertEquals( clusterTestModuleResults, clusterTestStatus.getModuleResults() );
    assertTrue( clusterTestStatus.isDone() );
    assertEquals( testsDone, clusterTestStatus.getTestsDone() );
    assertEquals( testsRunning, clusterTestStatus.getTestsRunning() );
    assertEquals( testsOutstanding, clusterTestStatus.getTestsOutstanding() );
    done = false;
    initStatus();
    assertEquals( clusterTestModuleResults, clusterTestStatus.getModuleResults() );
    assertFalse( clusterTestStatus.isDone() );
    assertEquals( testsDone, clusterTestStatus.getTestsDone() );
    assertEquals( testsRunning, clusterTestStatus.getTestsRunning() );
    assertEquals( testsOutstanding, clusterTestStatus.getTestsOutstanding() );
  }

  @Test
  public void testToString() {
    assertTrue( clusterTestStatus.toString().contains( clusterTestModuleResults.toString() ) );
    assertTrue( clusterTestStatus.toString().contains( Integer.toString( testsDone ) ) );
    assertTrue( clusterTestStatus.toString().contains( Integer.toString( testsRunning ) ) );
    assertTrue( clusterTestStatus.toString().contains( Integer.toString( testsOutstanding ) ) );
    assertTrue( clusterTestStatus.toString().contains( "done=" + done ) );
    done = false;
    initStatus();
    assertTrue( clusterTestStatus.toString().contains( clusterTestModuleResults.toString() ) );
    assertTrue( clusterTestStatus.toString().contains( Integer.toString( testsDone ) ) );
    assertTrue( clusterTestStatus.toString().contains( Integer.toString( testsRunning ) ) );
    assertTrue( clusterTestStatus.toString().contains( Integer.toString( testsOutstanding ) ) );
    assertTrue( clusterTestStatus.toString().contains( "done=" + done ) );
  }
}
