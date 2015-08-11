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

package org.pentaho.big.data.api.clusterTest.test.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestResultImplTest {
  private ClusterTest clusterTest;
  private List<ClusterTestResultEntry> clusterTestResultEntries;
  private long timeTaken;
  private ClusterTestResultImpl clusterTestResult;
  private ClusterTestResultEntry clusterTestResultEntry;
  private ClusterTestEntrySeverity info;

  @Before
  public void setup() {
    clusterTest = mock( ClusterTest.class );
    clusterTestResultEntry = mock( ClusterTestResultEntry.class );
    info = ClusterTestEntrySeverity.INFO;
    when( clusterTestResultEntry.getSeverity() ).thenReturn( info );
    clusterTestResultEntries = new ArrayList<>( Arrays.asList( clusterTestResultEntry ) );
    timeTaken = 10L;
    clusterTestResult = new ClusterTestResultImpl( clusterTest, clusterTestResultEntries, timeTaken );
  }

  @Test
  public void testGetMaxSeverity() {
    assertEquals( info, clusterTestResult.getMaxSeverity() );
  }

  @Test
  public void testGetClusterTestResultEntries() {
    assertEquals( clusterTestResultEntries, clusterTestResult.getClusterTestResultEntries() );
  }

  @Test
  public void testGetClusterTest() {
    assertEquals( clusterTest, clusterTestResult.getClusterTest() );
  }

  @Test
  public void testGetTimeTaken() {
    assertEquals( timeTaken, clusterTestResult.getTimeTaken() );
  }

  @Test
  public void testToString() {
    String string = clusterTestResult.toString();
    assertTrue( string.contains( info.toString() ) );
    assertTrue( string.contains( clusterTestResultEntry.toString() ) );
    assertTrue( string.contains( clusterTest.toString() ) );
    assertTrue( string.contains( String.valueOf( timeTaken ) ) );
  }
}
