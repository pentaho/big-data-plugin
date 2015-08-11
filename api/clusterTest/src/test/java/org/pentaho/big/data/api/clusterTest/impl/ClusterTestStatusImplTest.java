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
  private boolean done;
  private ClusterTestStatusImpl clusterTestStatus;

  @Before
  public void setup() {
    clusterTestModuleResults = mock( List.class );
    done = true;
    initStatus();
  }

  private void initStatus() {
    clusterTestStatus = new ClusterTestStatusImpl( clusterTestModuleResults, done );
  }

  @Test
  public void testConstructor() {
    assertEquals( clusterTestModuleResults, clusterTestStatus.getModuleResults() );
    assertTrue( clusterTestStatus.isDone() );
    done = false;
    initStatus();
    assertEquals( clusterTestModuleResults, clusterTestStatus.getModuleResults() );
    assertFalse( clusterTestStatus.isDone() );
  }

  @Test
  public void testToString() {
    assertTrue( clusterTestStatus.toString().contains( clusterTestModuleResults.toString() ) );
    assertTrue( clusterTestStatus.toString().contains( "done=true" ) );
    done = false;
    initStatus();
    assertTrue( clusterTestStatus.toString().contains( "done=false" ) );
  }
}
