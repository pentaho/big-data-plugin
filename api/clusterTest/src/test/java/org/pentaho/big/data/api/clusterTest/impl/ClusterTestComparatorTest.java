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
import org.pentaho.big.data.api.clusterTest.test.ClusterTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class ClusterTestComparatorTest {
  private ClusterTestComparator clusterTestComparator;
  private Map<String, Integer> orderedModules;
  private ClusterTest clusterTest1;
  private ClusterTest clusterTest2;
  private String d = "d";
  private String c = "c";
  private String a = "a";
  private String b = "b";

  @Before
  public void setup() {
    orderedModules = new HashMap<>();
    orderedModules.put( d, 0 );
    orderedModules.put( c, 1 );
    orderedModules.put( a, 2 );
    orderedModules.put( b, 3 );
    clusterTestComparator = new ClusterTestComparator( orderedModules );
    clusterTest1 = mock( ClusterTest.class );
    clusterTest2 = mock( ClusterTest.class );
  }

  @Test
  public void testModuleSameOrderedIdsSame() {
    when( clusterTest1.getModule() ).thenReturn( a );
    when( clusterTest2.getModule() ).thenReturn( a );
    when( clusterTest1.getId() ).thenReturn( b );
    when( clusterTest2.getId() ).thenReturn( b );
    assertEquals( 0, clusterTestComparator.compare( clusterTest1, clusterTest2 ) );
  }

  @Test
  public void testModuleSameOrderedIdsDifferent1() {
    when( clusterTest1.getModule() ).thenReturn( a );
    when( clusterTest2.getModule() ).thenReturn( a );
    when( clusterTest1.getId() ).thenReturn( a );
    when( clusterTest2.getId() ).thenReturn( b );
    assertTrue( clusterTestComparator.compare( clusterTest1, clusterTest2 ) < 0 );
  }

  @Test
  public void testModuleSameOrderedIdsDifferent2() {
    when( clusterTest1.getModule() ).thenReturn( a );
    when( clusterTest2.getModule() ).thenReturn( a );
    when( clusterTest1.getId() ).thenReturn( b );
    when( clusterTest2.getId() ).thenReturn( a );
    assertTrue( clusterTestComparator.compare( clusterTest1, clusterTest2 ) > 0 );
  }

  @Test
  public void testModuleSameUnrderedIdsSame() {
    when( clusterTest1.getModule() ).thenReturn( "e" );
    when( clusterTest2.getModule() ).thenReturn( "e" );
    when( clusterTest1.getId() ).thenReturn( b );
    when( clusterTest2.getId() ).thenReturn( b );
    assertEquals( 0, clusterTestComparator.compare( clusterTest1, clusterTest2 ) );
  }

  @Test
  public void testModuleDifferentOrdered() {
    when( clusterTest1.getModule() ).thenReturn( a );
    when( clusterTest2.getModule() ).thenReturn( b );
    when( clusterTest1.getId() ).thenReturn( d );
    when( clusterTest2.getId() ).thenReturn( c );
    assertTrue( clusterTestComparator.compare( clusterTest1, clusterTest2 ) < 0 );
  }

  @Test
  public void testModuleDifferentFirstOrdered() {
    orderedModules.remove( a );
    when( clusterTest1.getModule() ).thenReturn( b );
    when( clusterTest2.getModule() ).thenReturn( a );
    when( clusterTest1.getId() ).thenReturn( d );
    when( clusterTest2.getId() ).thenReturn( c );
    assertTrue( clusterTestComparator.compare( clusterTest1, clusterTest2 ) < 0 );
  }

  @Test
  public void testModuleDifferentSecondOrdered() {
    orderedModules.remove( b );
    when( clusterTest1.getModule() ).thenReturn( b );
    when( clusterTest2.getModule() ).thenReturn( a );
    when( clusterTest1.getId() ).thenReturn( d );
    when( clusterTest2.getId() ).thenReturn( c );
    assertTrue( clusterTestComparator.compare( clusterTest1, clusterTest2 ) > 0 );
  }

  @Test
  public void testModuleDifferentNotOrdered() {
    orderedModules.remove( a );
    orderedModules.remove( b );
    when( clusterTest1.getModule() ).thenReturn( a );
    when( clusterTest2.getModule() ).thenReturn( b );
    when( clusterTest1.getId() ).thenReturn( d );
    when( clusterTest2.getId() ).thenReturn( c );
    assertTrue( clusterTestComparator.compare( clusterTest1, clusterTest2 ) < 0 );
  }
}
