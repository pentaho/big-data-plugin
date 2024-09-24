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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.RuntimeTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/20/15.
 */
public class RuntimeTestComparatorTest {
  private RuntimeTestComparator runtimeTestComparator;
  private Map<String, Integer> orderedModules;
  private RuntimeTest runtimeTest1;
  private RuntimeTest runtimeTest2;
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
    runtimeTestComparator = new RuntimeTestComparator( orderedModules );
    runtimeTest1 = mock( RuntimeTest.class );
    runtimeTest2 = mock( RuntimeTest.class );
  }

  @Test
  public void testModuleSameOrderedIdsSame() {
    when( runtimeTest1.getModule() ).thenReturn( a );
    when( runtimeTest2.getModule() ).thenReturn( a );
    when( runtimeTest1.getId() ).thenReturn( b );
    when( runtimeTest2.getId() ).thenReturn( b );
    assertEquals( 0, runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) );
  }

  @Test
  public void testModuleSameOrderedIdsDifferent1() {
    when( runtimeTest1.getModule() ).thenReturn( a );
    when( runtimeTest2.getModule() ).thenReturn( a );
    when( runtimeTest1.getId() ).thenReturn( a );
    when( runtimeTest2.getId() ).thenReturn( b );
    assertTrue( runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) < 0 );
  }

  @Test
  public void testModuleSameOrderedIdsDifferent2() {
    when( runtimeTest1.getModule() ).thenReturn( a );
    when( runtimeTest2.getModule() ).thenReturn( a );
    when( runtimeTest1.getId() ).thenReturn( b );
    when( runtimeTest2.getId() ).thenReturn( a );
    assertTrue( runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) > 0 );
  }

  @Test
  public void testModuleSameUnrderedIdsSame() {
    when( runtimeTest1.getModule() ).thenReturn( "e" );
    when( runtimeTest2.getModule() ).thenReturn( "e" );
    when( runtimeTest1.getId() ).thenReturn( b );
    when( runtimeTest2.getId() ).thenReturn( b );
    assertEquals( 0, runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) );
  }

  @Test
  public void testModuleDifferentOrdered() {
    when( runtimeTest1.getModule() ).thenReturn( a );
    when( runtimeTest2.getModule() ).thenReturn( b );
    when( runtimeTest1.getId() ).thenReturn( d );
    when( runtimeTest2.getId() ).thenReturn( c );
    assertTrue( runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) < 0 );
  }

  @Test
  public void testModuleDifferentFirstOrdered() {
    orderedModules.remove( a );
    when( runtimeTest1.getModule() ).thenReturn( b );
    when( runtimeTest2.getModule() ).thenReturn( a );
    when( runtimeTest1.getId() ).thenReturn( d );
    when( runtimeTest2.getId() ).thenReturn( c );
    assertTrue( runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) < 0 );
  }

  @Test
  public void testModuleDifferentSecondOrdered() {
    orderedModules.remove( b );
    when( runtimeTest1.getModule() ).thenReturn( b );
    when( runtimeTest2.getModule() ).thenReturn( a );
    when( runtimeTest1.getId() ).thenReturn( d );
    when( runtimeTest2.getId() ).thenReturn( c );
    assertTrue( runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) > 0 );
  }

  @Test
  public void testModuleDifferentNotOrdered() {
    orderedModules.remove( a );
    orderedModules.remove( b );
    when( runtimeTest1.getModule() ).thenReturn( a );
    when( runtimeTest2.getModule() ).thenReturn( b );
    when( runtimeTest1.getId() ).thenReturn( d );
    when( runtimeTest2.getId() ).thenReturn( c );
    assertTrue( runtimeTestComparator.compare( runtimeTest1, runtimeTest2 ) < 0 );
  }
}
