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

package org.pentaho.big.data.api.cluster;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/28/15.
 */
public class NamedClusterComparatorTest {
  private NamedCluster namedCluster1;
  private NamedCluster namedCluster2;
  private String firstName;
  private String secondName;

  @Before
  public void setup() {
    firstName = "a";
    secondName = "b";

    namedCluster1 = mock( NamedCluster.class );
    namedCluster2 = mock( NamedCluster.class );
  }

  @Test
  public void testFirstNameFirst() {
    when( namedCluster1.getName() ).thenReturn( firstName );
    when( namedCluster2.getName() ).thenReturn( secondName );
    assertTrue( "Expected " + firstName + " before " + secondName,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondName + " after " + firstName,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testFirstNameFirstFirstUpper() {
    String firstUpper = firstName.toUpperCase();
    when( namedCluster1.getName() ).thenReturn( firstUpper );
    when( namedCluster2.getName() ).thenReturn( secondName );
    assertTrue( "Expected " + firstUpper + " before " + secondName,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondName + " after " + firstUpper,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testFirstNameFirstSecondUpper() {
    String secondUpper = secondName.toUpperCase();
    when( namedCluster1.getName() ).thenReturn( firstName );
    when( namedCluster2.getName() ).thenReturn( secondUpper );
    assertTrue( "Expected " + firstName + " before " + secondUpper,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondUpper + " after " + firstName,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testFirstNameFirstBothUpper() {
    String firstUpper = firstName.toUpperCase();
    String secondUpper = secondName.toUpperCase();
    when( namedCluster1.getName() ).thenReturn( firstUpper );
    when( namedCluster2.getName() ).thenReturn( secondUpper );
    assertTrue( "Expected " + firstUpper + " before " + secondUpper,
      NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) < 0 );
    assertTrue( "Expected " + secondUpper + " after " + firstUpper,
      NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) > 0 );
  }

  @Test
  public void testEqual() {
    when( namedCluster1.getName() ).thenReturn( firstName );
    when( namedCluster2.getName() ).thenReturn( firstName );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) );
  }

  @Test
  public void testEqualOneUpper() {
    when( namedCluster1.getName() ).thenReturn( firstName.toUpperCase() );
    when( namedCluster2.getName() ).thenReturn( firstName );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) );
  }

  @Test
  public void testEqualBothUpper() {
    when( namedCluster1.getName() ).thenReturn( firstName.toUpperCase() );
    when( namedCluster2.getName() ).thenReturn( firstName.toUpperCase() );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster1, namedCluster2 ) );
    assertEquals( 0, NamedCluster.comparator.compare( namedCluster2, namedCluster1 ) );
  }
}
