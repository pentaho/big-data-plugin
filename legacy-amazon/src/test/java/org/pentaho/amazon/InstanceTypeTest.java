/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by Aliaksandr_Zhuk on 2/8/2018.
 */
@RunWith( MockitoJUnitRunner.class )
public class InstanceTypeTest {

  private List<String> expectedSortedInstanceTypes;
  private List<InstanceType> instanceTypes;

  @Before
  public void setUp() {
    expectedSortedInstanceTypes = new ArrayList<>();

    InstanceType cMedium = new InstanceType( "c1.medium", "optimized" );
    InstanceType c2xLarge = new InstanceType( "c1.2xlarge", "optimized" );
    InstanceType c4xLarge = new InstanceType( "c1.4xlarge", "optimized" );
    InstanceType mMedium = new InstanceType( "m3.medium", "memory" );
    InstanceType mxLarge = new InstanceType( "m3.xlarge", "memory" );
    InstanceType m2xLarge = new InstanceType( "m3.2xlarge", "memory" );
    InstanceType m4xLarge = new InstanceType( "m3.4xlarge", "memory" );

    expectedSortedInstanceTypes.add( "c1.medium" );
    expectedSortedInstanceTypes.add( "c1.2xlarge" );
    expectedSortedInstanceTypes.add( "c1.4xlarge" );
    expectedSortedInstanceTypes.add( "m3.medium" );
    expectedSortedInstanceTypes.add( "m3.xlarge" );
    expectedSortedInstanceTypes.add( "m3.2xlarge" );
    expectedSortedInstanceTypes.add( "m3.4xlarge" );

    instanceTypes = new ArrayList<>();

    instanceTypes.add( m4xLarge );
    instanceTypes.add( cMedium );
    instanceTypes.add( c2xLarge );
    instanceTypes.add( mMedium );
    instanceTypes.add( m2xLarge );
    instanceTypes.add( c4xLarge );
    instanceTypes.add( mxLarge );
  }

  @Test
  public void testSortInstanceTypes_getValidOrderOfSortedElements() {

    List<String> sortedInstanceTypes = InstanceType.sortInstanceTypes( instanceTypes );

    assertEquals( expectedSortedInstanceTypes, sortedInstanceTypes );
  }
}
