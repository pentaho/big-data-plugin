/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;


import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for NamedClusterBridgeImpl. This is a bridge class so most tests will/should be integration tests.
 */
public class NamedClusterBridgeImplTest {
  private org.pentaho.di.core.namedcluster.model.NamedCluster namedCluster;
  private NamedClusterBridgeImpl namedClusterBridge;

  private String namedClusterName;

  @Before
  public void setup() {

    namedCluster = mock( org.pentaho.di.core.namedcluster.model.NamedCluster.class );
    namedClusterName = "namedClusterName";
    when( namedCluster.toString() ).thenReturn( "Named cluster: " + namedClusterName );

    namedClusterBridge = new NamedClusterBridgeImpl( namedCluster );
  }

  @Test
  public void testToString() {
    assertEquals( "Named cluster: " + namedClusterName, namedClusterBridge.toString() );
  }
}
