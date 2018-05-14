/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.eclipse.swt.events.SelectionListener;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/5/15.
 */
public class NamedClusterWidgetBridgedImplTest {
  private NamedClusterWidgetImpl namedClusterWidget;
  private NamedClusterWidgetBridgedImpl namedClusterWidgetBridged;

  @Before
  public void setup() {
    namedClusterWidget = mock( NamedClusterWidgetImpl.class );
    namedClusterWidgetBridged = new NamedClusterWidgetBridgedImpl( namedClusterWidget );
  }

  @Test
  public void testInitiate() {
    namedClusterWidgetBridged.initiate();
    verify( namedClusterWidget ).initiate();
  }

  @Test
  public void testGetComposite() {
    assertEquals( namedClusterWidget, namedClusterWidgetBridged.getComposite() );
  }

  @Test
  public void testGetSelectedNamedCluster() {
    org.pentaho.hadoop.shim.api.cluster.NamedCluster namedCluster = mock(
      org.pentaho.hadoop.shim.api.cluster.NamedCluster.class );
    String testName = "testName";
    when( namedCluster.getName() ).thenReturn( testName );
    when( namedClusterWidget.getSelectedNamedCluster() ).thenReturn( namedCluster );
    assertEquals( testName, namedClusterWidgetBridged.getSelectedNamedCluster().getName() );
  }

  @Test
  public void testSetSelectedNamedCluster() {
    String testName = "testName";
    namedClusterWidgetBridged.setSelectedNamedCluster( testName );
    verify( namedClusterWidget ).setSelectedNamedCluster( testName );
  }

  @Test
  public void testAddSelectionListener() {
    SelectionListener selectionListener = mock( SelectionListener.class );
    namedClusterWidgetBridged.addSelectionListener( selectionListener );
    verify( namedClusterWidget ).addSelectionListener( selectionListener );
  }
}
