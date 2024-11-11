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
 *
 * @deprecated
 */
@Deprecated
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
