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

package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.plugins.common.ui.NamedClusterDialogImpl;

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
public class NamedClusterDialogBridgeImplTest {
  private NamedClusterDialogImpl delegate;
  private NamedClusterDialogBridgeImpl namedClusterDialogBridge;

  @Before
  public void setup() {
    delegate = mock( NamedClusterDialogImpl.class );
    namedClusterDialogBridge = new NamedClusterDialogBridgeImpl( delegate );
  }

  @Test
  public void testGetNamedCluster() {
    NamedCluster namedCluster = mock( NamedCluster.class );
    String testName = "testName";
    when( namedCluster.getName() ).thenReturn( testName );
    when( delegate.getNamedCluster() ).thenReturn( namedCluster );
    assertEquals( testName, namedClusterDialogBridge.getNamedCluster().getName() );
  }

  @Test
  public void testSetNamedCluster() {
    org.pentaho.di.core.namedcluster.model.NamedCluster namedCluster =
      mock( org.pentaho.di.core.namedcluster.model.NamedCluster.class );
    String testName = "testName";
    when( namedCluster.getName() ).thenReturn( testName );
    namedClusterDialogBridge.setNamedCluster( namedCluster );
    ArgumentCaptor<NamedCluster> namedClusterArgumentCaptor = ArgumentCaptor.forClass( NamedCluster.class );
    verify( delegate ).setNamedCluster( namedClusterArgumentCaptor.capture() );
    assertEquals( testName, namedClusterArgumentCaptor.getValue().getName() );
  }

  @Test
  public void testSetNewClusterCheckTrue() {
    namedClusterDialogBridge.setNewClusterCheck( true );
    verify( delegate ).setNewClusterCheck( true );
  }

  @Test
  public void testSetNewClusterCheckFalse() {
    namedClusterDialogBridge.setNewClusterCheck( false );
    verify( delegate ).setNewClusterCheck( false );
  }

  @Test
  public void testOpen() {
    String openResult = "openResult";
    when( delegate.open() ).thenReturn( openResult );
    assertEquals( openResult, namedClusterDialogBridge.open() );
  }
}
