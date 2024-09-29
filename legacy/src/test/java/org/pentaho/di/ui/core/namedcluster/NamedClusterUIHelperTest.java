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


package org.pentaho.di.ui.core.namedcluster;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Created by bryan on 8/31/15.
 */
public class NamedClusterUIHelperTest {
  @Test(timeout = 10000)
  public void testNamedClusterFactoryHolder() {
    final NamedClusterUIFactory namedClusterUIFactory = mock( NamedClusterUIFactory.class );
    final NamedClusterUIHelper.NamedClusterUIFactoryHolder namedClusterUIFactoryHolder = new NamedClusterUIHelper
      .NamedClusterUIFactoryHolder();
    new Thread( new Runnable() {
      @Override public void run() {
        try {
          Thread.sleep( 300 );
          namedClusterUIFactoryHolder.setNamedClusterUIFactory( namedClusterUIFactory );
        } catch ( InterruptedException e ) {
          e.printStackTrace();
        }
      }
    } ).start();
    assertEquals( namedClusterUIFactory, namedClusterUIFactoryHolder.getNamedClusterUIFactory() );
  }
}
