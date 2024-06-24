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

package org.pentaho.big.data.impl.cluster;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;
import org.pentaho.di.trans.TransMeta;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Created by tkafalas on 7/25/2017.
 */
public class NamedClusterServiceNewTransExtensionPointTest {

  @Test
  public void testCallExtensionPoint() throws Exception {
    NamedClusterService mockNamedClusterService = mock( NamedClusterService.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    TransMeta mockTransMeta = mock( TransMeta.class );
    NamedClusterServiceNewTransExtensionPoint namedClusterServiceExtensionPoint =
      new NamedClusterServiceNewTransExtensionPoint( mockNamedClusterService );

    namedClusterServiceExtensionPoint.callExtensionPoint( logChannelInterface, mockTransMeta );
    verify( mockTransMeta ).setNamedClusterServiceOsgi( any( NamedClusterServiceOsgi.class ) );
  }

}
