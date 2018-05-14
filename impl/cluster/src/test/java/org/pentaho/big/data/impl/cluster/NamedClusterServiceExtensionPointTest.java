/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.impl.cluster;

import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by tkafalas on 7/25/2017.
 */
public class NamedClusterServiceExtensionPointTest {

  @Test
  public void testCallExtensionPointWithTransMeta() throws Exception {
    NamedClusterService mockNamedClusterService = mock( NamedClusterService.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    TransMeta mockTransMeta = mock( TransMeta.class );
    NamedClusterServiceExtensionPoint namedClusterServiceExtensionPoint =
      new NamedClusterServiceExtensionPoint( mockNamedClusterService );

    namedClusterServiceExtensionPoint.callExtensionPoint( logChannelInterface, mockTransMeta );
    verify( mockTransMeta ).setNamedClusterServiceOsgi( any( NamedClusterServiceOsgi.class ) );
  }

  @Test
  public void testCallExtensionPointWithTrans() throws Exception {
    NamedClusterService mockNamedClusterService = mock( NamedClusterService.class );
    LogChannelInterface logChannelInterface = mock( LogChannelInterface.class );
    TransMeta mockTransMeta = mock( TransMeta.class );
    Trans mockTrans = mock( Trans.class );
    when( mockTrans.getTransMeta() ).thenReturn( mockTransMeta );
    NamedClusterServiceExtensionPoint namedClusterServiceExtensionPoint =
      new NamedClusterServiceExtensionPoint( mockNamedClusterService );

    namedClusterServiceExtensionPoint.callExtensionPoint( logChannelInterface, mockTrans );
    verify( mockTransMeta ).setNamedClusterServiceOsgi( any( NamedClusterServiceOsgi.class ) );
  }

}
