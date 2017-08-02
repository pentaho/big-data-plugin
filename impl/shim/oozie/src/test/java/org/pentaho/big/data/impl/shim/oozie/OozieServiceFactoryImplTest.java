/*******************************************************************************
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/

package org.pentaho.big.data.impl.shim.oozie;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.oozie.OozieService;
import org.pentaho.hadoop.shim.ConfigurationException;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.oozie.shim.api.OozieClient;
import org.pentaho.oozie.shim.api.OozieClientFactory;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

@RunWith( MockitoJUnitRunner.class )
public class OozieServiceFactoryImplTest {

  @Mock HadoopConfiguration configuration;
  @Mock NamedCluster cluster;
  @Mock OozieClientFactory clientFactory;
  @Mock OozieClient oozieClient;
  OozieServiceFactoryImpl serviceFactory;
  private static final String OOZIE_URL = "http://oozieurl";

  @Before public void before() {
    serviceFactory = new OozieServiceFactoryImpl( true, configuration );
    when( cluster.getOozieUrl() ).thenReturn( OOZIE_URL );
  }

  @Test
  public void testGetServiceClass() throws Exception {
    assertEquals( serviceFactory.getServiceClass(),
      OozieService.class );
  }

  @Test
  public void testCanHandle() throws Exception {
    assertThat( serviceFactory.canHandle( cluster ),
      is( true ) );
  }

  @Test
  public void testCannotHandle() throws Exception {
    OozieServiceFactoryImpl serviceFactoryImpl =
      new OozieServiceFactoryImpl( false, configuration );
    assertThat( serviceFactoryImpl.canHandle( cluster ),
      is( false ) );
  }

  @Test
  public void testCannotHandleGateway() throws Exception {
    when( cluster.isUseGateway() ).thenReturn( true );
    assertThat( serviceFactory.canHandle( cluster ), is( false ) );
  }

  @Test
  public void testCreate() throws Exception {
    when( cluster.getOozieUrl() ).thenReturn( OOZIE_URL );
    when( configuration.getShim( OozieClientFactory.class ) )
      .thenReturn( clientFactory );
    when( clientFactory.create( OOZIE_URL ) )
      .thenReturn( oozieClient );
    serviceFactory.create( cluster );
    verify( clientFactory ).create( OOZIE_URL );

    assertThat( serviceFactory.create( cluster ), not( nullValue() ) );
  }

  @Test
  public void testFallbackCreate() throws Exception {
    when( configuration.getShim( OozieClientFactory.class ) )
      .thenThrow( mock( ConfigurationException.class ) );
    serviceFactory.create( cluster );
    verify( clientFactory, times( 0 ) ).create( OOZIE_URL );
    // did not create from the shim, but still return an OozieService
    assertThat( serviceFactory.create( cluster ), not( nullValue() ) );
  }

}
