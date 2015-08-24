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

package org.pentaho.big.data.impl.cluster.tests.oozie;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.clusterTest.TestMessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetter;
import org.pentaho.big.data.api.clusterTest.i18n.MessageGetterFactory;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTest;
import org.pentaho.big.data.api.clusterTest.network.ConnectivityTestFactory;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestEntrySeverity;
import org.pentaho.big.data.api.clusterTest.test.ClusterTestResultEntry;

import java.net.MalformedURLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.expectOneEntry;
import static org.pentaho.big.data.api.clusterTest.ClusterTestEntryUtil.verifyClusterTestResultEntry;

/**
 * Created by bryan on 8/24/15.
 */
public class PingOozieHostTestTest {
  private MessageGetterFactory messageGetterFactory;
  private ConnectivityTestFactory connectivityTestFactory;
  private PingOozieHostTest pingOozieHostTest;
  private NamedCluster namedCluster;
  private MessageGetter messageGetter;
  private String oozieUrl;
  private String oozieHost;
  private String ooziePort;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( PingOozieHostTest.class );
    connectivityTestFactory = mock( ConnectivityTestFactory.class );
    pingOozieHostTest = new PingOozieHostTest( messageGetterFactory, connectivityTestFactory );
    oozieHost = "oozieHost";
    ooziePort = "8080";
    oozieUrl = "http://" + oozieHost + ":" + ooziePort + "/oozie";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getOozieUrl() ).thenReturn( oozieUrl );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( PingOozieHostTest.PING_OOZIE_HOST_TEST_NAME ),
      pingOozieHostTest.getName() );
  }

  @Test
  public void testMalformedURLException() {
    oozieUrl = "one-malformed-url";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getOozieUrl() ).thenReturn( oozieUrl );
    verifyClusterTestResultEntry( expectOneEntry( pingOozieHostTest.runTest( namedCluster ) ),
      ClusterTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingOozieHostTest.PING_OOZIE_HOST_TEST_MALFORMED_URL_DESC ),
      messageGetter.getMessage( PingOozieHostTest.PING_OOZIE_HOST_TEST_MALFORMED_URL_MESSAGE, oozieUrl ),
      MalformedURLException.class );
  }

  @Test
  public void testSuccess() {
    List<ClusterTestResultEntry> results = mock( List.class );
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory.create( messageGetterFactory, oozieHost, ooziePort, false ) )
      .thenReturn( connectivityTest );
    when( connectivityTest.runTest() ).thenReturn( results );
    assertEquals( results, pingOozieHostTest.runTest( namedCluster ) );
  }
}
