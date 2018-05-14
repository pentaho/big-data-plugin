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

package org.pentaho.big.data.impl.cluster.tests.oozie;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTest;
import org.pentaho.runtime.test.network.ConnectivityTestFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;

import java.net.MalformedURLException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

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
    RuntimeTestResultSummary runtimeTestResultSummary = pingOozieHostTest.runTest( namedCluster );
    verifyRuntimeTestResultEntry( runtimeTestResultSummary.getOverallStatusEntry(),
      RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( PingOozieHostTest.PING_OOZIE_HOST_TEST_MALFORMED_URL_DESC ),
      messageGetter.getMessage( PingOozieHostTest.PING_OOZIE_HOST_TEST_MALFORMED_URL_MESSAGE, oozieUrl ),
      MalformedURLException.class );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testSuccess() {
    RuntimeTestResultEntry results = mock( RuntimeTestResultEntry.class );
    String testDescription = "test-description";
    when( results.getDescription() ).thenReturn( testDescription );
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory.create( messageGetterFactory, oozieHost, ooziePort, false ) )
      .thenReturn( connectivityTest );
    when( connectivityTest.runTest() ).thenReturn( results );
    RuntimeTestResultSummary runtimeTestResultSummary = pingOozieHostTest.runTest( namedCluster );
    assertEquals( testDescription, runtimeTestResultSummary.getOverallStatusEntry().getDescription() );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }
}
