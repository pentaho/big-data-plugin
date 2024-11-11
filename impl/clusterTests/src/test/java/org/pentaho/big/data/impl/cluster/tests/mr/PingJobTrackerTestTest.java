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


package org.pentaho.big.data.impl.cluster.tests.mr;

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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 8/24/15.
 */
public class PingJobTrackerTestTest {
  private MessageGetterFactory messageGetterFactory;
  private ConnectivityTestFactory connectivityTestFactory;
  private PingJobTrackerTest pingJobTrackerTest;
  private NamedCluster namedCluster;
  private MessageGetter messageGetter;
  private String jobTrackerHost;
  private String jobTrackerPort;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( PingJobTrackerTest.class );
    connectivityTestFactory = mock( ConnectivityTestFactory.class );
    pingJobTrackerTest = new PingJobTrackerTest( messageGetterFactory, connectivityTestFactory );
    jobTrackerHost = "jobTrackerHost";
    jobTrackerPort = "829";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getJobTrackerHost() ).thenReturn( jobTrackerHost );
    when( namedCluster.getJobTrackerPort() ).thenReturn( jobTrackerPort );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( PingJobTrackerTest.PING_JOB_TRACKER_TEST_NAME ),
      pingJobTrackerTest.getName() );
  }

  @Test
  public void testSuccess() {
    RuntimeTestResultEntry results = mock( RuntimeTestResultEntry.class );
    String testDescription = "test-description";
    when( results.getDescription() ).thenReturn( testDescription );
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory.create( messageGetterFactory, jobTrackerHost, jobTrackerPort, true ) )
      .thenReturn( connectivityTest );
    when( connectivityTest.runTest() ).thenReturn( results );
    RuntimeTestResultSummary runtimeTestResultSummary = pingJobTrackerTest.runTest( namedCluster );
    assertEquals( testDescription, runtimeTestResultSummary.getOverallStatusEntry().getDescription() );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIsMapR() {
    when( namedCluster.isMapr() ).thenReturn( true );
    RuntimeTestResultSummary runtimeTestResultSummary = pingJobTrackerTest.runTest( namedCluster );
    RuntimeTestResultEntry results = runtimeTestResultSummary.getOverallStatusEntry();
    assertEquals( RuntimeTestEntrySeverity.INFO, results.getSeverity() );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }
}
