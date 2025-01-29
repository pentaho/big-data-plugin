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


package org.pentaho.big.data.impl.cluster.tests.hdfs;

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
public class PingFileSystemEntryPointTestTest {
  private MessageGetterFactory messageGetterFactory;
  private ConnectivityTestFactory connectivityTestFactory;
  private PingFileSystemEntryPointTest fileSystemEntryPointTest;
  private NamedCluster namedCluster;
  private MessageGetter messageGetter;
  private String hdfsHost;
  private String hdfsPort;

  @Before
  public void setup() {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( PingFileSystemEntryPointTest.class );
    connectivityTestFactory = mock( ConnectivityTestFactory.class );
    fileSystemEntryPointTest = new PingFileSystemEntryPointTest( messageGetterFactory, connectivityTestFactory );
    hdfsHost = "hdfsHost";
    hdfsPort = "8025";
    namedCluster = mock( NamedCluster.class );
    when( namedCluster.getHdfsHost() ).thenReturn( hdfsHost );
    when( namedCluster.getHdfsPort() ).thenReturn( hdfsPort );
    when( namedCluster.isMapr() ).thenReturn( false );
  }

  @Test
  public void testGetName() {
    assertEquals( messageGetter.getMessage( PingFileSystemEntryPointTest.PING_FILE_SYSTEM_ENTRY_POINT_TEST_NAME ),
      fileSystemEntryPointTest.getName() );
  }

  @Test
  public void testSuccess() {
    RuntimeTestResultEntry results = mock( RuntimeTestResultEntry.class );
    String testDescription = "test-description";
    when( results.getDescription() ).thenReturn( testDescription );
    ConnectivityTest connectivityTest = mock( ConnectivityTest.class );
    when( connectivityTestFactory.create( messageGetterFactory, hdfsHost, hdfsPort, true ) )
      .thenReturn( connectivityTest );
    when( connectivityTest.runTest() ).thenReturn( results );
    RuntimeTestResultSummary runtimeTestResultSummary = fileSystemEntryPointTest.runTest( namedCluster );
    assertEquals( testDescription, runtimeTestResultSummary.getOverallStatusEntry().getDescription() );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }

  @Test
  public void testIsMapR() {
    when( namedCluster.isMapr() ).thenReturn( true );
    RuntimeTestResultSummary runtimeTestResultSummary = fileSystemEntryPointTest.runTest( namedCluster );
    RuntimeTestResultEntry results = runtimeTestResultSummary.getOverallStatusEntry();
    assertEquals( RuntimeTestEntrySeverity.INFO, results.getSeverity() );
    assertEquals( 0, runtimeTestResultSummary.getRuntimeTestResultEntries().size() );
  }
}
