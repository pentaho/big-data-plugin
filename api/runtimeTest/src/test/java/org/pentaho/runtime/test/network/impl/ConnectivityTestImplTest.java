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


package org.pentaho.runtime.test.network.impl;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by bryan on 8/21/15.
 */
public class ConnectivityTestImplTest {
  private String hostname;
  private String port;
  private boolean haPossible;
  private RuntimeTestEntrySeverity severityOfFailures;
  private ConnectivityTestImpl.SocketFactory socketFactory;
  private ConnectivityTestImpl.InetAddressFactory inetAddressFactory;
  private ConnectivityTestImpl connectTest;
  private MessageGetterFactory messageGetterFactory;
  private MessageGetter messageGetter;
  private InetAddress inetAddress;
  private Socket socket;

  @Before
  public void setup() throws IOException {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( ConnectivityTestImpl.class );
    hostname = "hostname";
    port = "89";
    haPossible = false;
    severityOfFailures = RuntimeTestEntrySeverity.WARNING;
    socketFactory = mock( ConnectivityTestImpl.SocketFactory.class );
    socket = mock( Socket.class );
    when( socketFactory.create( hostname, Integer.valueOf( port ) ) ).thenReturn( socket );
    inetAddressFactory = mock( ConnectivityTestImpl.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenReturn( true );
    init();
  }

  private void init() {
    connectTest =
      new ConnectivityTestImpl( messageGetterFactory, hostname, port, haPossible, severityOfFailures, socketFactory,
        inetAddressFactory );
  }

  @Test
  public void testBlankHostname() {
    hostname = "";
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_HOST_BLANK_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_HOST_BLANK_MESSAGE ) );
  }

  @Test
  public void testBlankPortNoHa() {
    port = "";
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_PORT_BLANK_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_PORT_BLANK_MESSAGE ) );
  }

  @Test
  public void testBlankPortHa() {
    port = "";
    haPossible = true;
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.INFO,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_HA_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_HA_MESSAGE, hostname ) );
  }

  @Test
  public void testNonNumericPort() {
    port = "abc";
    haPossible = true;
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_PORT_NUMBER_FORMAT_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE, port ), NumberFormatException.class );
  }

  @Test
  public void testUnreachableHostname() throws IOException {
    inetAddressFactory = mock( ConnectivityTestImpl.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenReturn( false );
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_UNREACHABLE_DESC, hostname ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_UNREACHABLE_MESSAGE, hostname ) );
  }

  @Test
  public void testUnknownHostException() throws IOException {
    inetAddressFactory = mock( ConnectivityTestImpl.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenThrow( new UnknownHostException() );
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_UNKNOWN_HOSTNAME_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE, hostname ), UnknownHostException.class );
  }

  @Test
  public void testReachableIOException() throws IOException {
    inetAddressFactory = mock( ConnectivityTestImpl.InetAddressFactory.class );
    inetAddress = mock( InetAddress.class );
    when( inetAddressFactory.create( hostname ) ).thenReturn( inetAddress );
    when( inetAddress.isReachable( anyInt() ) ).thenThrow( new IOException() );
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_NETWORK_ERROR_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_NETWORK_ERROR_MESSAGE, hostname, port ), IOException.class );
  }

  @Test
  public void testSocketIOException() throws IOException {
    when( socketFactory.create( hostname, Integer.valueOf( port ) ) ).thenThrow( new IOException() );
    init();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_CONNECT_FAIL_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_CONNECT_FAIL_MESSAGE, hostname, port ), IOException.class );
  }

  @Test
  public void testSuccess() throws IOException {
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.INFO,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_CONNECT_SUCCESS_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_CONNECT_SUCCESS_MESSAGE, hostname, port ) );
    verify( socket ).close();
  }
}
