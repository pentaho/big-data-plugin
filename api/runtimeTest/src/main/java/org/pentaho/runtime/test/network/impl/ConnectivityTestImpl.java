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

import org.pentaho.di.core.Const;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.network.ConnectivityTest;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bryan on 8/14/15.
 */
public class ConnectivityTestImpl implements ConnectivityTest {
  public static final String CONNECT_TEST_HOST_BLANK_DESC = "ConnectTest.HostBlank.Desc";
  public static final String CONNECT_TEST_HOST_BLANK_MESSAGE = "ConnectTest.HostBlank.Message";
  public static final String CONNECT_TEST_HA_DESC = "ConnectTest.HA.Desc";
  public static final String CONNECT_TEST_HA_MESSAGE = "ConnectTest.HA.Message";
  public static final String CONNECT_TEST_PORT_BLANK_DESC = "ConnectTest.PortBlank.Desc";
  public static final String CONNECT_TEST_PORT_BLANK_MESSAGE = "ConnectTest.PortBlank.Message";
  public static final String CONNECT_TEST_CONNECT_SUCCESS_DESC = "ConnectTest.ConnectSuccess.Desc";
  public static final String CONNECT_TEST_CONNECT_SUCCESS_MESSAGE = "ConnectTest.ConnectSuccess.Message";
  public static final String CONNECT_TEST_CONNECT_FAIL_DESC = "ConnectTest.ConnectFail.Desc";
  public static final String CONNECT_TEST_CONNECT_FAIL_MESSAGE = "ConnectTest.ConnectFail.Message";
  public static final String CONNECT_TEST_UNKNOWN_HOSTNAME_DESC = "ConnectTest.UnknownHostname.Desc";
  public static final String CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE = "ConnectTest.UnknownHostname.Message";
  public static final String CONNECT_TEST_NETWORK_ERROR_DESC = "ConnectTest.NetworkError.Desc";
  public static final String CONNECT_TEST_NETWORK_ERROR_MESSAGE = "ConnectTest.NetworkError.Message";
  public static final String CONNECT_TEST_PORT_NUMBER_FORMAT_DESC = "ConnectTest.PortNumberFormat.Desc";
  public static final String CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE = "ConnectTest.PortNumberFormat.Message";
  public static final String CONNECT_TEST_UNREACHABLE_DESC = "ConnectTest.Unreachable.Desc";
  public static final String CONNECT_TEST_UNREACHABLE_MESSAGE = "ConnectTest.Unreachable.Message";
  private static final Class<?> PKG = ConnectivityTestImpl.class;
  protected final MessageGetter messageGetter;
  protected final String hostname;
  protected final String port;
  private final boolean haPossible;
  protected final RuntimeTestEntrySeverity severityOfFalures;
  private final SocketFactory socketFactory;
  protected final InetAddressFactory inetAddressFactory;

  public ConnectivityTestImpl( MessageGetterFactory messageGetterFactory, String hostname, String port,
                               boolean haPossible ) {
    this( messageGetterFactory, hostname, port, haPossible, RuntimeTestEntrySeverity.FATAL );
  }

  public ConnectivityTestImpl( MessageGetterFactory messageGetterFactory, String hostname, String port,
                               boolean haPossible,
                               RuntimeTestEntrySeverity severityOfFailures ) {
    this( messageGetterFactory, hostname, port, haPossible, severityOfFailures, new SocketFactory(),
      new InetAddressFactory() );
  }

  public ConnectivityTestImpl( MessageGetterFactory messageGetterFactory, String hostname, String port,
                               boolean haPossible,
                               RuntimeTestEntrySeverity severityOfFailures, SocketFactory socketFactory,
                               InetAddressFactory inetAddressFactory ) {
    this.messageGetter = messageGetterFactory.create( PKG );

    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );

    this.hostname = variables.environmentSubstitute( hostname );
    this.port = variables.environmentSubstitute( port );
    this.haPossible = haPossible;
    this.severityOfFalures = severityOfFailures;
    this.socketFactory = socketFactory;
    this.inetAddressFactory = inetAddressFactory;
  }

  @Override public RuntimeTestResultEntry runTest() {
    List<RuntimeTestResultEntry> runtimeTestResultEntries = new ArrayList<>();

    if ( Const.isEmpty( hostname ) ) {
      return new RuntimeTestResultEntryImpl( severityOfFalures,
        messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_DESC ),
        messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_MESSAGE ) );
    } else if ( Const.isEmpty( port ) ) {
      if ( haPossible ) {
        return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.INFO,
          messageGetter.getMessage( CONNECT_TEST_HA_DESC ),
          messageGetter.getMessage( CONNECT_TEST_HA_MESSAGE, hostname ) );
      } else {
        return new RuntimeTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_PORT_BLANK_DESC ),
          messageGetter.getMessage( CONNECT_TEST_PORT_BLANK_MESSAGE ) );
      }
    } else {
      Socket socket = null;
      try {
        if ( inetAddressFactory.create( hostname ).isReachable( 10 * 1000 ) ) {
          try {
            socket = socketFactory.create( hostname, Integer.valueOf( port ) );
            return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.INFO,
              messageGetter.getMessage( CONNECT_TEST_CONNECT_SUCCESS_DESC ),
              messageGetter.getMessage( CONNECT_TEST_CONNECT_SUCCESS_MESSAGE, hostname, port ) );
          } catch ( IOException e ) {
            return new RuntimeTestResultEntryImpl( severityOfFalures,
              messageGetter.getMessage( CONNECT_TEST_CONNECT_FAIL_DESC ),
              messageGetter.getMessage( CONNECT_TEST_CONNECT_FAIL_MESSAGE, hostname, port ), e );
          } finally {
            if ( socket != null ) {
              try {
                socket.close();
              } catch ( IOException e ) {
                // Ignore
              }
            }
          }
        } else {
          return new RuntimeTestResultEntryImpl( severityOfFalures,
            messageGetter.getMessage( CONNECT_TEST_UNREACHABLE_DESC, hostname ),
            messageGetter.getMessage( CONNECT_TEST_UNREACHABLE_MESSAGE, hostname ) );
        }
      } catch ( UnknownHostException e ) {
        return new RuntimeTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_UNKNOWN_HOSTNAME_DESC ),
          messageGetter.getMessage( CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE, hostname ), e );
      } catch ( IOException e ) {
        return new RuntimeTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_NETWORK_ERROR_DESC ),
          messageGetter.getMessage( CONNECT_TEST_NETWORK_ERROR_MESSAGE, hostname, port ), e );
      } catch ( NumberFormatException e ) {
        return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( CONNECT_TEST_PORT_NUMBER_FORMAT_DESC ),
          messageGetter.getMessage( CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE, port ), e );
      }
    }
  }

  /**
   * Pulled out class to enable mock injection in tests
   */
  public static class SocketFactory {
    public Socket create( String hostname, int port ) throws IOException {
      return new Socket( hostname, port );
    }
  }

  /**
   * Pulled out class to enable mock injection in tests
   */
  public static class InetAddressFactory {
    public InetAddress create( String hostname ) throws UnknownHostException {
      return InetAddress.getByName( hostname );
    }
  }
}
