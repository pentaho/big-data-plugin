/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.runtime.test.network.impl;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.runtime.test.TestMessageGetterFactory;
import org.pentaho.runtime.test.i18n.MessageGetter;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.pentaho.runtime.test.RuntimeTestEntryUtil.verifyRuntimeTestResultEntry;

/**
 * Created by dstepanov on 29/04/17.
 */
public class GatewayConnectivityTestImplTest {

  public static final String HTTPS = "https://";
  public static final String HTTP = "http://";
  public static final String KETTLE_KNOX_IGNORE_SSL = "KETTLE_KNOX_IGNORE_SSL";
  private String hostname;
  private String port;
  private RuntimeTestEntrySeverity severityOfFailures;
  private ConnectivityTestImpl connectTest;
  private MessageGetterFactory messageGetterFactory;
  private MessageGetter messageGetter;
  private URI uri;
  private String path;
  private String topology;
  private String user;
  private String password;
  private HttpClient httpClient;

  @Before
  public void setup() throws IOException {
    messageGetterFactory = new TestMessageGetterFactory();
    messageGetter = messageGetterFactory.create( ConnectivityTestImpl.class );
    hostname = "hostname";
    port = "8443";
    user = "user";
    password = "password";
    topology = "/gateway/default";
    path = "/testPath";
    uri = URI.create( HTTPS + hostname + ":" + port + topology );
    severityOfFailures = RuntimeTestEntrySeverity.WARNING;
    httpClient = mock( HttpClient.class, Mockito.CALLS_REAL_METHODS );
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( anyObject() );
    doReturn( httpResponseMock ).when( httpClient ).execute( any( HttpUriRequest.class ), any( HttpContext.class) );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 200 ).when( statusLineMock ).getStatusCode();
    init();
    System.setProperty( KETTLE_KNOX_IGNORE_SSL, "false" );
  }

  private void init() {
    connectTest =
      new GatewayConnectivityTestImpl( messageGetterFactory, uri, path, user, password, severityOfFailures ) {
        @Override
        HttpClient getHttpClient() {
          return HttpClients.createDefault();
        }
      };
  }

  private void initMock() {
    connectTest =
      new GatewayConnectivityTestImpl( messageGetterFactory, uri, path, user, password, severityOfFailures ) {
        @Override
        HttpClient getHttpClient() {
          return httpClient;
        }
        @Override
        HttpClient getHttpClient( String user, String password ) {
          return httpClient;
        }
      };
  }

  @Test
  public void testHttp() throws IOException {
    uri = URI.create( HTTP + hostname + ":" + port + topology );
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.INFO,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_DESC ),
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_MESSAGE,
        uri.toString() + path ) );
  }

  @Test
  public void testBlankHostname() {
    uri = URI.create( HTTPS + "" + ":" + port + topology );
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_HOST_BLANK_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_HOST_BLANK_MESSAGE ) );
  }

  @Test
  public void testUnknownHostException() throws IOException {
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( ConnectivityTestImpl.CONNECT_TEST_UNKNOWN_HOSTNAME_DESC ), messageGetter.getMessage(
        ConnectivityTestImpl.CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE, hostname ), UnknownHostException.class );
  }

  @Test
  public void testIOException() throws IOException {
    doThrow( new IOException() ).when( httpClient )
      .execute( any( HttpUriRequest.class ), any( HttpContext.class ) );
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_EXECUTION_FAILED_DESC ),
      messageGetter.getMessage(
        GatewayConnectivityTestImpl.GATEWAY_CONNECT_EXECUTION_FAILED_MESSAGE, uri.toString() + path ),
      IOException.class );
  }

  @Test
  public void testSSLException() throws IOException {
    doThrow( new SSLException( "errorMessage" ) ).when( httpClient )
      .execute( any( HttpUriRequest.class ), any( HttpContext.class ) );
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_SSLEXCEPTION_DESC ),
      messageGetter.getMessage(
        GatewayConnectivityTestImpl.GATEWAY_CONNECT_SSLEXCEPTION_MESSAGE, uri.toString() + path, "errorMessage" ),
      SSLException.class );
  }

  @Test
  public void testNoSuchAlgorithmException() {
    System.setProperty( KETTLE_KNOX_IGNORE_SSL, "true" );
    connectTest =
      new GatewayConnectivityTestImpl( messageGetterFactory, uri, path, user, password, severityOfFailures ) {

        @Override SSLContext getTlsContext() throws NoSuchAlgorithmException {
          throw new NoSuchAlgorithmException();
        }
      };
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TLSCONTEXT_DESC ),
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TLSCONTEXT_MESSAGE ),
      NoSuchAlgorithmException.class );
  }

  @Test
  public void testKeyManagementException() {
    System.setProperty( KETTLE_KNOX_IGNORE_SSL, "true" );
    connectTest =
      new GatewayConnectivityTestImpl( messageGetterFactory, uri, path, user, password, severityOfFailures ) {

        @Override void initContextWithTrustAll( SSLContext ctx ) throws KeyManagementException {
          throw new KeyManagementException();
        }

      };
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.FATAL,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TLSCONTEXTINIT_DESC ),
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TLSCONTEXTINIT_MESSAGE ),
      KeyManagementException.class );
  }

  @Test
  public void testSuccess() throws IOException {
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.INFO,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_DESC ),
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_MESSAGE,
        uri.toString() + path ) );
  }

  @Test
  public void test401() throws IOException {
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any( HttpUriRequest.class ), any( HttpContext.class) );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 401 ).when( statusLineMock ).getStatusCode();
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_UNAUTHORIZED_DESC ),
      messageGetter
        .getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_UNAUTHORIZED_MESSAGE, uri.toString() + path,
          user ) );
  }

  @Test
  public void test403() throws IOException {
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any( HttpUriRequest.class ), any( HttpContext.class) );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 403 ).when( statusLineMock ).getStatusCode();
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_FORBIDDEN_DESC ),
      messageGetter.getMessage(
        GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_FORBIDDEN_MESSAGE, uri.toString() + path, user ) );
  }

  @Test
  public void test404() throws IOException {
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any( HttpUriRequest.class ), any( HttpContext.class) );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 404 ).when( statusLineMock ).getStatusCode();
    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), severityOfFailures,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_SERVICE_NOT_FOUND_DESC ),
      messageGetter.getMessage(
        GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_SERVICE_NOT_FOUND_MESSAGE, uri.toString() + path ) );
  }

  @Test
  public void testUnknownCode() throws IOException {
    Integer returnCode = 0;
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any( HttpUriRequest.class ), any( HttpContext.class ) );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( returnCode ).when( statusLineMock ).getStatusCode();

    initMock();
    verifyRuntimeTestResultEntry( connectTest.runTest(), RuntimeTestEntrySeverity.WARNING,
      messageGetter.getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_CONNECT_UNKNOWN_RETURN_CODE_DESC ),
      messageGetter
        .getMessage( GatewayConnectivityTestImpl.GATEWAY_CONNECT_TEST_CONNECT_UNKNOWN_RETURN_CODE_MESSAGE, user,
          returnCode.toString(), uri.toString() + path ) );
  }

}