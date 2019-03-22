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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.pentaho.di.core.util.HttpClientManager;
import org.pentaho.di.core.util.HttpClientUtil;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;


/**
 * Created by dstepanov on 26/04/17.
 */
public class GatewayConnectivityTestImpl extends ConnectivityTestImpl {
  public static final String GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_DESC =
    "GatewayConnectTest.Success.Desc";
  public static final String GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_MESSAGE =
    "GatewayConnectTest.Success.Message";
  public static final String GATEWAY_CONNECT_TEST_CONNECT_UNKNOWN_RETURN_CODE_DESC =
    "GatewayConnectTest.UnknownReturnCode.Desc";
  public static final String GATEWAY_CONNECT_TEST_CONNECT_UNKNOWN_RETURN_CODE_MESSAGE =
    "GatewayConnectTest.UnknownReturnCode.Message";
  public static final String GATEWAY_CONNECT_TEST_SERVICE_NOT_FOUND_DESC =
    "GatewayConnectTest.ServiceNotFound.Desc";
  public static final String GATEWAY_CONNECT_TEST_SERVICE_NOT_FOUND_MESSAGE =
    "GatewayConnectTest.ServiceNotFound.Message";
  public static final String GATEWAY_CONNECT_TEST_FORBIDDEN_DESC =
    "GatewayConnectTest.Forbidden.Desc";
  public static final String GATEWAY_CONNECT_TEST_FORBIDDEN_MESSAGE =
    "GatewayConnectTest.Forbidden.Message";
  public static final String GATEWAY_CONNECT_TLSCONTEXT_DESC =
    "GatewayConnectTest.TLSContext.Desc";
  public static final String GATEWAY_CONNECT_SSLEXCEPTION_MESSAGE =
    "GatewayConnectTest.SSLException.Message";
  public static final String GATEWAY_CONNECT_SSLEXCEPTION_DESC =
    "GatewayConnectTest.SSLException.Desc";
  public static final String GATEWAY_CONNECT_TLSCONTEXT_MESSAGE =
    "GatewayConnectTest.TLSContext.Message";
  public static final String GATEWAY_CONNECT_TEST_UNAUTHORIZED_DESC =
    "GatewayConnectTest.Unauthorized.Desc";
  public static final String GATEWAY_CONNECT_TEST_UNAUTHORIZED_MESSAGE =
    "GatewayConnectTest.Unauthorized.Message";
  public static final String GATEWAY_CONNECT_TLSCONTEXTINIT_DESC =
    "GatewayConnectTest.TLSContextInit.Desc";
  public static final String GATEWAY_CONNECT_TLSCONTEXTINIT_MESSAGE =
    "GatewayConnectTest.TLSContextInit.Message";
  public static final String GATEWAY_CONNECT_EXECUTION_FAILED_DESC =
    "GatewayConnectTest.ExecutionFailed.Desc";
  public static final String GATEWAY_CONNECT_EXECUTION_FAILED_MESSAGE =
    "GatewayConnectTest.ExecutionFailed.Message";

  private static final Class<?> PKG = GatewayConnectivityTestImpl.class;
  private final URI uri;
  private final String path;
  private final String user;
  private final String password;
  private final Variables variables;
  private HttpClientManager httpClientManager = HttpClientManager.getInstance();

  public GatewayConnectivityTestImpl( MessageGetterFactory messageGetterFactory, URI uri, String testPath,
                                      String user, String password, RuntimeTestEntrySeverity severity ) {
    super( messageGetterFactory, uri.getHost(), Integer.toString( uri.getPort() ), true,
      severity );

    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    variables = new Variables();
    variables.initializeVariablesFrom( null );
    this.path = variables.environmentSubstitute( testPath );
    this.password = variables.environmentSubstitute( password );
    this.user = variables.environmentSubstitute( user );
    this.uri = uri.resolve( uri.getPath() + path );
  }

  @Override
  public RuntimeTestResultEntry runTest() {

    if ( StringUtils.isBlank( hostname ) ) {
      return new RuntimeTestResultEntryImpl( severityOfFalures,
        messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_DESC ),
        messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_MESSAGE ) );
    } else {

      try {
        Integer portInt = Integer.parseInt( port );
        // Ignore ssl certificate issues if KETTLE_KNOX_IGNORE_SSL = true
        if ( variables.getBooleanValueOfVariable( "${KETTLE_KNOX_IGNORE_SSL}", false ) ) {
          SSLContext ctx = getTlsContext();
          initContextWithTrustAll( ctx );
          SSLContext.setDefault( ctx );
        }
        String userString = "";
        HttpClientContext context = null;
        HttpGet method = new HttpGet( uri.toString() );
        HttpClient httpClient;

        if ( StringUtils.isNotBlank( user ) ) {
          userString = user;
          httpClient = getHttpClient( user, password );
          context = HttpClientUtil.createPreemptiveBasicAuthentication( uri.getHost(), portInt, user, password );
        } else {
          httpClient = getHttpClient();
        }

        HttpResponse httpResponse =
          context != null ? httpClient.execute( method, context ) : httpClient.execute( method );
        Integer returnCode = httpResponse.getStatusLine().getStatusCode();

        switch ( returnCode ) {
          case 200: {
            return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.INFO,
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_DESC ),
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_CONNECT_SUCCESS_MESSAGE, uri.toString() ) );
          }
          case 404: {
            return new RuntimeTestResultEntryImpl( severityOfFalures,
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_SERVICE_NOT_FOUND_DESC ),
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_SERVICE_NOT_FOUND_MESSAGE, uri.toString() ) );
          }
          case 403: {
            return new RuntimeTestResultEntryImpl( severityOfFalures,
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_FORBIDDEN_DESC ),
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_FORBIDDEN_MESSAGE, uri.toString(), userString ) );
          }
          case 401: {
            return new RuntimeTestResultEntryImpl( severityOfFalures,
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_UNAUTHORIZED_DESC ),
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_UNAUTHORIZED_MESSAGE, uri.toString(), userString ) );
          } default: {
            return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.WARNING,
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_CONNECT_UNKNOWN_RETURN_CODE_DESC ),
              messageGetter.getMessage( GATEWAY_CONNECT_TEST_CONNECT_UNKNOWN_RETURN_CODE_MESSAGE, userString,
                returnCode.toString(), uri.toString() ) );
          }
        }
      } catch ( NoSuchAlgorithmException e ) {
        return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( GATEWAY_CONNECT_TLSCONTEXT_DESC ),
          messageGetter.getMessage( GATEWAY_CONNECT_TLSCONTEXT_MESSAGE ), e );
      } catch ( SSLException e ) {
        return new RuntimeTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( GATEWAY_CONNECT_SSLEXCEPTION_DESC ),
          messageGetter.getMessage( GATEWAY_CONNECT_SSLEXCEPTION_MESSAGE, uri.toString(), e.getMessage() ), e );
      } catch ( UnknownHostException e ) {
        return new RuntimeTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( CONNECT_TEST_UNKNOWN_HOSTNAME_DESC ),
          messageGetter.getMessage( CONNECT_TEST_UNKNOWN_HOSTNAME_MESSAGE, uri.getHost() ), e );
      } catch ( KeyManagementException e ) {
        return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( GATEWAY_CONNECT_TLSCONTEXTINIT_DESC ),
          messageGetter.getMessage( GATEWAY_CONNECT_TLSCONTEXTINIT_MESSAGE ), e );
      } catch ( IOException e ) {
        return new RuntimeTestResultEntryImpl( severityOfFalures,
          messageGetter.getMessage( GATEWAY_CONNECT_EXECUTION_FAILED_DESC ),
          messageGetter.getMessage( GATEWAY_CONNECT_EXECUTION_FAILED_MESSAGE, uri.toString() ), e );
      } catch ( NumberFormatException e ) {
        return new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.FATAL,
          messageGetter.getMessage( CONNECT_TEST_PORT_NUMBER_FORMAT_DESC ),
          messageGetter.getMessage( CONNECT_TEST_PORT_NUMBER_FORMAT_MESSAGE, port ), e );
      }
    }
  }

  void initContextWithTrustAll( SSLContext ctx ) throws KeyManagementException {
    ctx.init( new KeyManager[ 0 ], new TrustManager[] { new X509TrustManager() {

      @Override public void checkClientTrusted( X509Certificate[] x509Certificates, String s )
        throws CertificateException {

      }

      @Override public void checkServerTrusted( X509Certificate[] x509Certificates, String s )
        throws CertificateException {

      }

      @Override public X509Certificate[] getAcceptedIssuers() {
        return null;
      }
    } }, new SecureRandom() );
  }

  SSLContext getTlsContext() throws NoSuchAlgorithmException {
    return SSLContext.getInstance( "TLS" );
  }

  @VisibleForTesting
  HttpClient getHttpClient() {
    return httpClientManager.createDefaultClient();
  }

  @VisibleForTesting
  HttpClient getHttpClient( String user, String password ) {
    HttpClientManager.HttpClientBuilderFacade clientBuilder = httpClientManager.createBuilder();
    clientBuilder.setCredentials( user, password );
    return clientBuilder.build();
  }

}
