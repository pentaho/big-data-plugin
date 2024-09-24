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

package org.pentaho.big.data.impl.cluster.tests.hdfs;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.pentaho.big.data.impl.cluster.tests.ClusterRuntimeTestEntry;
import org.pentaho.di.core.util.HttpClientManager;
import org.pentaho.di.core.util.HttpClientUtil;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.runtime.test.i18n.MessageGetterFactory;
import org.pentaho.runtime.test.result.RuntimeTestEntrySeverity;
import org.pentaho.runtime.test.result.RuntimeTestResultEntry;
import org.pentaho.runtime.test.result.RuntimeTestResultSummary;
import org.pentaho.runtime.test.result.org.pentaho.runtime.test.result.impl.RuntimeTestResultSummaryImpl;
import org.pentaho.runtime.test.test.impl.RuntimeTestResultEntryImpl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Arrays;

/**
 * Created by vamshidhar on 8/14/15.
 */
public class GatewayWriteToAndDeleteFromUsersHomeFolderTest extends WriteToAndDeleteFromUsersHomeFolderTest {

  public static final String CONNECT_TEST_HOST_BLANK_DESC = "WriteToAndDeleteFromUsersHomeFolderTest.HostBlank.Desc";
  public static final String CONNECT_TEST_HOST_BLANK_MESSAGE =
    "WriteToAndDeleteFromUsersHomeFolderTest.HostBlank.Message";

  public static final String CONNECT_FILE_SYSTEM_TEST_PATH = "/webhdfs/v1/?op=LISTSTATUS";
  public static final String PENTAHO_SHIM_TEST_FILE_PATH = "/webhdfs/v1/~/pentaho-shim-test-file.test?op=LISTSTATUS";
  public static final String PENTAHO_SHIM_TEST_FILE_PATH_DELETE = "/webhdfs/v1/~/pentaho-shim-test-file.test?op=DELETE";
  public static final String PENTAHO_SHIM_TEST_FILE_PATH_CREATE = "/webhdfs/v1/~/pentaho-shim-test-file.test?op=CREATE";

  private final HttpClientManager httpClientManager = HttpClientManager.getInstance();

  public GatewayWriteToAndDeleteFromUsersHomeFolderTest( MessageGetterFactory messageGetterFactory,
                                                         HadoopFileSystemLocator hadoopFileSystemLocator ) {
    super( messageGetterFactory, hadoopFileSystemLocator );
  }

  @Override
  public RuntimeTestResultSummary runTest( Object objectUnderTest ) {

    // Safe to cast as our accepts method will only return true for named clusters
    NamedCluster namedCluster = (NamedCluster) objectUnderTest;

    // The connection information might be parameterized. Since we aren't tied to a transformation or job, in order to
    // use a parameter, the value would have to be set as a system property or in kettle.properties, etc.
    // Here we try to resolve the parameters if we can:
    Variables variables = new Variables();
    variables.initializeVariablesFrom( null );

    if ( !namedCluster.isUseGateway() ) {
      return super.runTest( objectUnderTest );
    } else {
      String url = namedCluster.getGatewayUrl();
      String password =
        namedCluster.decodePassword( variables.environmentSubstitute( namedCluster.getGatewayPassword() ) );
      String username = variables.environmentSubstitute( namedCluster.getGatewayUsername() );

      URI uri = URI.create( url );
      String hostname = uri.getHost();
      int port = uri.getPort();
      boolean ignoreSSL = variables.getBooleanValueOfVariable( "${KETTLE_KNOX_IGNORE_SSL}", false );

      if ( StringUtils.isBlank( hostname ) ) {
        return new RuntimeTestResultSummaryImpl( new RuntimeTestResultEntryImpl( RuntimeTestEntrySeverity.WARNING,
          messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_DESC ),
          messageGetter.getMessage( CONNECT_TEST_HOST_BLANK_MESSAGE ) ) );
      }

      boolean exists;
      try {
        exists = doesFileExists( url, username, password, port, ignoreSSL );
      } catch ( IOException | NoSuchAlgorithmException | KeyManagementException e ) {
        return new RuntimeTestResultSummaryImpl(
          new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.FATAL, messageGetter
            .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_DESC ),
            messageGetter
              .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CHECKING_IF_FILE_EXISTS_MESSAGE,
                CONNECT_FILE_SYSTEM_TEST_PATH, CONNECT_FILE_SYSTEM_TEST_PATH ),
            e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
      }

      if ( exists ) {
        return new RuntimeTestResultSummaryImpl(
          new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
            messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_DESC ),
            messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_FILE_EXISTS_MESSAGE,
              CONNECT_FILE_SYSTEM_TEST_PATH,
              CONNECT_FILE_SYSTEM_TEST_PATH ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
      } else {
        String fileLocationUrl;
        try {
          fileLocationUrl = createFile( url, username, password, port, ignoreSSL );
        } catch ( IOException | NoSuchAlgorithmException | KeyManagementException e ) {
          return new RuntimeTestResultSummaryImpl(
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
              messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_DESC ),
              messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_CREATING_FILE_MESSAGE,
                CONNECT_FILE_SYSTEM_TEST_PATH, CONNECT_FILE_SYSTEM_TEST_PATH ),
              e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
        }
        RuntimeTestResultEntry writeExceptionEntry = null;
        try {
          if ( !appendContentToFile( fileLocationUrl, username, password, port, ignoreSSL ) ) {
            writeExceptionEntry = new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
              messageGetter
                .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_DESC ),
              messageGetter.getMessage(
                WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_MESSAGE,
                CONNECT_FILE_SYSTEM_TEST_PATH, CONNECT_FILE_SYSTEM_TEST_PATH ),
              ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY );
          }
        } catch ( IOException | NoSuchAlgorithmException | KeyManagementException e ) {
          writeExceptionEntry = new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
            messageGetter
              .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_DESC ),
            messageGetter.getMessage(
              WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_TO_FILE_MESSAGE,
              CONNECT_FILE_SYSTEM_TEST_PATH, CONNECT_FILE_SYSTEM_TEST_PATH ),
            e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY );
        }

        try {
          if ( deleteFile( url, username, password, port, ignoreSSL ) ) {
            if ( writeExceptionEntry == null ) {
              return new RuntimeTestResultSummaryImpl(
                new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.INFO,
                  messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_DESC ),
                  messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_SUCCESS_MESSAGE,
                    PENTAHO_SHIM_TEST_FILE_PATH ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
            } else {
              return new RuntimeTestResultSummaryImpl( writeExceptionEntry );
            }
          } else {
            if ( writeExceptionEntry == null ) {
              return new RuntimeTestResultSummaryImpl(
                new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
                  messageGetter.getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_DESC ),
                  messageGetter.getMessage(
                    WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_UNABLE_TO_DELETE_MESSAGE,
                    PENTAHO_SHIM_TEST_FILE_PATH,
                    PENTAHO_SHIM_TEST_FILE_PATH ), ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ) );
            } else {
              return new RuntimeTestResultSummaryImpl( writeExceptionEntry );
            }
          }
        } catch ( IOException | NoSuchAlgorithmException | KeyManagementException e ) {
          RuntimeTestResultEntryImpl deleteExceptionEntry =
            new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
              messageGetter
                .getMessage( WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_DESC ),
              messageGetter.getMessage(
                WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_DELETING_FILE_MESSAGE,
                PENTAHO_SHIM_TEST_FILE_PATH, PENTAHO_SHIM_TEST_FILE_PATH ),
              e, ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY );
          if ( writeExceptionEntry == null ) {
            return new RuntimeTestResultSummaryImpl( deleteExceptionEntry );
          } else {
            return new RuntimeTestResultSummaryImpl(
              new ClusterRuntimeTestEntry( messageGetterFactory, RuntimeTestEntrySeverity.WARNING,
                messageGetter.getMessage(
                  WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_DELETING_FILE_DESC ), messageGetter
                .getMessage(
                  WRITE_TO_AND_DELETE_FROM_USERS_HOME_FOLDER_TEST_ERROR_WRITING_DELETING_FILE_MESSAGE ),
                ClusterRuntimeTestEntry.DocAnchor.ACCESS_DIRECTORY ),
              Arrays.asList( writeExceptionEntry, deleteExceptionEntry ) );
          }
        }
      }
    }
  }

  private boolean doesFileExists( String url, String user, String password, int port, boolean ignoreSSL )
    throws NoSuchAlgorithmException, IOException, KeyManagementException {

    ServerResponse response =
      runServiceTest( RequestType.GET, URI.create( url + PENTAHO_SHIM_TEST_FILE_PATH ), user, password, port,
        ignoreSSL );

    return response.getStatusCode() == 200;
  }

  private String createFile( String url, String user, String password, int port, boolean ignoreSSL )
    throws NoSuchAlgorithmException, IOException, KeyManagementException {
    ServerResponse responseCreate =
      runServiceTest( RequestType.CREATE, URI.create( url + PENTAHO_SHIM_TEST_FILE_PATH_CREATE ), user, password, port,
        ignoreSSL );

    if ( responseCreate.getStatusCode() == 307 ) {
      return responseCreate.getLocationHeader();
    } else {
      return null;
    }
  }

  private boolean appendContentToFile( String location, String user, String password, int port, boolean ignoreSSL )
    throws NoSuchAlgorithmException, IOException, KeyManagementException {
    ServerResponse responseWrite =
      runServiceTest( RequestType.APPEND, URI.create( location ), user, password, port,
        ignoreSSL );

    return responseWrite.getStatusCode() == 201;
  }

  private boolean deleteFile( String url, String user, String password, int port, boolean ignoreSSL )
    throws NoSuchAlgorithmException, IOException, KeyManagementException {

    ServerResponse response =
      runServiceTest( RequestType.DELETE, URI.create( url + PENTAHO_SHIM_TEST_FILE_PATH_DELETE ), user, password, port,
        ignoreSSL );

    return response.getStatusCode() == 200;
  }

  private ServerResponse runServiceTest( RequestType requestType, URI uri, String user, String password, int port,
                                         boolean ignoreSSL )
    throws NoSuchAlgorithmException, KeyManagementException, IOException {

    // Ignore ssl certificate issues if KETTLE_KNOX_IGNORE_SSL = true
    if ( ignoreSSL ) {
      SSLContext ctx = getTlsContext();
      initContextWithTrustAll( ctx );
      SSLContext.setDefault( ctx );
    }
    HttpClientContext context = null;

    HttpUriRequest method = getHttpRequestMethod( requestType, uri );

    CloseableHttpClient httpClient = null;
    try {
      if ( StringUtils.isNotBlank( user ) ) {
        httpClient = getHttpClient( user, password );
        context = HttpClientUtil.createPreemptiveBasicAuthentication( uri.getHost(), port, user, password );
      } else {
        httpClient = httpClientManager.createDefaultClient();
      }

      HttpResponse httpResponse =
        context != null ? httpClient.execute( method, context ) : httpClient.execute( method );

      Header locationHeader = httpResponse.getFirstHeader( "Location" );
      return new ServerResponse( locationHeader != null ? locationHeader.getValue() : null,
        httpResponse.getStatusLine().getStatusCode() );
    } finally {
      if ( httpClient != null ) {
        httpClient.close();
      }
    }
  }

  private HttpUriRequest getHttpRequestMethod( RequestType requestType, URI uri ) throws UnsupportedEncodingException {
    if ( requestType == RequestType.GET ) {
      return new HttpGet( uri.toString() );
    } else if ( requestType == RequestType.APPEND ) {
      HttpPut putMethod = new HttpPut( uri );
      putMethod.setEntity( new StringEntity( HELLO_CLUSTER ) );
      return putMethod;
    } else if ( requestType == RequestType.CREATE ) {
      return new HttpPut( uri );
    } else if ( requestType == RequestType.DELETE ) {
      return new HttpDelete( uri );
    }

    return null;
  }

  void initContextWithTrustAll( SSLContext ctx ) throws KeyManagementException {
    ctx.init( new KeyManager[ 0 ], new TrustManager[] { new X509TrustManager() {

      @Override public void checkClientTrusted( X509Certificate[] x509Certificates, String s ) {
        // Nothing to do
      }

      @Override public void checkServerTrusted( X509Certificate[] x509Certificates, String s ) {
        // Nothing to do
      }

      @Override public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[ 0 ];
      }
    } }, new SecureRandom() );
  }

  SSLContext getTlsContext() throws NoSuchAlgorithmException {
    return SSLContext.getInstance( "TLS" );
  }

  CloseableHttpClient getHttpClient( String user, String password ) {
    HttpClientManager.HttpClientBuilderFacade clientBuilder = httpClientManager.createBuilder();
    clientBuilder.setCredentials( user, password );
    return clientBuilder.build();
  }

  enum RequestType {
    GET, APPEND, CREATE, DELETE
  }

  static class ServerResponse {
    private final String locationHeader;
    private final int statusCode;

    ServerResponse( String locationHeader, int statusCode ) {
      this.locationHeader = locationHeader;
      this.statusCode = statusCode;
    }

    public String getLocationHeader() {
      return locationHeader;
    }

    public int getStatusCode() {
      return statusCode;
    }
  }
}