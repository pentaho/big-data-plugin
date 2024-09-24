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

package org.pentaho.di.trans.steps.couchdbinput;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/28/15.
 */
public class CouchDbInputTest {
  private String testName;
  private StepMockHelper stepMockHelper;
  private CouchDbInput couchDbInput;
  private CouchDbInput.HttpClientFactory httpClientFactory;
  private CouchDbInput.GetMethodFactory getMethodFactory;

  @Before
  public void setup() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init( false );
    Encr.init( "Kettle" );
    testName = "testName";
    stepMockHelper = new StepMockHelper( testName, CouchDbInputMeta.class, CouchDbInputData.class );
    when( stepMockHelper.logChannelInterfaceFactory.create( any(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mock( LogChannelInterface.class ) );
    httpClientFactory = mock( CouchDbInput.HttpClientFactory.class );
    getMethodFactory = mock( CouchDbInput.GetMethodFactory.class );
    couchDbInput =
      spy( new CouchDbInput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans ) );
  }

  @Test
  public void testInitException() {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "testView";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );

    CouchDbInput.HttpClientFactory httpClientFactory = mock( CouchDbInput.HttpClientFactory.class );
    CouchDbInput.GetMethodFactory getMethodFactory = mock( CouchDbInput.GetMethodFactory.class );

    HttpGet getMethod = mock( HttpGet.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    when( httpClientFactory.createHttpClient() ).thenThrow( new RuntimeException() );
    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }

  @Test
  public void testInit() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "testView";
    final String testUser = "testUser";
    final String testPassword = "testPassword";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );
    when( couchDbInputMeta.getAuthenticationUser() ).thenReturn( testUser );
    when( couchDbInputMeta.getAuthenticationPassword() ).thenReturn( testPassword );

    HttpGet getMethod = mock( HttpGet.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    doReturn( httpClient ).when( couchDbInput ).createHttpClient( anyString(), anyString() );
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    HttpEntity httpEntity = mock(HttpEntity.class);
    doReturn( httpEntity ).when( httpResponseMock ).getEntity();
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any() );
    doReturn( httpResponseMock ).when( httpClient ).execute( any( HttpUriRequest.class ), any( HttpContext.class ) );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 200 ).when( statusLineMock ).getStatusCode();
    assertTrue( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
    verify( couchDbInput ).createHttpClient( "testUser", "testPassword" );
    verify( couchDbInput ).getHttpClientContext( "testHostname", Integer.parseInt( testPort ) );
  }

  @Test
  public void testInitNoDesignDoc() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "";
    final String testView = "testView";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );

    HttpGet getMethod = mock( HttpGet.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    doReturn( httpClient ).when( couchDbInput ).createHttpClient( anyString(), anyString() );
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any() );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 200 ).when( statusLineMock ).getStatusCode();

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }

  @Test
  public void testInitNoView() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );

    HttpGet getMethod = mock( HttpGet.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    doReturn( httpClient ).when( couchDbInput ).createHttpClient( anyString(), anyString() );
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any() );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 200 ).when( statusLineMock ).getStatusCode();

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }

  @Test
  public void testInit199() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "testView";
    final String testUser = "testUser";
    final String testPassword = "testPassword";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );
    when( couchDbInputMeta.getAuthenticationUser() ).thenReturn( testUser );
    when( couchDbInputMeta.getAuthenticationPassword() ).thenReturn( testPassword );

    HttpGet getMethod = mock( HttpGet.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    doReturn( httpClient ).when( couchDbInput ).createHttpClient( anyString(), anyString() );
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any() );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 199 ).when( statusLineMock ).getStatusCode();

    //when( getMethod.getResponseBodyAsStream() ).thenReturn( new ByteArrayInputStream( "fail".getBytes() ) );

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }

  @Test
  public void testInit300() throws IOException {
    CouchDbInputMeta couchDbInputMeta = (CouchDbInputMeta) stepMockHelper.initStepMetaInterface;
    CouchDbInputData couchDbInputData = (CouchDbInputData) stepMockHelper.initStepDataInterface;

    final String testHostname = "testHostname";
    final String testPort = "9999";
    final String testDbName = "testDbName";
    final String testDoc = "testDoc";
    final String testView = "testView";
    final String testUser = "testUser";
    final String testPassword = "testPassword";

    when( couchDbInputMeta.getHostname() ).thenReturn( testHostname );
    when( couchDbInputMeta.getPort() ).thenReturn( testPort );
    when( couchDbInputMeta.getDbName() ).thenReturn( testDbName );
    when( couchDbInputMeta.getDesignDocument() ).thenReturn( testDoc );
    when( couchDbInputMeta.getViewName() ).thenReturn( testView );
    when( couchDbInputMeta.getAuthenticationUser() ).thenReturn( testUser );
    when( couchDbInputMeta.getAuthenticationPassword() ).thenReturn( testPassword );

    HttpGet getMethod = mock( HttpGet.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    doReturn( httpClient ).when( couchDbInput ).createHttpClient( anyString(), anyString() );
    HttpResponse httpResponseMock = mock(HttpResponse.class);
    StatusLine statusLineMock = mock(StatusLine.class);
    doReturn( httpResponseMock ).when( httpClient ).execute( any() );
    doReturn( statusLineMock ).when( httpResponseMock ).getStatusLine();
    doReturn( 199 ).when( statusLineMock ).getStatusCode();

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }
}
