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

package org.pentaho.di.trans.steps.couchdbinput;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.logging.LoggingObjectInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.trans.steps.mock.StepMockHelper;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
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
    when( stepMockHelper.logChannelInterfaceFactory.create( anyObject(), any( LoggingObjectInterface.class ) ) )
      .thenReturn( mock( LogChannelInterface.class ) );
    httpClientFactory = mock( CouchDbInput.HttpClientFactory.class );
    getMethodFactory = mock( CouchDbInput.GetMethodFactory.class );
    couchDbInput =
      new CouchDbInput( stepMockHelper.stepMeta, stepMockHelper.stepDataInterface, 0, stepMockHelper.transMeta,
        stepMockHelper.trans, httpClientFactory, getMethodFactory );
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

    GetMethod getMethod = mock( GetMethod.class );
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

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    HttpState httpState = mock( HttpState.class );
    HttpClientParams httpClientParams = mock( HttpClientParams.class );
    when( httpClient.getState() ).thenReturn( httpState );
    when( httpClient.getParams() ).thenReturn( httpClientParams );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 200 );

    assertTrue( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
    ArgumentCaptor<UsernamePasswordCredentials> argumentCaptor =
      ArgumentCaptor.forClass( UsernamePasswordCredentials.class );
    verify( httpState ).setCredentials( eq( AuthScope.ANY ), argumentCaptor.capture() );
    UsernamePasswordCredentials usernamePasswordCredentials = argumentCaptor.getValue();
    assertEquals( testUser, usernamePasswordCredentials.getUserName() );
    assertEquals( testPassword, usernamePasswordCredentials.getPassword() );
    verify( httpClientParams ).setAuthenticationPreemptive( true );
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

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 200 );

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

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 200 );

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

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    HttpState httpState = mock( HttpState.class );
    HttpClientParams httpClientParams = mock( HttpClientParams.class );
    when( httpClient.getState() ).thenReturn( httpState );
    when( httpClient.getParams() ).thenReturn( httpClientParams );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 199 );
    when( getMethod.getResponseBodyAsStream() ).thenReturn( new ByteArrayInputStream( "fail".getBytes() ) );

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

    GetMethod getMethod = mock( GetMethod.class );
    when( getMethodFactory.create( CouchDbInput
      .buildUrl( testHostname, Const.toInt( testPort, 5984 ), testDbName, testDoc, testView ) ) ).thenReturn(
      getMethod );

    HttpClient httpClient = mock( HttpClient.class );
    HttpState httpState = mock( HttpState.class );
    HttpClientParams httpClientParams = mock( HttpClientParams.class );
    when( httpClient.getState() ).thenReturn( httpState );
    when( httpClient.getParams() ).thenReturn( httpClientParams );
    when( httpClientFactory.createHttpClient() ).thenReturn( httpClient );
    when( httpClient.executeMethod( getMethod ) ).thenReturn( 199 );

    assertFalse( couchDbInput.init( couchDbInputMeta, couchDbInputData ) );
  }
}
