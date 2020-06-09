/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.api;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class ClientHelper {

  private ClientHelper() {
  }

  private static final String APPLICATION_JSON = "application/json";
  private static final String ACCEPT = "Accept";
  private static final String COOKIE = "Cookie";
  private static final String CONTENT_TYPE = "Content-type";
  private static final String WDSESSION_ID = "WDSessionId";

  @SuppressWarnings( "squid:S2095" )
  public static CloseableHttpResponse doPost( String sessionId, String url, HttpEntity entity ) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPost httpPost = new HttpPost( url );
    httpPost.setEntity( entity );
    httpPost.setHeader( ACCEPT, APPLICATION_JSON );
    httpPost.setHeader( CONTENT_TYPE, APPLICATION_JSON );
    httpPost.setHeader( COOKIE, WDSESSION_ID + "=" + sessionId );
    return client.execute( httpPost );
  }

  @SuppressWarnings( "squid:S2095" )
  public static CloseableHttpResponse doGet( String sessionId, String url ) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpGet httpGet = new HttpGet( url );
    httpGet.setHeader( ACCEPT, ClientHelper.APPLICATION_JSON );
    httpGet.setHeader( COOKIE, WDSESSION_ID + "=" + sessionId );
    return client.execute( httpGet );
  }

  @SuppressWarnings( "squid:S2095" )
  public static CloseableHttpResponse doPut( String sessionId, String url, HttpEntity entity ) throws IOException {
    CloseableHttpClient client = HttpClients.createDefault();
    HttpPut httpPut = new HttpPut( url );
    httpPut.setEntity( entity );
    httpPut.setHeader( CONTENT_TYPE, ClientHelper.APPLICATION_JSON );
    httpPut.setHeader( ACCEPT, ClientHelper.APPLICATION_JSON );
    httpPut.setHeader( COOKIE, WDSESSION_ID + "=" + sessionId );

    return client.execute( httpPut );
  }

}
