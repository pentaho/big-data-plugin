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

package com.pentaho.di.plugins.catalog.api.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.authentication.LoginResult;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class Authentication extends Resource {

  public Authentication( CatalogClient catalogClient ) {
    super( catalogClient );
  }

  private static final String SET_COOKIE = "Set-Cookie";
  private static final String LOGIN_URL = CatalogClient.API_ROOT + "/login";

  public boolean login( String username, String password ) {
    String json = "{\"username\":\"" + username + "\", \"password\":\"" + password + "\"}";
    StringEntity entity;
    try {
      entity = new StringEntity( json );
    } catch ( UnsupportedEncodingException uee ) {
      return false;
    }

    HttpResponse response;
    try {
      response = catalogClient.doPost( LOGIN_URL, entity );
    } catch ( IOException ioe ) {
      return false;
    }

    ObjectMapper objectMapper = new ObjectMapper();
    LoginResult loginResult;
    try {
      loginResult = objectMapper.readValue( response.getEntity().getContent(), LoginResult.class );
    } catch ( IOException ioe ) {
      return false;
    }

    if ( loginResult != null ) {
      Header header = response.getFirstHeader( SET_COOKIE );
      if ( header != null && header.getElements().length > 0 ) {
        HeaderElement headerElement = header.getElements()[ 0 ];
        catalogClient.setSessionId( headerElement.getValue() );
      }
    } else {
      return false;
    }

    return loginResult.getLoginResult().equals( LoginResult.SUCCESS );
  }

  public void logout() {
    // Do nothing.
  }

}
