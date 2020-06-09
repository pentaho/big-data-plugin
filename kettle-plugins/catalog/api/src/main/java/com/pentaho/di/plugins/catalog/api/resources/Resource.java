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
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class Resource {
  protected CatalogClient catalogClient;
  private ObjectMapper objectMapper;

  public Resource( CatalogClient catalogClient ) {
    this.catalogClient = catalogClient;

    objectMapper = new ObjectMapper();
  }

  private <T> List<T> convertList( HttpResponse httpResponse, Class<T> clazz ) {
    try {
      return objectMapper.readValue( httpResponse.getEntity().getContent(),
              objectMapper.getTypeFactory().constructCollectionType( List.class, clazz ) );
    } catch ( IOException ioe ) {
      return new ArrayList<>();
    }
  }

  private <T> T convert( HttpResponse httpResponse, Class<T> clazz ) {
    try {
      return objectMapper.readValue( httpResponse.getEntity().getContent(), clazz );
    } catch ( IOException ioe ) {
      return null;
    }
  }

  public <T> T read( String url, Class<T> clazz ) {
    try ( CloseableHttpResponse httpResponse = catalogClient.doGet( url ) ) {
      if ( isGoodResponse( httpResponse ) ) {
        return convert( httpResponse, clazz );
      }
    } catch ( IOException ioe ) {
      return null;
    }

    return null;
  }

  public <T> T write( String url, HttpEntity body, Class<T> clazz ) {
    try ( CloseableHttpResponse httpResponse = catalogClient.doPost( url, body ) ) {
      if ( isGoodResponse( httpResponse ) ) {
        return convert( httpResponse, clazz );
      }
    } catch ( IOException ioe ) {
      return null;
    }

    return null;
  }

  public <T> T update( String url, HttpEntity body, Class<T> clazz ) {
    try ( CloseableHttpResponse httpResponse = catalogClient.doPut( url, body ) ) {
      if ( isGoodResponse( httpResponse ) ) {
        return convert( httpResponse, clazz );
      }
    } catch ( IOException ioe ) {
      return null;
    }

    return null;
  }

  public <T> List<T> readAll( String url, Class<T> clazz ) {
    try ( CloseableHttpResponse httpResponse = catalogClient.doGet( url ) ) {
      if ( isGoodResponse( httpResponse ) ) {
        return convertList( httpResponse, clazz );
      }
    } catch ( IOException ex ) {
      return new ArrayList<>();
    }
    return new ArrayList<>();
  }

  private boolean isGoodResponse( CloseableHttpResponse resp ) {
    return resp != null && resp.getStatusLine().getStatusCode() == 200 && resp.getEntity() != null;
  }
}
