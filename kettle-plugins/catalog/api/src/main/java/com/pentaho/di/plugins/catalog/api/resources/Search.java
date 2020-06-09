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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.search.FacetsResult;
import org.apache.http.entity.StringEntity;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchCriteria;
import com.pentaho.di.plugins.catalog.api.entities.search.SearchResult;

import java.io.UnsupportedEncodingException;

public class Search extends Resource {

  private static final String BASE_URL = CatalogClient.API_ROOT + "/search";
  private static final String NEW = BASE_URL + "/new";
  private static final String FACETS = BASE_URL + "/facets";

  public Search( CatalogClient catalogClient ) {
    super( catalogClient );
  }

  public SearchResult doNew( SearchCriteria searchCriteria ) {
    StringEntity stringEntity = null;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      String body = objectMapper.writeValueAsString( searchCriteria );
      stringEntity = new StringEntity( body );
    } catch ( UnsupportedEncodingException | JsonProcessingException ignored ) {
      // Do nothing
    }
    return write( NEW, stringEntity, SearchResult.class );
  }

  public SearchResult doNew( String body ) {
    StringEntity stringEntity = null;
    try {
      stringEntity = new StringEntity( body );
    } catch ( UnsupportedEncodingException ignored ) {
      // Do nothing
    }
    return write( NEW, stringEntity, SearchResult.class );
  }

  public FacetsResult doFacets() {
    return read( FACETS, FacetsResult.class );
  }
}
