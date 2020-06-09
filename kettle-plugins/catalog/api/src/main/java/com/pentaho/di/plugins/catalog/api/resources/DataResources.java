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
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;

public class DataResources extends Resource {

  private static final String BASE_URL = CatalogClient.API_ROOT + "/dataresource";
  private static final String READ_URL = BASE_URL + "/";

  public DataResources( CatalogClient catalogClient ) {
    super( catalogClient );
  }

  public DataResource read( String key ) {
    return read( READ_URL + key, DataResource.class );
  }

  public DataResource update( String key, DataResource updateObj ) {
    StringEntity payload = null;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      String body = objectMapper.writeValueAsString( updateObj );
      payload = new StringEntity( body );
    } catch ( JsonProcessingException | UnsupportedEncodingException ex ) {
      //TODO: ignore for now until better reqs are finalized
    }
    return update( READ_URL + key, payload, DataResource.class );
  }
}
