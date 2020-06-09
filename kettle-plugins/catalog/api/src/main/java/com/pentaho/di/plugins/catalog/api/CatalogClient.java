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

import com.pentaho.di.plugins.catalog.api.resources.Authentication;
import com.pentaho.di.plugins.catalog.api.resources.DataResources;
import com.pentaho.di.plugins.catalog.api.resources.DataSources;
import com.pentaho.di.plugins.catalog.api.resources.Search;
import com.pentaho.di.plugins.catalog.api.resources.TagDomains;
import com.pentaho.di.plugins.catalog.api.resources.TagAssociations;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;

public class CatalogClient {

  public static final String API_VERSION = "v2";
  public static final String API_ROOT = "/api/" + API_VERSION;

  public static final String HTTP = "http";
  public static final String HTTPS = "https";

  private String host;
  private String port;
  private boolean secure;
  private String sessionId;

  private Authentication authentication;
  private DataResources dataResources;
  private DataSources dataSources;
  private Search search;
  private TagDomains tagDomains;
  private TagAssociations tagAssociations;

  public CatalogClient( String host, String port, boolean secure ) {
    this.host = host;
    this.port = port;
    this.secure = secure;

    authentication = new Authentication( this );
    dataResources = new DataResources( this );
    dataSources = new DataSources( this );
    search = new Search( this );
    tagDomains = new TagDomains( this );
    tagAssociations = new TagAssociations( this );
  }

  public CatalogClient( String host, String port ) {
    this( host, port, false );
  }

  public CatalogClient( String sessionId ) {
    this.sessionId = sessionId;
  }

  public String buildUrl( String path ) {
    return ( secure ? HTTPS : HTTP ) + "://" + host + ":" + port + path;
  }

  public CloseableHttpResponse doPost( String url, HttpEntity entity ) throws IOException {
    return ClientHelper.doPost( sessionId, buildUrl( url ), entity );
  }

  public CloseableHttpResponse doGet( String url ) throws IOException {
    return ClientHelper.doGet( sessionId, buildUrl( url ) );
  }

  public CloseableHttpResponse doPut( String url, HttpEntity entity ) throws IOException {
    return ClientHelper.doPut( sessionId, buildUrl( url ), entity );
  }

  public String getSessionId() {
    return sessionId;
  }

  public void setSessionId( String sessionId ) {
    this.sessionId = sessionId;
  }

  public Authentication getAuthentication() {
    return authentication;
  }

  public DataResources getDataResources() {
    return dataResources;
  }

  public DataSources getDataSources() {
    return dataSources;
  }

  public Search getSearch() {
    return search;
  }

  public void setSearch( Search search ) {
    this.search = search;
  }

  public TagDomains getTagDomains() {
    return tagDomains;
  }

  public void setTagDomains( TagDomains tagDomains ) {
    this.tagDomains = tagDomains;
  }

  public TagAssociations getTagAssociations() {
    return tagAssociations;
  }

  public void setTagAssociations( TagAssociations tagAssociations ) {
    this.tagAssociations = tagAssociations;
  }
}
