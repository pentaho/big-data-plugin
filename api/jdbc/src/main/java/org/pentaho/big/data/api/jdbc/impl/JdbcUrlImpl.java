/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.api.jdbc.impl;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.jdbc.JdbcUrl;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Created by bryan on 4/4/16.
 */
public class JdbcUrlImpl implements JdbcUrl {
  public static final String PENTAHO_NAMED_CLUSTER = "pentahoNamedCluster";
  private static final Logger logger = LoggerFactory.getLogger( JdbcUrlImpl.class );
  private final URI uri;
  private final Map<String, String> queryParams;
  private final NamedClusterService namedClusterService;
  private final MetastoreLocator metastoreLocator;

  public JdbcUrlImpl( String url, NamedClusterService namedClusterService, MetastoreLocator metastoreLocator )
    throws URISyntaxException {
    this.namedClusterService = namedClusterService;
    this.metastoreLocator = metastoreLocator;
    if ( !url.startsWith( "jdbc:" ) ) {
      throw new URISyntaxException( url, "Should start with \"jdbc:\"" );
    }
    uri = new URI( url.substring( 5 ) );
    String query = null;
    String path = uri.getPath();
    if ( path != null ) {
      int beginIndex = path.indexOf( ';' );
      if ( beginIndex >= 0 ) {
        query = path.substring( beginIndex );
      }
    }
    if ( query == null ) {
      queryParams = new HashMap<>();
    } else {
      queryParams = Arrays.asList( query.split( ";" ) ).stream()
        .map( s -> {
          int i = s.indexOf( '=' );
          if ( i < 0 || i >= s.length() - 1 ) {
            return null;
          }
          return new String[] { s.substring( 0, i ), s.substring( i + 1 ) };
        } )
        .filter( Objects::nonNull )
        .collect( Collectors.toMap( r -> r[ 0 ], t -> t[ 1 ] ) );
    }
  }

  @Override public String toString() {
    String queryParameters = queryParams.entrySet().stream()
      .map( entry -> entry.getKey() + "=" + entry.getValue() )
      .filter( Objects::nonNull )
      .sorted( String::compareToIgnoreCase )
      .collect( Collectors.joining( ";" ) );
    String path = uri.getPath();
    int semicolon = path.indexOf( ';' );
    if ( semicolon >= 0 ) {
      path = path.substring( 0, semicolon );
    }
    return "jdbc:" + uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + path
      + ( queryParameters != null && queryParameters.length() > 0 ? ";" + queryParameters : "" );
  }

  @Override public void setQueryParam( String key, String value ) {
    queryParams.put( key, value );
  }

  @Override public String getQueryParam( String key ) {
    return queryParams.get( key );
  }

  @Override public NamedCluster getNamedCluster()
    throws MetaStoreException {
    IMetaStore metaStore = metastoreLocator.getMetastore();
    if ( metaStore == null ) {
      return null;
    }
    String queryParam = getQueryParam( PENTAHO_NAMED_CLUSTER );
    if ( queryParam == null ) {
      return null;
    }
    return namedClusterService.read( queryParam, metaStore );
  }

  @Override
  public String getHost() {
    return uri.getHost();
  }
}
