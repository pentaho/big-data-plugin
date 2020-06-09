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

package com.pentaho.di.plugins.catalog.provider;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.connections.ConnectionProvider;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.function.Supplier;

public class CatalogProvider implements ConnectionProvider<CatalogDetails> {

  private Supplier<ConnectionManager> connectionManagerSupplier = ConnectionManager::getInstance;
  private Supplier<VariableSpace> variableSpace = Variables::getADefaultVariableSpace;

  public static final String CATALOG = "Catalog";
  public static final String KEY = "catalog";

  @Override public String getName() {
    return CATALOG;
  }

  @Override public String getKey() {
    return KEY;
  }

  @Override public Class<CatalogDetails> getClassType() {
    return CatalogDetails.class;
  }

  @Override public List<String> getNames() {
    return connectionManagerSupplier.get().getNamesByType( getClass() );
  }

  @Override public List<CatalogDetails> getConnectionDetails() {
    return (List<CatalogDetails>) connectionManagerSupplier.get().getConnectionDetailsByScheme( getKey() );
  }

  @Override public boolean test( CatalogDetails connectionDetails ) {
    URL url;
    try {
      url = new URL( getVar( connectionDetails.getUrl(), variableSpace.get() ) );
    } catch ( MalformedURLException mue ) {
      return false;
    }

    CatalogClient catalogClient =
      new CatalogClient( url.getHost(), String.valueOf( url.getPort() ), url.getProtocol().equals( CatalogClient.HTTPS ) );
    return catalogClient.getAuthentication().login( getVar( connectionDetails.getUsername(), variableSpace.get() ),
      getVar( connectionDetails.getPassword(), variableSpace.get() ) );
  }

  @Override public CatalogDetails prepare( CatalogDetails connectionDetails ) {
    return connectionDetails;
  }

  private String getVar( String value, VariableSpace variableSpace ) {
    if ( variableSpace != null ) {
      return variableSpace.environmentSubstitute( value );
    }
    return value;
  }
}
