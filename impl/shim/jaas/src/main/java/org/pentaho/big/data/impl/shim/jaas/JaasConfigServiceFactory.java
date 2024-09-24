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
package org.pentaho.big.data.impl.shim.jaas;

import org.pentaho.hadoop.shim.api.jaas.JaasConfigService;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceFactory;

import java.util.Properties;

public class JaasConfigServiceFactory implements NamedClusterServiceFactory<JaasConfigService> {

  public JaasConfigServiceFactory(
    @SuppressWarnings( "unused" ) boolean isActiveConfiguration, Object hadoopConfiguration ) {
  }
  @Override public Class<JaasConfigService> getServiceClass() {
    return JaasConfigService.class;
  }

  @Override public boolean canHandle( NamedCluster namedCluster ) {
    return true;
  }

  @Override public JaasConfigService create( NamedCluster namedCluster ) {
    return new JaasConfigServiceImpl( new Properties() );
  }
}
