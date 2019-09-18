/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.impl;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.hadoop.shim.api.format.FormatService;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public abstract class NamedClusterResolver {

  private static String extractScheme( String fileUri ) {
    String scheme = null;
    try {
      scheme = new URI( fileUri ).getScheme();
      return scheme;
    } catch ( URISyntaxException e ) {
      e.printStackTrace();
    }
    return scheme;
  }

  private static String extractHostName( String fileUri ) {
    String hostName = null;
    try {
      hostName = new URI( fileUri ).getHost();
      return hostName;
    } catch ( URISyntaxException e ) {
      e.printStackTrace();
    }
    return hostName;
  }

  public static NamedCluster resolveNamedCluster( NamedClusterServiceLocator namedClusterServiceLocator,
                                                  NamedClusterService namedClusterService,
                                                  MetastoreLocatorOsgi metaStoreService, String fileUri ) {
    NamedCluster namedCluster = null;
    if ( fileUri != null ) {
      String scheme = extractScheme( fileUri );
      String hostName = extractHostName( fileUri );
      if ( scheme != null && scheme.equals( "hc" ) ) {
        namedCluster = namedClusterService.getNamedClusterByName( hostName, metaStoreService.getMetastore() );
      } else {
        namedCluster = namedClusterService.getNamedClusterByHost( hostName, metaStoreService.getMetastore() );
      }
    }
    if ( namedCluster == null ) {
      namedCluster =
        getNamedClusterWithExistingFormatService( namedClusterServiceLocator, namedClusterService, metaStoreService );
    }
    return namedCluster;
  }

  private static NamedCluster getNamedClusterWithExistingFormatService(
    NamedClusterServiceLocator namedClusterServiceLocator, NamedClusterService namedClusterService,
    MetastoreLocatorOsgi metaStoreService ) {
    try {
      List<NamedCluster> namedClusters = namedClusterService.list( metaStoreService.getMetastore() );
      for ( NamedCluster nc : namedClusters ) {
        if ( namedClusterServiceLocator.getDefaultShim().equals( nc.getShimIdentifier() ) ) {
          FormatService formatService = namedClusterServiceLocator.getService( nc, FormatService.class );
          if ( formatService != null ) {
            return nc;
          }
        }
      }
    } catch ( MetaStoreException e ) {
      e.printStackTrace();
    } catch ( ClusterInitializationException e ) {
      e.printStackTrace();
    }
    return null;
  }
}
