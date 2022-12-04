/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2022 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats;

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.metastore.locator.api.MetastoreLocator;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Optional;

public class NamedClusterResolver {

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;
  private MetastoreLocator metaStoreService;

  public NamedClusterResolver( NamedClusterServiceLocator namedClusterServiceLocator,
                               NamedClusterService namedClusterService ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
    try {
      Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
      this.metaStoreService = metastoreLocators.stream().findFirst().get();
    } catch ( Exception e ) {
      LOG.logError( "Error getting MetastoreLocator", e );
    }
  }

  private static final LogChannelInterface LOG = LogChannel.GENERAL;

  public NamedCluster resolveNamedCluster( String fileName ) {
    return resolveNamedCluster( fileName, null );
  }

  public NamedCluster resolveNamedCluster( String fileName, String embeddedMetastoreKey ) {
    NamedCluster namedCluster = null;
    Optional<URI> uri = fileUri( fileName );

    if ( uri.isPresent() ) {
      String scheme = uri.get().getScheme();
      String hostName = uri.get().getHost();
      if ( scheme != null && scheme.equals( "hc" ) ) {
        namedCluster = namedClusterService.getNamedClusterByName( hostName, metaStoreService.getMetastore( ) );
        if ( namedCluster == null && embeddedMetastoreKey != null ) {
          namedCluster = namedClusterService
            .getNamedClusterByName( hostName, metaStoreService.getExplicitMetastore( embeddedMetastoreKey ) );
        }
      } else {
        namedCluster =
          namedClusterService.getNamedClusterByHost( hostName, metaStoreService.getMetastore( embeddedMetastoreKey ) );
        if ( namedCluster == null && embeddedMetastoreKey != null ) {
          namedCluster = namedClusterService
            .getNamedClusterByHost( hostName, metaStoreService.getExplicitMetastore( embeddedMetastoreKey ) );
        }
      }
    }
    return namedCluster;
  }

  private Optional<URI> fileUri( String fileName ) {
    try {
      return Optional.of( new URI( fileName ) );
    } catch ( URISyntaxException e ) {
      LOG.logDebug( String.format( "Couldn't parse %s as a URI.", fileName ) );
      return Optional.empty();
    }
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }
}
