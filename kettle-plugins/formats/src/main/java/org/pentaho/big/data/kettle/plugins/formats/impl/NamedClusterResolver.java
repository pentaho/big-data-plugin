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


package org.pentaho.big.data.kettle.plugins.formats.impl;

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

  }

  protected synchronized MetastoreLocator getMetastoreLocator() {
    if ( this.metaStoreService == null ) {
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        this.metaStoreService = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        LOG.logError( "Error getting MetastoreLocator", e );
      }
    }
    return this.metaStoreService;
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
        namedCluster = namedClusterService.getNamedClusterByName( hostName, getMetastoreLocator().getMetastore( ) );
        if ( namedCluster == null && embeddedMetastoreKey != null ) {
          namedCluster = namedClusterService
            .getNamedClusterByName( hostName, getMetastoreLocator().getExplicitMetastore( embeddedMetastoreKey ) );
        }
      } else {
        namedCluster =
          namedClusterService.getNamedClusterByHost( hostName, getMetastoreLocator().getMetastore( embeddedMetastoreKey ) );
        if ( namedCluster == null && embeddedMetastoreKey != null ) {
          namedCluster = namedClusterService
            .getNamedClusterByHost( hostName, getMetastoreLocator().getExplicitMetastore( embeddedMetastoreKey ) );
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
