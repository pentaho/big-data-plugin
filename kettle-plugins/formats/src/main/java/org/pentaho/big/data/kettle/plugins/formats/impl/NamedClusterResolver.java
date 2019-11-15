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

import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;

import java.net.URI;
import java.net.URISyntaxException;

public class NamedClusterResolver {

  private NamedClusterResolver() {
    // static methods only
  }

  private static final LogChannelInterface LOG = LogChannel.GENERAL;

  private static String extractScheme( String fileUri ) {
    try {
      return new URI( fileUri ).getScheme();
    } catch ( URISyntaxException e ) {
      LOG.logError( "Failed to extract scheme for " + fileUri, e );
    }
    return null;
  }

  private static String extractHostName( String fileUri ) {
    try {
      return new URI( fileUri ).getHost();
    } catch ( URISyntaxException e ) {
      LOG.logError( "Failed to extract hostname for " + fileUri, e );
    }
    return null;
  }

  public static NamedCluster resolveNamedCluster( NamedClusterService namedClusterService,
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
    return namedCluster;
  }

}
