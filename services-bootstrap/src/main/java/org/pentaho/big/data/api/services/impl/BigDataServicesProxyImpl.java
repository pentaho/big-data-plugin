/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.big.data.api.services.impl;

import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.bigdata.api.hdfs.impl.HadoopFileSystemLocatorImpl;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.service.ServiceProvider;
import org.pentaho.di.core.service.ServiceProviderInterface;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.internal.ShimIdentifier;
import org.pentaho.hadoop.shim.api.services.BigDataServicesProxy;

import java.util.HashMap;
import java.util.Map;

@ServiceProvider(
        id = "BigDataServicesProxy",
        description = "Provides access to shared big data services",
        provides = BigDataServicesProxy.class
)
public class BigDataServicesProxyImpl implements BigDataServicesProxy, ServiceProviderInterface<BigDataServicesProxy> {

    private static NamedClusterServiceLocator namedClusterServiceLocator = null;
    private static HadoopFileSystemLocator hadoopFileSystemLocator = null;

    @Override
    public boolean isSingleton() {
        return true;
    }


    @Override
    public NamedClusterServiceLocator getNamedClusterServiceLocator() {
        if ( namedClusterServiceLocator == null ) {
            namedClusterServiceLocator = NamedClusterServiceLocatorImpl.getInstance();
        }
        return namedClusterServiceLocator;
    }

    @Override
    public HadoopFileSystemLocator getHadoopFileSystemLocator() {
        if ( hadoopFileSystemLocator == null ) {
            hadoopFileSystemLocator = HadoopFileSystemLocatorImpl.getInstance();
        }
        return hadoopFileSystemLocator;
    }

  @Override
  public Map<String, String> getShimIdentifier() {
    HadoopConfigurationBootstrap hadoopConfigurationBootstrap = HadoopConfigurationBootstrap.getInstance();
    HadoopConfiguration hadoopConfiguration;
    try {
      HadoopConfigurationLocator hadoopConfigurationProvider = (HadoopConfigurationLocator) hadoopConfigurationBootstrap.getProvider();
      hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();
    } catch ( org.pentaho.hadoop.shim.api.ConfigurationException e ) {
      return null;
    }
    ShimIdentifier identifier = hadoopConfiguration.getHadoopShim().getShimIdentifier();
    Map<String, String> shimIdentifier = new HashMap<>();
    shimIdentifier.put( ShimIdentifier.SHIM_ID, identifier.getId() );
    shimIdentifier.put( ShimIdentifier.SHIM_VENDOR, identifier.getVendor() );
    shimIdentifier.put( ShimIdentifier.SHIM_VERSION, identifier.getVersion() );
    return shimIdentifier;
  }
}
