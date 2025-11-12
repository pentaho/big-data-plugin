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

package org.pentaho.big.data.api.services;

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.services.BigDataServicesProxy;

import java.util.Collection;

public class BigDataServicesHelper {


  private BigDataServicesHelper() {
  }

  public static NamedClusterServiceLocator getNamedClusterServiceLocator() {
    try {
      Collection<BigDataServicesProxy> namedClusterServiceLocatorFactories = PluginServiceLoader.loadServices( BigDataServicesProxy.class );
      return namedClusterServiceLocatorFactories.stream().findFirst().map( BigDataServicesProxy::getNamedClusterServiceLocator ).orElse( null );
    } catch ( KettlePluginException e ) {
      e.printStackTrace();
      return null;
    }
  }

  public static HadoopFileSystemLocator getHadoopFileSystemLocator() {
    try {
      Collection<BigDataServicesProxy> namedClusterServiceLocatorFactories = PluginServiceLoader.loadServices( BigDataServicesProxy.class );
      return namedClusterServiceLocatorFactories.stream().findFirst().map( BigDataServicesProxy::getHadoopFileSystemLocator ).orElse( null );
    } catch ( KettlePluginException e ) {
      e.printStackTrace();
      return null;
    }
  }

  public static String getShimIdentifier() {
    try {
      Collection<BigDataServicesProxy> namedClusterServiceLocatorFactories = PluginServiceLoader.loadServices( BigDataServicesProxy.class );
      return namedClusterServiceLocatorFactories.stream().findFirst().map( BigDataServicesProxy::getShimIdentifier ).orElse( null );
    } catch ( Exception e ) {
      return null;
    }
  }

  public static NamedClusterService getNamedClusterService() {
    try {
      Collection<BigDataServicesProxy> bigDataServicesProxies = PluginServiceLoader.loadServices( BigDataServicesProxy.class );
      return bigDataServicesProxies.stream().findFirst().map( BigDataServicesProxy::getNamedClusterService ).orElse( null );
    } catch ( KettlePluginException e ) {
      e.printStackTrace();
      return null;
    }
  }
}
