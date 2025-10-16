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
import org.pentaho.di.core.service.ServiceProvider;
import org.pentaho.di.core.service.ServiceProviderInterface;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.hadoop.shim.api.services.BigDataServicesProxy;

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
}
