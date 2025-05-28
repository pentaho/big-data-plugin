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
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;

import java.util.Collection;

public class BigDataServicesHelper {

    private BigDataServicesHelper(){}

    public static NamedClusterServiceLocator getNamedClusterServiceLocator() {
        try {
            Collection<BigDataServicesProxy> namedClusterServiceLocatorFactories = PluginServiceLoader.loadServices( BigDataServicesProxy.class );
            NamedClusterServiceLocator namedClusterServiceLocator = namedClusterServiceLocatorFactories.stream().findFirst().get().getNamedClusterServiceLocator();
            return namedClusterServiceLocator;
        } catch (KettlePluginException e) {
            e.printStackTrace();
            return null;
        }
    }
}
