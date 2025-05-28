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

import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;

public interface BigDataServicesProxy {
    NamedClusterServiceLocator getNamedClusterServiceLocator();
}
