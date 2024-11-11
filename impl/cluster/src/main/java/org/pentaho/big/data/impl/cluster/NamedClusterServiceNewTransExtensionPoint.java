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

package org.pentaho.big.data.impl.cluster;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;

import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;

/**
 * Created by tkafalas on 7/14/2017.
 * * <p>
 * This class exists because two ExtensionPoint annotations are not allowed on the same class
 */
@ExtensionPoint( id = "NamedClusterServiceNewTransExtensionPoint", extensionPointId = "TransformationCreateNew",
  description = "" )
public class NamedClusterServiceNewTransExtensionPoint extends NamedClusterServiceExtensionPoint {
  NamedClusterServiceOsgi namedClusterServiceOsgi;

  public NamedClusterServiceNewTransExtensionPoint( NamedClusterService namedClusterService ) {
    super( namedClusterService );
  }
}
