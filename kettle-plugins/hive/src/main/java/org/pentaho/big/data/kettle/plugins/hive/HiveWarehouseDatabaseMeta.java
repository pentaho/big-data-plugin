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

package org.pentaho.big.data.kettle.plugins.hive;

import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;

@DatabaseMetaPlugin( type = "HIVEWAREHOUSE", typeDescription = "Hive Warehouse Connector" )
public class HiveWarehouseDatabaseMeta extends Hive2DatabaseMeta {
  public HiveWarehouseDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService ) {
    super( driverLocator, namedClusterService );
  }
}
