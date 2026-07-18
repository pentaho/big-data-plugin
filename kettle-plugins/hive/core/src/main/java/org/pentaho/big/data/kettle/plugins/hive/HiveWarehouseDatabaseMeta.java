/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.hive;

import org.pentaho.big.data.api.jdbc.impl.DriverLocatorImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.jdbc.DriverLocator;

@DatabaseMetaPlugin( type = "HIVEWAREHOUSE", typeDescription = "Hive Warehouse Connector" )
public class HiveWarehouseDatabaseMeta extends Hive2DatabaseMeta {
  public HiveWarehouseDatabaseMeta() {
    this( DriverLocatorImpl.getInstance(), NamedClusterManager.getInstance() );
  }
  public HiveWarehouseDatabaseMeta( DriverLocator driverLocator, NamedClusterService namedClusterService ) {
    super( driverLocator, namedClusterService );
  }
}
