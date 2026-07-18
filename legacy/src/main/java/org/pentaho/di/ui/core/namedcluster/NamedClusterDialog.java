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



package org.pentaho.di.ui.core.namedcluster;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
/**
 * Created by bryan on 8/17/15.
 */
public interface NamedClusterDialog {
  void setNamedCluster( NamedCluster namedCluster );

  NamedCluster getNamedCluster();

  void setNewClusterCheck( boolean newClusterCheck );

  String open();
}
