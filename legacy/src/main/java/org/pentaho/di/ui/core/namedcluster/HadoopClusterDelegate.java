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

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;

/**
 * Created by bryan on 8/17/15.
 */
public interface HadoopClusterDelegate {
  String editNamedCluster( IMetaStore metaStore, NamedCluster namedCluster, Shell shell );

  String newNamedCluster( VariableSpace variableSpace, IMetaStore metaStore, Shell shell );

  void dupeNamedCluster( IMetaStore metaStore, NamedCluster nc, Shell shell );

  void delNamedCluster( IMetaStore metaStore, NamedCluster namedCluster );
}
