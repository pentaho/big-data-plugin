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


package org.pentaho.di.ui.core.namedcluster;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.core.variables.VariableSpace;
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
