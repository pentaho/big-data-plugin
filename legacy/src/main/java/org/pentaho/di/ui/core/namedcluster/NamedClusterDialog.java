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

package org.pentaho.di.ui.core.namedcluster;

import org.pentaho.di.core.namedcluster.model.NamedCluster;

/**
 * Created by bryan on 8/17/15.
 */
public interface NamedClusterDialog {
  void setNamedCluster( NamedCluster namedCluster );

  NamedCluster getNamedCluster();

  void setNewClusterCheck( boolean newClusterCheck );

  String open();
}
