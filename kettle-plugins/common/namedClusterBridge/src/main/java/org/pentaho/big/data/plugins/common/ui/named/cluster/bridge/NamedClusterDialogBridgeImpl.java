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


package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.pentaho.big.data.plugins.common.ui.NamedClusterDialogImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.namedcluster.NamedClusterDialog;

/**
 * Created by bryan on 8/17/15.
 *
 * @deprecated
 */
@Deprecated
public class NamedClusterDialogBridgeImpl implements NamedClusterDialog {
  private final NamedClusterDialogImpl delegate;

  public NamedClusterDialogBridgeImpl( NamedClusterDialogImpl delegate ) {
    this.delegate = delegate;
  }

  @Override public NamedCluster getNamedCluster() {
    return NamedClusterBridgeImpl.fromOsgiNamedCluster( delegate.getNamedCluster() );
  }

  @Override public void setNamedCluster( NamedCluster namedCluster ) {
    delegate.setNamedCluster( new NamedClusterBridgeImpl( namedCluster ) );
  }

  @Override public void setNewClusterCheck( boolean newClusterCheck ) {
    delegate.setNewClusterCheck( newClusterCheck );
  }

  @Override public String open() {
    return delegate.open();
  }
}
