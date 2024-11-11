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


package org.pentaho.big.data.plugins.common.ui.named.cluster.bridge;

import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.widgets.Composite;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.ui.core.namedcluster.NamedClusterWidget;

/**
 * Created by bryan on 8/17/15.
 *
 * @deprecated
 */
@Deprecated
public class NamedClusterWidgetBridgedImpl implements NamedClusterWidget {
  private final NamedClusterWidgetImpl namedClusterWidget;

  public NamedClusterWidgetBridgedImpl( NamedClusterWidgetImpl namedClusterWidget ) {
    this.namedClusterWidget = namedClusterWidget;
  }

  @Override public void initiate() {
    namedClusterWidget.initiate();
  }

  @Override public Composite getComposite() {
    return namedClusterWidget;
  }

  @Override public NamedCluster getSelectedNamedCluster() {
    return NamedClusterBridgeImpl.fromOsgiNamedCluster( namedClusterWidget.getSelectedNamedCluster() );
  }

  @Override public void setSelectedNamedCluster( String name ) {
    namedClusterWidget.setSelectedNamedCluster( name );
  }

  @Override public void addSelectionListener( SelectionListener selectionListener ) {
    namedClusterWidget.addSelectionListener( selectionListener );
  }
}
