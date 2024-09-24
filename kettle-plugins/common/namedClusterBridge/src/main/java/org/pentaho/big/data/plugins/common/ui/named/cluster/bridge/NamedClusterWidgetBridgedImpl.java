/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
