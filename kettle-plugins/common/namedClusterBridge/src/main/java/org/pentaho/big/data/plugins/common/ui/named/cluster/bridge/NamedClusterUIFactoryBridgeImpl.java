/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.clusterTest.ClusterTester;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.big.data.plugins.common.ui.NamedClusterDialogImpl;
import org.pentaho.big.data.plugins.common.ui.NamedClusterWidgetImpl;
import org.pentaho.di.ui.core.namedcluster.HadoopClusterDelegate;
import org.pentaho.di.ui.core.namedcluster.NamedClusterDialog;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIFactory;
import org.pentaho.di.ui.core.namedcluster.NamedClusterUIHelper;
import org.pentaho.di.ui.core.namedcluster.NamedClusterWidget;
import org.pentaho.di.ui.spoon.Spoon;

/**
 * Created by bryan on 8/17/15.
 */
public class NamedClusterUIFactoryBridgeImpl implements NamedClusterUIFactory {
  private final NamedClusterService namedClusterService;
  private final ClusterTester clusterTester;

  public NamedClusterUIFactoryBridgeImpl( NamedClusterService namedClusterService, ClusterTester clusterTester ) {
    this.namedClusterService = namedClusterService;
    this.clusterTester = clusterTester;
    NamedClusterUIHelper.setNamedClusterUIFactory( this );
  }

  @Override public NamedClusterWidget createNamedClusterWidget( Composite parent, boolean showLabel ) {
    return new NamedClusterWidgetBridgedImpl(
      new NamedClusterWidgetImpl( parent, showLabel, namedClusterService, clusterTester ) );
  }

  @Override public HadoopClusterDelegate createHadoopClusterDelegate( Spoon spoon ) {
    return new HadoopClusterDelegateBridgeImpl(
      new HadoopClusterDelegateImpl( spoon, namedClusterService, clusterTester ) );
  }

  @Override public NamedClusterDialog createNamedClusterDialog( Shell shell ) {
    return new NamedClusterDialogBridgeImpl( new NamedClusterDialogImpl( shell, namedClusterService, clusterTester ) );
  }
}
